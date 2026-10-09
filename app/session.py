"""CallSession — owns one call end-to-end: audio in, routed STT, phrase
buffering, persistence, and live fan-out to subscribed WebSocket clients.

A call runs one or more concurrent "tracks", each an independent STT engine
fed the same resampled audio:

  - the *primary* track is the fallback chain from ROUTING — "final"
    events from it are the call's canonical transcript; a "fatal" event
    advances it to the next engine in the chain.
  - *parallel* tracks (PARALLEL_ENGINES) run alongside the primary track
    for quality comparison (ТЗ item 3) — their output is tagged with their
    own engine name and persisted/broadcast the same way, but a "fatal"
    event just stops that track; it never affects the primary transcript.

Phrase assembly: forwards "final" events downstream (tagged with the
producing engine) and live "partial" events for real-time display.
Consecutive repeats of the same partial or final text within a track are
deduped so downstream only sees changes; switching to a fallback engine
starts a fresh segment. Dedup state is per-track, so two engines
transcribing the same audio never dedupe against each other.

Persistence: a single-leg call (no role) persists to its own per-call
data/transcripts/{call_id}.jsonl, its only record. A role-tagged (dual-leg)
call skips that file entirely — its "final" records already land in the
per-engine conversation files that main.py's broadcast() writes
(data/transcripts/conversations/{conversation_id}-{engine}.jsonl), so a
separate per-call copy would just be redundant.

Role-tagged (dual-leg) calls also get their 16kHz audio recorded to
data/recordings/{call_id}.wav as it arrives, so main.py can run an offline
Whisper pass over it after the call ends (see adapters.transcribe_recording)
without needing to keep up with the call live.
"""
import asyncio
import audioop
import json
import logging
import os
import time
import wave
import uuid
from pathlib import Path

from .adapters import build_adapter
from . import observability

log = logging.getLogger("session")

TRANSCRIPT_DIR = Path("/data/transcripts")
TRANSCRIPT_DIR.mkdir(parents=True, exist_ok=True)

# Recordings are only made for role-tagged (dual-leg) calls — that's the
# only context where a downstream offline pass (e.g. Whisper comparison,
# see main.py) has any use for the audio; single-leg ru/ky calls don't get
# one, to avoid the disk cost for no benefit.
RECORDINGS_DIR = Path("/data/recordings")
RECORDINGS_DIR.mkdir(parents=True, exist_ok=True)

# language -> ordered list of (build_name, lang, tag); first is primary,
# rest are fallbacks tried in order if the previous one goes "fatal".
# build_name selects the adapter via build_adapter(); tag is what shows up
# in persisted/broadcast "engine" fields and merged filenames — they only
# differ where one adapter class serves multiple language configs that each
# need their own distinct output (vosk, whisper).
#
# GigaAM Multilingual replaces both vosk-ky and vosk-ru (2026-09-08) — one
# model covers both languages, including code-switched speech within a
# single utterance, which the vosk-ky/vosk-ru split couldn't: vosk-ky
# hallucinated fluent-sounding but unrelated Kyrgyz when fed Russian
# speech (confirmed live — fed the same clip to vosk-ru instead and got
# sensible output), because a single-language model has no way to
# recognize audio outside its language, only to force-fit it into one.
# Compared directly against vosk-ky on a real production call recording
# (see gigaam-server/asr_server.py's docstring): fewer garbled words,
# correct quantities/dish names it had gotten wrong. Measured RTF~0.29 on
# this CPU-only box — real-time capable without a GPU, ~10x faster than
# Whisper-small was here. vosk-ky/vosk-ru are stopped (see
# docker-compose.yml's "disabled" profile) but not deleted, for a fast
# rollback if GigaAM doesn't hold up under real concurrent-call load —
# that hasn't been tested yet, only single-call.
ROUTING = {
    "ru": [("gigaam", "ru", "gigaam")],
    "ky": [("gigaam", "ky", "gigaam")],
}

PARALLEL_ENGINES = {}

# Whisper comparison track — separately gated, off by default. Measured
# RTF=3.06 on this CPU-only box (24s to transcribe one 8s window) means it
# falls permanently behind live audio, so its "final" only reflects
# whatever's left in its rolling buffer when the call ends, not the whole
# conversation — not comparable to Vosk/AiRUN's full-coverage output. (The
# offline post-call Whisper pass in main.py is unaffected by this and
# produces its own "whisper-ky"/"whisper-ru" files regardless.)
if os.environ.get("PARALLEL_STT_ENABLED", "false").lower() not in ("0", "false", "no"):
    PARALLEL_ENGINES.setdefault("ru", []).append(("whisper", "ru", "whisper"))
    PARALLEL_ENGINES.setdefault("ky", []).append(("whisper", "ky", "whisper-ky"))


def _normalize(text: str) -> str:
    return " ".join(text.split())


class _EngineTrack:
    """One running STT engine within a call, fed a copy of the call's audio.

    `is_primary` controls fallback behavior: a primary track advances to
    the next engine in its chain on "fatal"; a non-primary (comparison)
    track just stops — it never triggers a fallback of its own.
    """

    def __init__(self, call_id: str, chain: list, adapter_config: dict, on_event, is_primary: bool):
        self.chain = chain
        self.adapter_config = adapter_config
        self.on_event = on_event  # async callable(engine_name, kind, text)
        self.is_primary = is_primary
        self._call_id = call_id
        self._chain_idx = 0
        self._pcm_queue: asyncio.Queue | None = None
        self._event_queue: asyncio.Queue = asyncio.Queue()
        self._engine_task: asyncio.Task | None = None
        self._dispatcher_task: asyncio.Task | None = None
        self._last_partial_text: str | None = None
        self._last_final_text: str | None = None
        self._last_final_at = time.monotonic()
        self._audio_seconds = 0.0
        self._last_final_audio_seconds = 0.0
        self._metric_labels: dict[str, str] = {}

    async def start(self):
        self._dispatcher_task = asyncio.create_task(self._dispatch_events())
        await self._start_engine(0)

    async def _start_engine(self, idx: int):
        if idx >= len(self.chain):
            if self.is_primary:
                log.error("[%s] no more engines in fallback chain", self._call_id)
            return
        self._chain_idx = idx
        build_name, lang, tag = self.chain[idx]
        adapter = build_adapter(build_name, lang, **self.adapter_config)
        self._metric_labels = {"model": tag, "language": lang}
        observability.set_gauge("stt_queue_size", 0, self._metric_labels)
        observability.inc("stt_active_jobs", labels=self._metric_labels)
        observability.event_log(log, "stt_started", call_id=self._call_id,
                                model=tag, language=lang, status="started")
        self._pcm_queue = asyncio.Queue()
        self._last_partial_text = None
        self._last_final_text = None
        await self.on_event(tag, "system", f"engine started: {tag} ({lang})")
        self._engine_task = asyncio.create_task(
            adapter.stream(self._pcm_queue, self._event_queue)
        )

    async def feed(self, pcm16k: bytes):
        if self._pcm_queue is not None:
            self._audio_seconds += len(pcm16k) / (2 * 16000)
            await self._pcm_queue.put(pcm16k)
            observability.set_gauge("stt_queue_size", self._pcm_queue.qsize(), self._metric_labels)

    async def _dispatch_events(self):
        while True:
            ev = await self._event_queue.get()
            try:
                await self._handle_event(ev)
            except asyncio.CancelledError:
                raise
            except Exception:
                # A downstream failure (e.g. broadcaster/persistence
                # raising) must not silently kill this loop — that would
                # stop processing every subsequent event for this track
                # (all "final"/"partial" output) with no visible error.
                # Log it and keep going.
                log.exception("[%s] failed to handle STT event %r", self._call_id, ev.get("type"))

    async def _handle_event(self, ev: dict):
        t = ev.get("type")
        name = self.chain[self._chain_idx][2]

        if t == "final":
            text = _normalize(ev.get("text", ""))
            if not text or text == self._last_final_text:
                return
            self._last_final_text = text
            self._last_partial_text = None
            now = time.monotonic()
            processing = max(0, now - self._last_final_at)
            audio_duration = max(0, self._audio_seconds - self._last_final_audio_seconds)
            inference_latency = ev.get("inference_latency_ms")
            pipeline_latency = ev.get("pipeline_latency_ms")
            if inference_latency is not None:
                inference_latency = max(0, float(inference_latency) / 1000)
            else:
                inference_latency = processing
            if pipeline_latency is not None:
                pipeline_latency = max(0, float(pipeline_latency) / 1000)
            if ev.get("audio_duration_ms") is not None:
                audio_duration = max(0, float(ev["audio_duration_ms"]) / 1000)
            if audio_duration:
                observability.observe("stt_audio_duration_seconds", audio_duration, self._metric_labels)
                observability.observe("stt_rtf", inference_latency / audio_duration, self._metric_labels)
            observability.observe("stt_latency_seconds", inference_latency, self._metric_labels)
            if pipeline_latency is not None:
                observability.observe("stt_pipeline_latency_seconds", pipeline_latency, self._metric_labels)
            observability.inc("stt_requests_total", labels={**self._metric_labels, "status": "success"})
            segment_id = f"seg-{uuid.uuid4()}"
            observability.event_log(log, "audio_segment_received", call_id=self._call_id,
                                    segment_id=segment_id, model=name, status="received")
            await self.on_event(name, "final", text, {
                "segment_id": segment_id,
                "duration_ms": round(processing * 1000),
                "audio_duration_ms": round(audio_duration * 1000),
                "inference_latency_ms": round(inference_latency * 1000),
                "pipeline_latency_ms": round(pipeline_latency * 1000) if pipeline_latency is not None else None,
                "audio_start_timestamp": ev.get("audio_start_timestamp"),
                "audio_end_timestamp": ev.get("audio_end_timestamp"),
                "inference_start_timestamp": ev.get("inference_start_timestamp"),
                "inference_end_timestamp": ev.get("inference_end_timestamp"),
                "speech_end_timestamp": ev.get("speech_end_timestamp"),
                "segment_flush_timestamp": ev.get("segment_flush_timestamp"),
                "speech_duration_ms": ev.get("speech_duration_ms"),
                "silence_duration_ms": ev.get("silence_duration_ms"),
                "endpoint_delay_ms": ev.get("endpoint_delay_ms"),
                "speech_end_to_transcript_ms": ev.get("speech_end_to_transcript_ms"),
                "flush_reason": ev.get("flush_reason"),
                "active_stt_jobs_at_start": ev.get("active_stt_jobs_at_start"),
                "active_stt_jobs_at_end": ev.get("active_stt_jobs_at_end"),
                "process_cpu_percent": ev.get("process_cpu_percent"),
                "system_cpu_percent": ev.get("system_cpu_percent"),
                "system_load": ev.get("system_load"),
                "rms_min": ev.get("rms_min"), "rms_max": ev.get("rms_max"),
                "rms_mean": ev.get("rms_mean"), "rms_p50": ev.get("rms_p50"),
                "rms_p95": ev.get("rms_p95"),
                "speech_frame_count": ev.get("speech_frame_count"),
                "silence_frame_count": ev.get("silence_frame_count"),
                "speech_ratio": ev.get("speech_ratio"),
                "rtf": ev.get("rtf"),
            })
            self._last_final_at = now
            self._last_final_audio_seconds = self._audio_seconds
        elif t == "partial":
            text = _normalize(ev.get("text", ""))
            if not text or text == self._last_partial_text:
                return
            self._last_partial_text = text
            await self.on_event(name, "partial", text)
        elif t == "error":
            observability.inc("stt_errors_total", labels={**self._metric_labels, "error_type": "engine_error"})
            await self.on_event(name, "system", f"error: {ev.get('message')}")
        elif t == "fatal":
            observability.inc("stt_errors_total", labels={**self._metric_labels, "error_type": "fatal"})
            observability.event_log(log, "stt_failed", call_id=self._call_id,
                                    model=name, status="error", error_type="fatal")
            if self.is_primary:
                await self.on_event(name, "system", f"fatal ({ev.get('reason')}) — attempting fallback")
                await self._start_engine(self._chain_idx + 1)
            else:
                await self.on_event(name, "system", f"fatal ({ev.get('reason')}) — comparison track stopped")
        elif t == "done":
            await self.on_event(name, "system", "done")

    async def close(self):
        if self._pcm_queue is not None:
            await self._pcm_queue.put(None)
        if self._engine_task:
            try:
                await asyncio.wait_for(self._engine_task, timeout=10)
            except asyncio.TimeoutError:
                self._engine_task.cancel()
        if self._dispatcher_task:
            self._dispatcher_task.cancel()


class CallSession:
    def __init__(self, call_id: str, language: str, adapter_config: dict, broadcaster,
                 role: str | None = None, audio_broadcaster=None):
        self.call_id = call_id
        self.language = language
        self.adapter_config = adapter_config
        self.broadcaster = broadcaster  # callable(call_id, event_dict)
        self.audio_broadcaster = audio_broadcaster  # optional callable(call_id, pcm16k: bytes)
        self.role = role  # "client" / "operator" for dual-leg capture, else None

        self._resample_state = None
        self._resample_rate = None
        self._audio_frames = 0
        self._transcript_path = TRANSCRIPT_DIR / f"{call_id}.jsonl"

        self.recording_path: Path | None = None
        self._wav_writer: wave.Wave_write | None = None
        if role is not None:
            self.recording_path = RECORDINGS_DIR / f"{call_id}.wav"
            self._wav_writer = wave.open(str(self.recording_path), "wb")
            self._wav_writer.setnchannels(1)
            self._wav_writer.setsampwidth(2)
            self._wav_writer.setframerate(16000)

        primary_chain = ROUTING.get(language, ROUTING["ru"])
        self._tracks = [_EngineTrack(call_id, primary_chain, adapter_config, self._on_event, is_primary=True)]
        for build_name, lang, tag in PARALLEL_ENGINES.get(language, []):
            self._tracks.append(
                _EngineTrack(call_id, [(build_name, lang, tag)], adapter_config, self._on_event, is_primary=False)
            )

    async def start(self):
        for track in self._tracks:
            await track.start()

    async def feed(self, raw_audio: bytes, sample_rate: int = 8000):
        """Feed one AudioSocket audio payload (slin mono, any negotiated rate —
        see AUDIO_SAMPLE_RATES) to every track, resampled to 16kHz."""
        self._audio_frames += 1
        if sample_rate != self._resample_rate:
            self._resample_rate = sample_rate
            self._resample_state = None  # source rate changed — reset converter state
        pcm16k, self._resample_state = audioop.ratecv(
            raw_audio, 2, 1, sample_rate, 16000, self._resample_state
        )
        if self._wav_writer is not None:
            # Off the event loop: this loop also drains the AudioSocket TCP
            # stream in real time, and a blocking disk write here (this box
            # sees real swap pressure) can stall it long enough that
            # Asterisk's synchronous AudioSocket socket write blocks too —
            # which backs up frames in the bridge until it hits Asterisk's
            # internal refcount safety limit and crashes/wedges the channel
            # (confirmed via a live FRACK assertion in bridge_simple.so
            # during exactly this scenario).
            await asyncio.to_thread(self._wav_writer.writeframes, pcm16k)

        if self.audio_broadcaster is not None:
            # Live waveform only — never lets a slow/absent browser
            # subscriber affect this call's real STT/recording path.
            await self.audio_broadcaster(self.call_id, pcm16k)
        for track in self._tracks:
            await track.feed(pcm16k)

    async def _on_event(self, engine_name: str, kind: str, text: str, metadata: dict | None = None):
        if kind == "final":
            metadata = metadata or {}
            record = {
                "call_id": self.call_id,
                "engine": engine_name,
                "text": text,
                "ts": time.time(),
                "segment_id": metadata.get("segment_id") or f"seg-{uuid.uuid4()}",
            }
            for key in (
                "audio_duration_ms", "inference_latency_ms", "pipeline_latency_ms",
                "rtf", "audio_start_timestamp", "audio_end_timestamp",
                "inference_start_timestamp", "inference_end_timestamp",
                "speech_end_timestamp", "segment_flush_timestamp", "speech_duration_ms",
                "silence_duration_ms", "endpoint_delay_ms", "speech_end_to_transcript_ms",
            "flush_reason", "active_stt_jobs_at_start", "active_stt_jobs_at_end",
            "rms_min", "rms_max", "rms_mean", "rms_p50", "rms_p95",
            "speech_frame_count", "silence_frame_count", "speech_ratio",
                "process_cpu_percent", "system_cpu_percent", "system_load",
            ):
                if metadata.get(key) is not None:
                    record[key] = metadata[key]
            if self.role:
                record["role"] = self.role
            if metadata.get("rtf") is None:
                inference_ms = metadata.get("inference_latency_ms")
                audio_ms = metadata.get("audio_duration_ms")
                if inference_ms is not None and audio_ms:
                    record["rtf"] = float(inference_ms) / float(audio_ms)
            await asyncio.to_thread(self._persist, record)
            log.info("LIVE_WS_BROADCAST_EVENT %s", json.dumps(record, ensure_ascii=False, sort_keys=True))
            await self.broadcaster(self.call_id, record)
            observability.event_log(log, "stt_completed", call_id=self.call_id,
                                    segment_id=record["segment_id"], model=engine_name,
                                    language=self.language, status="success",
                                    duration_ms=metadata.get("duration_ms"),
                                    audio_duration_ms=metadata.get("audio_duration_ms"),
                                    inference_latency_ms=metadata.get("inference_latency_ms"),
                                    pipeline_latency_ms=metadata.get("pipeline_latency_ms"),
                                    audio_start_timestamp=metadata.get("audio_start_timestamp"),
                                    audio_end_timestamp=metadata.get("audio_end_timestamp"),
                                    inference_start_timestamp=metadata.get("inference_start_timestamp"),
                                    inference_end_timestamp=metadata.get("inference_end_timestamp"))
        elif kind == "partial":
            event = {
                "call_id": self.call_id, "engine": engine_name,
                "partial": text, "ts": time.time(),
            }
            if self.role:
                event["role"] = self.role
            await self.broadcaster(self.call_id, event)
        elif kind == "system":
            await asyncio.to_thread(self._log, f"[{engine_name}] {text}")

    def _persist(self, record: dict):
        # Role-tagged (dual-leg) calls skip the per-call file entirely —
        # their "final" records already land in the per-engine conversation
        # files (main.py's broadcast()), and duplicating them here was
        # redundant. Single-leg calls have no other persistence, so they
        # keep writing their own file as before.
        if self.role is not None:
            return
        with open(self._transcript_path, "a") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    def _log(self, msg: str):
        line = f"[{self.call_id}] {msg}"
        log.info(line)
        if self.role is not None:
            return
        with open(self._transcript_path, "a") as f:
            f.write(json.dumps({"call_id": self.call_id, "system": msg, "ts": time.time()},
                                ensure_ascii=False) + "\n")

    async def close(self):
        for track in self._tracks:
            observability.inc("stt_active_jobs", -1, track._metric_labels)
        for track in self._tracks:
            await track.close()
        if self._wav_writer is not None:
            self._wav_writer.close()
        await asyncio.to_thread(self._log, f"call ended, total audio frames: {self._audio_frames}")