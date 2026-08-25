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
producing engine, persisted to a per-call JSONL file) and live "partial"
events for real-time display. Consecutive repeats of the same partial or
final text within a track are deduped so downstream only sees changes;
switching to a fallback engine starts a fresh segment. Dedup state is
per-track, so two engines transcribing the same audio never dedupe against
each other.
"""
import asyncio
import audioop
import json
import logging
import os
import time
from pathlib import Path

from .adapters import build_adapter

log = logging.getLogger("session")

TRANSCRIPT_DIR = Path("/data/transcripts")
TRANSCRIPT_DIR.mkdir(parents=True, exist_ok=True)

# language -> ordered list of (engine_name, engine_lang); first is primary,
# rest are fallbacks tried in order if the previous one goes "fatal".
ROUTING = {
    "ru": [("vosk", "ru")],
    "ky": [("airun", "ky"), ("vosk", "ky")],
}

# language -> engines that run alongside the primary chain for the whole
# call, for quality comparison. Real RTF on this deployment's CPU has not
# been measured (ТЗ item 3) — running Whisper on every live call adds real
# CPU load on top of the primary engine. Kill switch: PARALLEL_STT_ENABLED.
PARALLEL_ENGINES = {
    "ru": [("whisper", "ru")],
    "ky": [("whisper", "ky")],
} if os.environ.get("PARALLEL_STT_ENABLED", "true").lower() not in ("0", "false", "no") else {}


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

    async def start(self):
        self._dispatcher_task = asyncio.create_task(self._dispatch_events())
        await self._start_engine(0)

    async def _start_engine(self, idx: int):
        if idx >= len(self.chain):
            if self.is_primary:
                log.error("[%s] no more engines in fallback chain", self._call_id)
            return
        self._chain_idx = idx
        name, lang = self.chain[idx]
        adapter = build_adapter(name, lang, **self.adapter_config)
        self._pcm_queue = asyncio.Queue()
        self._last_partial_text = None
        self._last_final_text = None
        await self.on_event(name, "system", f"engine started: {name} ({lang})")
        self._engine_task = asyncio.create_task(
            adapter.stream(self._pcm_queue, self._event_queue)
        )

    async def feed(self, pcm16k: bytes):
        if self._pcm_queue is not None:
            await self._pcm_queue.put(pcm16k)

    async def _dispatch_events(self):
        while True:
            ev = await self._event_queue.get()
            t = ev.get("type")
            name = self.chain[self._chain_idx][0]

            if t == "final":
                text = _normalize(ev.get("text", ""))
                if not text or text == self._last_final_text:
                    continue
                self._last_final_text = text
                self._last_partial_text = None
                await self.on_event(name, "final", text)
            elif t == "partial":
                text = _normalize(ev.get("text", ""))
                if not text or text == self._last_partial_text:
                    continue
                self._last_partial_text = text
                await self.on_event(name, "partial", text)
            elif t == "error":
                await self.on_event(name, "system", f"error: {ev.get('message')}")
            elif t == "fatal":
                if self.is_primary:
                    await self.on_event(name, "system", f"fatal ({ev.get('reason')}) — attempting fallback")
                    await self._start_engine(self._chain_idx + 1)
                else:
                    await self.on_event(name, "system", f"fatal ({ev.get('reason')}) — comparison track stopped")
                    return
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
    def __init__(self, call_id: str, language: str, adapter_config: dict, broadcaster):
        self.call_id = call_id
        self.language = language
        self.adapter_config = adapter_config
        self.broadcaster = broadcaster  # callable(call_id, event_dict)

        self._resample_state = None
        self._audio_frames = 0
        self._transcript_path = TRANSCRIPT_DIR / f"{call_id}.jsonl"

        primary_chain = ROUTING.get(language, ROUTING["ru"])
        self._tracks = [_EngineTrack(call_id, primary_chain, adapter_config, self._on_event, is_primary=True)]
        for name, lang in PARALLEL_ENGINES.get(language, []):
            self._tracks.append(
                _EngineTrack(call_id, [(name, lang)], adapter_config, self._on_event, is_primary=False)
            )

    async def start(self):
        for track in self._tracks:
            await track.start()

    async def feed(self, raw_8k: bytes):
        """Feed one AudioSocket audio payload (slin, 8kHz mono) to every track."""
        self._audio_frames += 1
        pcm16k, self._resample_state = audioop.ratecv(
            raw_8k, 2, 1, 8000, 16000, self._resample_state
        )
        for track in self._tracks:
            await track.feed(pcm16k)

    async def _on_event(self, engine_name: str, kind: str, text: str):
        if kind == "final":
            record = {
                "call_id": self.call_id,
                "engine": engine_name,
                "text": text,
                "ts": time.time(),
            }
            self._persist(record)
            await self.broadcaster(self.call_id, record)
        elif kind == "partial":
            await self.broadcaster(self.call_id, {
                "call_id": self.call_id, "engine": engine_name,
                "partial": text, "ts": time.time(),
            })
        elif kind == "system":
            self._log(f"[{engine_name}] {text}")

    def _persist(self, record: dict):
        with open(self._transcript_path, "a") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    def _log(self, msg: str):
        line = f"[{self.call_id}] {msg}"
        log.info(line)
        with open(self._transcript_path, "a") as f:
            f.write(json.dumps({"call_id": self.call_id, "system": msg, "ts": time.time()},
                                ensure_ascii=False) + "\n")

    async def close(self):
        for track in self._tracks:
            await track.close()
        self._log(f"call ended, total audio frames: {self._audio_frames}")
