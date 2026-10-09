"""GigaAM Multilingual ASR server — speaks the exact same WebSocket
protocol as vosk-server/asr_server.py, so the gateway's existing
VoskAdapter client code works against it unchanged (see
app.adapters.GigaAMAdapter, a one-line subclass just for a clearer tag):

  - client sends {"config": {"sample_rate": N}} once, before any audio
  - client sends raw PCM16LE binary chunks (mono, 16kHz — matches what
    CallSession.feed already resamples everything to)
  - client sends {"eof": 1} to end the session
  - server sends {"text": "..."} once per completed chunk

One model (ai-sage/GigaAM-Multilingual, "ctc" revision — the 220M
compact encoder) covers both Kyrgyz and Russian, including code-switched
speech within a single utterance, unlike routing between two
language-specific Vosk models. Measured on this deployment's CPU:
RTF~0.29 (single-connection) — real-time capable, no GPU required to
start.

No native partial/incremental output: the underlying model's plain
transcribe() caps at 25s of audio (LONGFORM_THRESHOLD in its own code)
and there's no streaming/RNNT decoder in the multilingual line, only
CTC. This server buffers incoming audio and transcribes it in
non-overlapping CHUNK_SECONDS windows instead — each completed window is
sent as a "final" (no "partial" is ever sent), so live text arrives in
~CHUNK_SECONDS-sized bursts rather than growing word by word like Vosk's
partials. transcribe_longform() (pyannote-based resegmentation) is
deliberately not used here — it requires a gated HF model
(pyannote/segmentation-3.0) and an HF_TOKEN, extra operational setup
this doesn't need for fixed-size chunking.
"""
import asyncio
import audioop
import json
import logging
import os
import resource
import tempfile
import time
import wave
from concurrent.futures import ThreadPoolExecutor

import websockets
import torch
import subprocess
from transformers import AutoModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("gigaam-server")

HOST = os.environ.get("GIGAAM_SERVER_HOST", "0.0.0.0")
PORT = int(os.environ.get("GIGAAM_SERVER_PORT", "2700"))
DEFAULT_SAMPLE_RATE = int(os.environ.get("GIGAAM_DEFAULT_SAMPLE_RATE", "16000"))
MODEL_REVISION = os.environ.get("GIGAAM_MODEL_REVISION", "ctc")  # ctc (220M) or large_ctc (600M)
CHUNK_SECONDS = float(os.environ.get("GIGAAM_CHUNK_SECONDS", "15"))
SEGMENTATION_MODE = os.environ.get("GIGAAM_SEGMENTATION_MODE", "fixed").lower()
VAD_SILENCE_MS = float(os.environ.get("GIGAAM_VAD_SILENCE_MS", "500"))
VAD_MIN_SEGMENT_SECONDS = float(os.environ.get("GIGAAM_VAD_MIN_SEGMENT_SECONDS", "0.7"))
VAD_MAX_SEGMENT_SECONDS = float(os.environ.get("GIGAAM_VAD_MAX_SEGMENT_SECONDS", "5"))
VAD_RMS_THRESHOLD = int(os.environ.get("GIGAAM_VAD_RMS_THRESHOLD", "350"))
# Below the model's own 25s cap (LONGFORM_THRESHOLD) with margin.
MIN_FLUSH_SECONDS = float(os.environ.get("GIGAAM_MIN_FLUSH_SECONDS", "0.6"))
# PyTorch's own CPU threading already spreads one transcribe() call across
# multiple cores — running several calls concurrently doesn't add free
# throughput on top of that, it splits the same core budget. Keep this
# low; raise only after measuring concurrent-call throughput for real,
# the same way vosk-server's concurrency bug only showed up under actual
# concurrent load.
MAX_WORKERS = int(os.environ.get("GIGAAM_SERVER_MAX_WORKERS", "4"))
_TORCH_THREADS = max(1, (os.cpu_count() or 1) // MAX_WORKERS)
torch.set_num_threads(_TORCH_THREADS)
log.info("loading ai-sage/GigaAM-Multilingual (revision=%s) ...", MODEL_REVISION)
MODEL = AutoModel.from_pretrained(
    "ai-sage/GigaAM-Multilingual", revision=MODEL_REVISION, trust_remote_code=True,
)
log.info("model loaded, listening on ws://%s:%d (mode=%s chunk=%.0fs vad_silence=%.0fms vad_max=%.1fs max_workers=%d)",
          HOST, PORT, SEGMENTATION_MODE, CHUNK_SECONDS, VAD_SILENCE_MS,
          VAD_MAX_SEGMENT_SECONDS, MAX_WORKERS)

EXECUTOR = ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="gigaam-decode")
ACTIVE_JOBS = 0


def _transcribe_pcm(pcm: bytes, sample_rate: int) -> str:
    with tempfile.NamedTemporaryFile(suffix=".wav", dir="/dev/shm") as f:
        wf = wave.open(f.name, "wb")
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(sample_rate)
        wf.writeframes(pcm)
        wf.close()
        result = MODEL.transcribe(f.name)
        return result.text if hasattr(result, "text") else str(result)


async def handle_connection(ws):
    peer = ws.remote_address
    sample_rate = DEFAULT_SAMPLE_RATE
    loop = asyncio.get_running_loop()
    buf = bytearray()
    buf_audio_start_ts = None
    vad_seen_speech = False
    vad_silence_ms = 0.0
    vad_speech_started_ts = None
    vad_speech_ended_ts = None
    rms_values = []
    speech_frame_count = 0
    silence_frame_count = 0
    chunk_bytes = int(CHUNK_SECONDS * sample_rate) * 2  # PCM16LE = 2 bytes/sample
    min_flush_bytes = int(MIN_FLUSH_SECONDS * sample_rate) * 2
    log.info("[%s] connection open", peer)

    async def flush(min_bytes: int, flush_reason: str):
        nonlocal buf, buf_audio_start_ts, vad_seen_speech, vad_silence_ms
        nonlocal vad_speech_started_ts, vad_speech_ended_ts, rms_values
        nonlocal speech_frame_count, silence_frame_count
        global ACTIVE_JOBS
        if len(buf) < min_bytes:
            return
        pcm, buf = bytes(buf), bytearray()
        audio_start_ts = buf_audio_start_ts or time.time()
        buf_audio_start_ts = None
        segment_silence_ms = vad_silence_ms if SEGMENTATION_MODE == "vad" else 0.0
        segment_rms = list(rms_values)
        segment_speech_frames = speech_frame_count
        segment_silence_frames = silence_frame_count
        speech_end_ts = vad_speech_ended_ts or time.time()
        vad_seen_speech = False
        vad_silence_ms = 0.0
        vad_speech_started_ts = None
        vad_speech_ended_ts = None
        rms_values = []
        speech_frame_count = 0
        silence_frame_count = 0
        audio_end_ts = time.time()
        silence_ms = segment_silence_ms
        if SEGMENTATION_MODE != "vad":
            speech_end_ts = audio_end_ts
        speech_duration_ms = max(0.0, len(pcm) / (2 * sample_rate) * 1000 - silence_ms)
        inference_start_ts = time.time()
        inference_start = time.monotonic()
        active_at_start = ACTIVE_JOBS
        ACTIVE_JOBS += 1
        cpu_start = resource.getrusage(resource.RUSAGE_SELF)
        try:
            text = await loop.run_in_executor(EXECUTOR, _transcribe_pcm, pcm, sample_rate)
            inference_latency_ms = round((time.monotonic() - inference_start) * 1000)
            inference_end_ts = time.time()
            ACTIVE_JOBS -= 1
            cpu_end = resource.getrusage(resource.RUSAGE_SELF)
            cpu_seconds = (cpu_end.ru_utime + cpu_end.ru_stime) - (cpu_start.ru_utime + cpu_start.ru_stime)
            wall_seconds = max(inference_latency_ms / 1000, 0.001)
            process_cpu_percent = cpu_seconds / wall_seconds * 100
            transcript_ready_ts = time.time()
            if text:
                sorted_rms = sorted(segment_rms)
                percentile = lambda values, p: values[min(len(values) - 1, int((len(values) - 1) * p))] if values else 0
                await ws.send(json.dumps({
                    "text": text,
                    "audio_start_timestamp": audio_start_ts,
                    "audio_end_timestamp": audio_end_ts,
                    "inference_start_timestamp": inference_start_ts,
                    "inference_end_timestamp": inference_end_ts,
                    "audio_duration_ms": round(len(pcm) / (2 * sample_rate) * 1000),
                    "inference_latency_ms": inference_latency_ms,
                    "pipeline_latency_ms": round((time.time() - audio_end_ts) * 1000),
                    "speech_end_timestamp": speech_end_ts,
                    "segment_flush_timestamp": audio_end_ts,
                    "speech_duration_ms": round(speech_duration_ms),
                    "silence_duration_ms": round(silence_ms),
                    "endpoint_delay_ms": round((audio_end_ts - speech_end_ts) * 1000),
                    "speech_end_to_transcript_ms": round((transcript_ready_ts - speech_end_ts) * 1000),
                    "flush_reason": flush_reason,
                    "active_stt_jobs_at_start": active_at_start,
                    "active_stt_jobs_at_end": ACTIVE_JOBS,
                    "process_cpu_percent": round(process_cpu_percent, 1),
                    "system_cpu_percent": None,
                    "system_load": os.getloadavg()[0] if hasattr(os, "getloadavg") else None,
                    "transcript_ready_timestamp": transcript_ready_ts,
                    "rtf": round(inference_latency_ms / max(len(pcm) / (2 * sample_rate) * 1000, 1), 6),
                    "rms_min": min(segment_rms) if segment_rms else 0,
                    "rms_max": max(segment_rms) if segment_rms else 0,
                    "rms_mean": round(sum(segment_rms) / len(segment_rms), 2) if segment_rms else 0,
                    "rms_p50": percentile(sorted_rms, 0.50),
                    "rms_p95": percentile(sorted_rms, 0.95),
                    "speech_frame_count": segment_speech_frames,
                    "silence_frame_count": segment_silence_frames,
                    "speech_ratio": round(segment_speech_frames / max(segment_speech_frames + segment_silence_frames, 1), 6),
                }))
        except Exception:
            ACTIVE_JOBS = max(0, ACTIVE_JOBS - 1)
            log.exception("[%s] transcription error on one chunk — dropping it, continuing", peer)

    try:
        async for message in ws:
            if isinstance(message, str):
                try:
                    parsed = json.loads(message)
                except json.JSONDecodeError:
                    log.warning("[%s] ignoring malformed control message: %r", peer, message[:200])
                    continue

                if "config" in parsed:
                    cfg = parsed.get("config") or {}
                    sample_rate = int(cfg.get("sample_rate", sample_rate))
                    chunk_bytes = int(CHUNK_SECONDS * sample_rate) * 2
                    min_flush_bytes = int(MIN_FLUSH_SECONDS * sample_rate) * 2
                    log.info("[%s] configured: sample_rate=%d", peer, sample_rate)
                    continue

                if "eof" in parsed:
                    await flush(min_flush_bytes, "eof")
                    break

                log.info("[%s] unrecognized control message, ignoring: %r", peer, parsed)
                continue

            # Binary audio frame.
            if not buf:
                buf_audio_start_ts = time.time()
            buf.extend(message)
            if SEGMENTATION_MODE == "vad":
                frame_ms = len(message) * 1000 / (2 * sample_rate)
                rms = audioop.rms(message, 2)
                rms_values.append(rms)
                voiced = rms >= VAD_RMS_THRESHOLD
                if voiced:
                    speech_frame_count += 1
                    if not vad_seen_speech:
                        vad_speech_started_ts = time.time()
                        log.info("VAD_SPEECH_STARTED role=%s timestamp=%.6f rms=%d threshold=%d", peer, vad_speech_started_ts, rms, VAD_RMS_THRESHOLD)
                    vad_seen_speech = True
                    vad_silence_ms = 0.0
                elif vad_seen_speech:
                    silence_frame_count += 1
                    vad_silence_ms += frame_ms
                    if vad_silence_ms >= VAD_SILENCE_MS and vad_speech_ended_ts is None:
                        vad_speech_ended_ts = time.time()
                        log.info("VAD_SPEECH_ENDED role=%s timestamp=%.6f rms=%d silence_ms=%.1f", peer, vad_speech_ended_ts, rms, vad_silence_ms)
                buffer_seconds = len(buf) / (2 * sample_rate)
                if buffer_seconds >= VAD_MAX_SEGMENT_SECONDS:
                    await flush(min_flush_bytes, "max_duration")
                elif (vad_seen_speech and buffer_seconds >= VAD_MIN_SEGMENT_SECONDS
                      and vad_silence_ms >= VAD_SILENCE_MS):
                    await flush(min_flush_bytes, "vad_silence")
            elif len(buf) >= chunk_bytes:
                await flush(1, "fixed_chunk")  # buffer is already >= one full chunk

    except websockets.exceptions.ConnectionClosed as e:
        log.info("[%s] connection closed: code=%s reason=%s", peer, e.code, e.reason)
    except Exception:
        log.exception("[%s] unexpected connection-level error", peer)
    finally:
        log.info("[%s] connection handler exiting", peer)


async def main():
    async with websockets.serve(handle_connection, HOST, PORT, max_size=None, ping_interval=None):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
