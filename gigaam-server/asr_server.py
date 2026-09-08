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
import json
import logging
import os
import tempfile
import wave
from concurrent.futures import ThreadPoolExecutor

import websockets
from transformers import AutoModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("gigaam-server")

HOST = os.environ.get("GIGAAM_SERVER_HOST", "0.0.0.0")
PORT = int(os.environ.get("GIGAAM_SERVER_PORT", "2700"))
DEFAULT_SAMPLE_RATE = int(os.environ.get("GIGAAM_DEFAULT_SAMPLE_RATE", "16000"))
MODEL_REVISION = os.environ.get("GIGAAM_MODEL_REVISION", "ctc")  # ctc (220M) or large_ctc (600M)
CHUNK_SECONDS = float(os.environ.get("GIGAAM_CHUNK_SECONDS", "15"))
# Below the model's own 25s cap (LONGFORM_THRESHOLD) with margin.
MIN_FLUSH_SECONDS = float(os.environ.get("GIGAAM_MIN_FLUSH_SECONDS", "0.6"))
# PyTorch's own CPU threading already spreads one transcribe() call across
# multiple cores — running several calls concurrently doesn't add free
# throughput on top of that, it splits the same core budget. Keep this
# low; raise only after measuring concurrent-call throughput for real,
# the same way vosk-server's concurrency bug only showed up under actual
# concurrent load.
MAX_WORKERS = int(os.environ.get("GIGAAM_SERVER_MAX_WORKERS", "3"))

log.info("loading ai-sage/GigaAM-Multilingual (revision=%s) ...", MODEL_REVISION)
MODEL = AutoModel.from_pretrained(
    "ai-sage/GigaAM-Multilingual", revision=MODEL_REVISION, trust_remote_code=True,
)
log.info("model loaded, listening on ws://%s:%d (chunk=%.0fs, max_workers=%d)",
          HOST, PORT, CHUNK_SECONDS, MAX_WORKERS)

EXECUTOR = ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="gigaam-decode")


def _transcribe_pcm(pcm: bytes, sample_rate: int) -> str:
    with tempfile.NamedTemporaryFile(suffix=".wav") as f:
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
    chunk_bytes = int(CHUNK_SECONDS * sample_rate) * 2  # PCM16LE = 2 bytes/sample
    min_flush_bytes = int(MIN_FLUSH_SECONDS * sample_rate) * 2
    log.info("[%s] connection open", peer)

    async def flush(min_bytes: int):
        nonlocal buf
        if len(buf) < min_bytes:
            return
        pcm, buf = bytes(buf), bytearray()
        try:
            text = await loop.run_in_executor(EXECUTOR, _transcribe_pcm, pcm, sample_rate)
            if text:
                await ws.send(json.dumps({"text": text}))
        except Exception:
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
                    await flush(min_flush_bytes)
                    break

                log.info("[%s] unrecognized control message, ignoring: %r", peer, parsed)
                continue

            # Binary audio frame.
            buf.extend(message)
            if len(buf) >= chunk_bytes:
                await flush(1)  # buffer is already >= one full chunk

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
