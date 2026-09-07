"""Minimal, crash-resistant replacement for alphacep/vosk-server's
reference asr_server.py.

Speaks exactly the same wire protocol app.adapters.VoskAdapter already
expects — no gateway-side changes needed, this only replaces what's behind
ws://vosk-ky:2700 / ws://vosk-ru:2700:

  - client sends {"config": {"sample_rate": N}} once, before any audio
  - client sends raw PCM16LE binary chunks
  - client sends {"eof": 1} to end the session
  - server sends {"partial": "..."} for partial hypotheses
  - server sends {"text": "..."} for final results

Why this exists: alphacep's own asr_server.py has a long-standing bug where
certain audio chunks make `vosk_recognizer_accept_waveform` raise a
TypeError deep in its ctypes/cffi layer. Their server doesn't guard against
this, so the exception propagates out of the connection handler and the
whole session dies with a 1011 close — killing live transcription for
whatever call was using it. Confirmed hitting both vosk-ky and vosk-ru in
this deployment repeatedly.

This wraps the exact same recognition engine (the `vosk` Python package —
same models, same Kaldi decoder, same accuracy) with a handler that treats
a recognizer error as "drop this one chunk and keep going" instead of
"tear down the connection". Same behavior for any other unexpected error
on a single chunk — the goal is that nothing short of the socket actually
closing can end a session early.
"""
import asyncio
import json
import logging
import os

import websockets
from vosk import KaldiRecognizer, Model, SetLogLevel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("asr-server")

MODEL_PATH = os.environ["VOSK_MODEL_PATH"]
HOST = os.environ.get("VOSK_SERVER_HOST", "0.0.0.0")
PORT = int(os.environ.get("VOSK_SERVER_PORT", "2700"))
DEFAULT_SAMPLE_RATE = int(os.environ.get("VOSK_DEFAULT_SAMPLE_RATE", "16000"))

SetLogLevel(-1)  # silence Kaldi's own console spam; we log at the connection level ourselves

log.info("loading model from %s ...", MODEL_PATH)
MODEL = Model(MODEL_PATH)
log.info("model loaded, listening on ws://%s:%d", HOST, PORT)


async def handle_connection(ws):
    peer = ws.remote_address
    sample_rate = DEFAULT_SAMPLE_RATE
    recognizer: KaldiRecognizer | None = None
    log.info("[%s] connection open", peer)

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
                    recognizer = KaldiRecognizer(MODEL, sample_rate)
                    recognizer.SetWords(False)
                    log.info("[%s] recognizer configured: sample_rate=%d", peer, sample_rate)
                    continue

                if "eof" in parsed:
                    if recognizer is not None:
                        try:
                            await ws.send(recognizer.FinalResult())
                        except Exception:
                            log.exception("[%s] error producing final result on eof", peer)
                    break

                log.info("[%s] unrecognized control message, ignoring: %r", peer, parsed)
                continue

            # Binary audio frame.
            if recognizer is None:
                # Audio arrived before any {"config": ...} — build one with
                # the default rate rather than silently dropping the call.
                recognizer = KaldiRecognizer(MODEL, sample_rate)
                recognizer.SetWords(False)
                log.warning("[%s] audio received before config; defaulting sample_rate=%d",
                            peer, sample_rate)

            try:
                if recognizer.AcceptWaveform(message):
                    await ws.send(recognizer.Result())
                else:
                    await ws.send(recognizer.PartialResult())
            except Exception:
                # This is the exact failure class that kills the upstream
                # reference server. Drop this one chunk, keep the
                # connection (and the call's transcript) alive.
                log.exception("[%s] recognizer error on one audio chunk — dropping it, continuing", peer)

    except websockets.exceptions.ConnectionClosed as e:
        log.info("[%s] connection closed: code=%s reason=%s", peer, e.code, e.reason)
    except Exception:
        log.exception("[%s] unexpected connection-level error", peer)
    finally:
        log.info("[%s] connection handler exiting", peer)


async def main():
    async with websockets.serve(handle_connection, HOST, PORT, max_size=None):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
