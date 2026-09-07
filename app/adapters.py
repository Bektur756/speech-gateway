"""STT engine adapters — Vosk (self-hosted), AiRUN (cloud), and Whisper
(local, optional, for quality comparison).

Every adapter exposes the same coroutine signature:

    async def stream(self, pcm_queue: asyncio.Queue, event_queue: asyncio.Queue) -> None

It reads PCM16/16kHz chunks from pcm_queue (a None sentinel means "end of
call") and pushes dict events onto event_queue:

    {"type": "ready"}
    {"type": "partial", "text": "..."}
    {"type": "final", "text": "...", "start": 1.2}
    {"type": "done"}
    {"type": "error", "message": "..."}
    {"type": "fatal", "reason": "..."}   # tells CallSession to fall back

A "fatal" event is the fallback trigger — anything the caller should treat
as "this engine can no longer serve this call" (auth failure, AiRUN balance
exhausted, connection refused, etc).
"""
import abc
import asyncio
import json
import logging

import websockets

log = logging.getLogger("adapters")

# AiRUN close codes that mean "this engine is now unusable for this call"
AIRUN_FATAL_CODES = {4402, 4403, 4408}


class STTAdapter(abc.ABC):
    name: str = "base"

    @abc.abstractmethod
    async def stream(self, pcm_queue: asyncio.Queue, event_queue: asyncio.Queue) -> None:
        ...


class VoskAdapter(STTAdapter):
    name = "vosk"

    # The alphacep vosk-server image has a recurring bug (confirmed
    # third-party, not ours) that kills the connection mid-session with a
    # 1011 close code. One reconnect attempt keeps a single crash from
    # taking down the whole comparison track for the rest of the call.
    MAX_RECONNECTS = 1

    def __init__(self, ws_url: str):
        self.ws_url = ws_url  # e.g. ws://vosk-ru:2700 or ws://vosk-ky:2700

    async def stream(self, pcm_queue, event_queue):
        eof_sent = False
        attempt = 0
        while True:
            try:
                # ping_interval disabled: vosk-server's decode call is a
                # synchronous cffi call into Kaldi that holds the GIL for
                # its whole duration, so under concurrent connections
                # (dual-leg calls open 2+ at once) its event loop can go
                # a while without a chance to answer a ping — a real but
                # harmless delay, not a dead connection. websockets'
                # default ping/pong keepalive was treating that delay as
                # a dead peer and force-closing with 1011, which is worse
                # than just letting a slow decode run long. The actual
                # liveness signal here is simpler and stronger anyway:
                # audio keeps arriving, or eof/the socket closing tells us
                # the call ended.
                async with websockets.connect(
                    self.ws_url, open_timeout=15, ping_interval=None
                ) as ws:
                    await ws.send(json.dumps({"config": {"sample_rate": 16000}}))
                    if attempt == 0:
                        await event_queue.put({"type": "ready"})

                    async def sender():
                        nonlocal eof_sent
                        while True:
                            chunk = await pcm_queue.get()
                            if chunk is None:
                                await ws.send(json.dumps({"eof": 1}))
                                eof_sent = True
                                return
                            await ws.send(chunk)

                    async def receiver():
                        async for raw in ws:
                            msg = json.loads(raw)
                            if "partial" in msg and msg["partial"]:
                                await event_queue.put({"type": "partial", "text": msg["partial"]})
                            elif "text" in msg and msg["text"]:
                                await event_queue.put({"type": "final", "text": msg["text"]})

                    if eof_sent:
                        # Audio side already finished before the drop — this
                        # reconnect is only to collect whatever final result
                        # the crash cut off; pcm_queue is already drained,
                        # so re-running sender() would just hang.
                        await ws.send(json.dumps({"eof": 1}))
                        await receiver()
                    else:
                        await asyncio.gather(sender(), receiver())
                await event_queue.put({"type": "done"})
                return
            except Exception as e:
                attempt += 1
                if attempt > self.MAX_RECONNECTS:
                    log.warning("vosk stream failed (%s): %s", self.ws_url, e)
                    await event_queue.put({"type": "fatal", "reason": str(e)})
                    return
                log.warning("vosk stream dropped (%s), reconnecting (%d/%d): %s",
                            self.ws_url, attempt, self.MAX_RECONNECTS, e)


class AiRUNAdapter(STTAdapter):
    name = "airun"
    URL_TMPL = "wss://api.airun.kg/v1/airun-asr-realtime/stream?language={lang}"

    def __init__(self, api_key: str, lang: str):
        self.api_key = api_key
        self.lang = lang

    async def stream(self, pcm_queue, event_queue):
        url = self.URL_TMPL.format(lang=self.lang)
        headers = {"Authorization": f"Bearer {self.api_key}"}
        try:
            async with websockets.connect(url, additional_headers=headers, open_timeout=5) as ws:
                ready = asyncio.Event()

                async def sender():
                    await ready.wait()
                    while True:
                        chunk = await pcm_queue.get()
                        if chunk is None:
                            await ws.send(json.dumps({"type": "stop"}))
                            return
                        await ws.send(chunk)

                async def receiver():
                    async for raw in ws:
                        msg = json.loads(raw)
                        t = msg.get("type")
                        if t == "ready":
                            ready.set()
                            await event_queue.put({"type": "ready"})
                        elif t == "partial":
                            await event_queue.put({"type": "partial", "text": msg.get("text", "")})
                        elif t == "final":
                            await event_queue.put({"type": "final", "text": msg.get("text", ""),
                                                    "start": msg.get("start")})
                        elif t == "done":
                            await event_queue.put({"type": "done"})
                            return
                        elif t == "error":
                            await event_queue.put({"type": "error", "message": msg.get("message")})
                            await event_queue.put({"type": "fatal", "reason": "airun_error"})
                            return

                await asyncio.gather(sender(), receiver())
        except websockets.exceptions.ConnectionClosed as e:
            if e.code in AIRUN_FATAL_CODES:
                log.warning("AiRUN closed with fatal code %s", e.code)
            await event_queue.put({"type": "fatal", "reason": f"ws_close_{e.code}"})
        except Exception as e:
            log.warning("AiRUN stream failed: %s", e)
            await event_queue.put({"type": "fatal", "reason": str(e)})


class WhisperAdapter(STTAdapter):
    """Windowed pseudo-streaming adapter around a local Whisper model.

    Whisper has no native streaming mode — it transcribes fixed windows of
    audio, typically up to 30s. To get a pseudo-realtime "partial" effect,
    this adapter keeps a rolling buffer of the last `window_seconds` of
    audio and re-transcribes it every `hop_seconds` of new audio; whatever
    is left in the buffer when the call ends is emitted as "final".

    Loads from a local model directory (mounted into the container, same
    pattern as vosk-ky's model volume) rather than the Hugging Face Hub id,
    so the container doesn't need internet access at runtime. Not part of
    the production ru/ky fallback chain — used for quality comparison on
    mixed ky/ru speech per ТЗ item 3. Real RTF on this deployment's CPU has
    not been measured — if inference can't keep up with real-time, this
    needs a GPU.
    """
    name = "whisper"
    SAMPLE_RATE = 16000
    BYTES_PER_SAMPLE = 2

    # Verified locally against nineninesix/kyrgyz-whisper-small revision
    # da514a5: the model's generation_config.json only carries Whisper's
    # original 99-language tag set, which has no "ky" — forcing
    # language="ky" via the standard pipeline raises ValueError. The model
    # card documents a workaround (trust_remote_code=True, custom
    # tokenizer with a real <|ky|> token), but that custom tokenizer's own
    # decode() throws TypeError on every language, not just ky — so it's
    # not currently usable end-to-end. Until upstream fixes that, skip
    # forcing language for these tags and let the model's fine-tuning
    # bias the auto-detected output instead.
    _UNSUPPORTED_LANGUAGE_TAGS = {"ky"}

    def __init__(self, lang: str | None = None,
                 model_id: str = "/models/kyrgyz-whisper-small",
                 window_seconds: float = 8.0, hop_seconds: float = 2.0):
        self.lang = lang
        self.model_id = model_id
        self.window_seconds = window_seconds
        self.hop_seconds = hop_seconds
        self._pipe = None

    def _load(self):
        if self._pipe is None:
            import torch
            from transformers import pipeline
            self._pipe = pipeline(
                "automatic-speech-recognition",
                model=self.model_id,
                device="cuda" if torch.cuda.is_available() else "cpu",
            )
        return self._pipe

    def _transcribe(self, pcm16: bytes) -> str:
        import numpy as np
        pipe = self._load()
        audio = np.frombuffer(pcm16, dtype="<i2").astype("float32") / 32768.0
        lang = self.lang if self.lang not in self._UNSUPPORTED_LANGUAGE_TAGS else None
        generate_kwargs = {"language": lang} if lang else {}
        result = pipe({"array": audio, "sampling_rate": self.SAMPLE_RATE},
                       generate_kwargs=generate_kwargs)
        return result.get("text", "").strip()

    async def stream(self, pcm_queue, event_queue):
        try:
            await asyncio.to_thread(self._load)
            await event_queue.put({"type": "ready"})

            window_bytes = int(self.window_seconds * self.SAMPLE_RATE) * self.BYTES_PER_SAMPLE
            hop_bytes = int(self.hop_seconds * self.SAMPLE_RATE) * self.BYTES_PER_SAMPLE
            buf = bytearray()
            unprocessed = 0

            while True:
                chunk = await pcm_queue.get()
                if chunk is None:
                    if buf:
                        text = await asyncio.to_thread(self._transcribe, bytes(buf))
                        if text:
                            await event_queue.put({"type": "final", "text": text})
                    await event_queue.put({"type": "done"})
                    return

                buf.extend(chunk)
                unprocessed += len(chunk)
                if len(buf) > window_bytes:
                    del buf[: len(buf) - window_bytes]

                if unprocessed >= hop_bytes:
                    unprocessed = 0
                    text = await asyncio.to_thread(self._transcribe, bytes(buf))
                    if text:
                        await event_queue.put({"type": "partial", "text": text})
        except Exception as e:
            log.warning("whisper stream failed: %s", e)
            await event_queue.put({"type": "fatal", "reason": str(e)})


# Reused across calls, keyed by (lang, model_id) — each fresh WhisperAdapter
# reloads the full model into memory (~1GB, confirmed in production: the
# gateway process grew to 5.2GB after a handful of calls, all reclaimed
# instantly by a restart), and that memory was never released between
# calls. Loading once and reusing the same instance fixes the leak and
# skips the reload cost on every subsequent call.
_whisper_adapter_cache: dict[tuple, "WhisperAdapter"] = {}
_whisper_adapter_cache_lock = asyncio.Lock()


async def transcribe_recording(wav_path, lang: str | None, model_id: str,
                                chunk_seconds: float = 20.0) -> list[tuple[float, str]]:
    """Offline, non-streaming transcription of a full call recording.

    Unlike WhisperAdapter.stream's overlapping rolling window (built to
    keep producing an improving "partial" during a live call), this has no
    real-time deadline — it walks the recording in sequential,
    non-overlapping chunks so every second of audio is transcribed exactly
    once. Returns a list of (offset_seconds, text) for each non-empty
    chunk. Still bound by the same measured RTF (~3x on this CPU) — a
    2-minute recording takes several minutes to process; that's expected
    and fine for a background job with no one waiting on it live.
    """
    import wave

    cache_key = (lang, model_id)
    async with _whisper_adapter_cache_lock:
        adapter = _whisper_adapter_cache.get(cache_key)
        if adapter is None:
            adapter = WhisperAdapter(lang=lang, model_id=model_id)
            await asyncio.to_thread(adapter._load)
            _whisper_adapter_cache[cache_key] = adapter

    results = []
    with wave.open(str(wav_path), "rb") as wf:
        assert wf.getframerate() == WhisperAdapter.SAMPLE_RATE, \
            f"expected {WhisperAdapter.SAMPLE_RATE}Hz recording, got {wf.getframerate()}"
        frames_per_chunk = int(chunk_seconds * wf.getframerate())
        offset = 0.0
        while True:
            data = wf.readframes(frames_per_chunk)
            if not data:
                break
            text = await asyncio.to_thread(adapter._transcribe, data)
            if text:
                results.append((offset, text))
            offset += chunk_seconds
    return results


def build_adapter(name: str, lang: str, *, vosk_ru_url: str, vosk_ky_url: str, airun_key: str,
                   whisper_model_id: str = "/models/kyrgyz-whisper-small") -> STTAdapter:
    if name == "vosk":
        url = vosk_ru_url if lang == "ru" else vosk_ky_url
        return VoskAdapter(url)
    if name == "vosk-ru":
        # Distinct name from "vosk" (which already handles ru/ky by lang)
        # so this can run as its own tagged track alongside a vosk-ky
        # track in the same call — e.g. comparing the ru model against the
        # ky model on the same ky-language audio — without both landing
        # under the same "vosk" engine tag / merged file.
        return VoskAdapter(vosk_ru_url)
    if name == "airun":
        return AiRUNAdapter(airun_key, lang)
    if name == "whisper":
        return WhisperAdapter(lang=lang, model_id=whisper_model_id)
    raise ValueError(f"unknown engine: {name}")
