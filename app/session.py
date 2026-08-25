"""CallSession — owns one call end-to-end: audio in, routed STT, phrase
buffering, persistence, and live fan-out to subscribed WebSocket clients.

Phrase assembly: forwards "final" events downstream (tagged with the
producing engine, persisted to a per-call JSONL file) and live "partial"
events for real-time display. Consecutive repeats of the same partial or
final text within a segment are deduped so downstream only sees changes;
switching to a fallback engine starts a fresh segment.
"""
import asyncio
import audioop
import json
import logging
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


def _normalize(text: str) -> str:
    return " ".join(text.split())


class CallSession:
    def __init__(self, call_id: str, language: str, adapter_config: dict, broadcaster):
        self.call_id = call_id
        self.language = language
        self.chain = ROUTING.get(language, ROUTING["ru"])
        self.adapter_config = adapter_config
        self.broadcaster = broadcaster  # callable(call_id, event_dict)

        self._resample_state = None
        self._active_pcm_queue: asyncio.Queue | None = None
        self._event_queue: asyncio.Queue = asyncio.Queue()
        self._engine_task: asyncio.Task | None = None
        self._dispatcher_task: asyncio.Task | None = None
        self._chain_idx = 0
        self._audio_frames = 0
        self._transcript_path = TRANSCRIPT_DIR / f"{call_id}.jsonl"
        self._last_partial_text: str | None = None
        self._last_final_text: str | None = None

    async def start(self):
        self._dispatcher_task = asyncio.create_task(self._dispatch_events())
        await self._start_engine(0)

    async def _start_engine(self, idx: int):
        if idx >= len(self.chain):
            log.error("[%s] no more engines in fallback chain", self.call_id)
            return
        self._chain_idx = idx
        name, lang = self.chain[idx]
        adapter = build_adapter(name, lang, **self.adapter_config)
        self._active_pcm_queue = asyncio.Queue()
        self._last_partial_text = None
        self._last_final_text = None
        self._log(f"engine started: {name} ({lang})")
        self._engine_task = asyncio.create_task(
            adapter.stream(self._active_pcm_queue, self._event_queue)
        )

    async def feed(self, raw_8k: bytes):
        """Feed one AudioSocket audio payload (slin, 8kHz mono)."""
        self._audio_frames += 1
        pcm16k, self._resample_state = audioop.ratecv(
            raw_8k, 2, 1, 8000, 16000, self._resample_state
        )
        if self._active_pcm_queue is not None:
            await self._active_pcm_queue.put(pcm16k)

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
                record = {
                    "call_id": self.call_id,
                    "engine": name,
                    "text": text,
                    "ts": time.time(),
                }
                self._persist(record)
                await self.broadcaster(self.call_id, record)
            elif t == "partial":
                text = _normalize(ev.get("text", ""))
                if not text or text == self._last_partial_text:
                    continue
                self._last_partial_text = text
                await self.broadcaster(self.call_id, {
                    "call_id": self.call_id, "engine": name,
                    "partial": text, "ts": time.time(),
                })
            elif t == "error":
                self._log(f"[{name}] error: {ev.get('message')}")
            elif t == "fatal":
                self._log(f"[{name}] fatal ({ev.get('reason')}) — attempting fallback")
                await self._start_engine(self._chain_idx + 1)
            elif t == "done":
                self._log(f"[{name}] done")

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
        if self._active_pcm_queue is not None:
            await self._active_pcm_queue.put(None)
        if self._engine_task:
            try:
                await asyncio.wait_for(self._engine_task, timeout=10)
            except asyncio.TimeoutError:
                self._engine_task.cancel()
        if self._dispatcher_task:
            self._dispatcher_task.cancel()
        self._log(f"call ended, total audio frames: {self._audio_frames}")
