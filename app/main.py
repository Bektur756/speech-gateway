"""Speech Gateway — entrypoint.

Two AudioSocket TCP listeners (one per language, since AudioSocket's UUID
frame has no room for a language tag):

    port 9098  ->  routed to Vosk (ru)
    port 9099  ->  routed to AiRUN with Vosk-ky fallback

Point each language's queue/context at the matching port in the Asterisk
dialplan. Downstream consumers subscribe to a call's live transcript via:

    ws://<gateway>/calls/{call_id}/stream
"""
import asyncio
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from .audiosocket import read_frame, TYPE_UUID, TYPE_AUDIO, TYPE_ERROR, TYPE_TERMINATE
from .session import CallSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("main")

AIRUN_API_KEY = os.environ["AIRUN_API_KEY"]
VOSK_RU_URL = os.environ.get("VOSK_RU_URL", "ws://vosk-ru:2700")
VOSK_KY_URL = os.environ.get("VOSK_KY_URL", "ws://vosk-ky:2700")
AUDIOSOCKET_HOST = os.environ.get("AUDIOSOCKET_HOST", "0.0.0.0")
PORT_RU = int(os.environ.get("AUDIOSOCKET_PORT_RU", 9098))
PORT_KY = int(os.environ.get("AUDIOSOCKET_PORT_KY", 9099))
# Third adapter for quality comparison (ТЗ item 3) — not in the production
# ru/ky fallback chain, but selectable via WHISPER_MODEL_PATH for ad-hoc
# testing. Points at a local model dir (mounted like vosk-ky's model volume,
# see docker-compose.yml) so the container needs no internet access at
# runtime. small = realtime-safer on CPU, medium = better accuracy.
WHISPER_MODEL_PATH = os.environ.get("WHISPER_MODEL_PATH", "/models/kyrgyz-whisper-small")

ADAPTER_CONFIG = dict(vosk_ru_url=VOSK_RU_URL, vosk_ky_url=VOSK_KY_URL, airun_key=AIRUN_API_KEY,
                       whisper_model_id=WHISPER_MODEL_PATH)

# call_id -> list of subscriber queues (for the live WS API)
_subscribers: dict[str, list[asyncio.Queue]] = {}
_sessions: dict[str, CallSession] = {}


async def broadcast(call_id: str, event: dict):
    for q in _subscribers.get(call_id, []):
        await q.put(event)


async def handle_call(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, language: str):
    call_id = "unknown"
    session: CallSession | None = None
    peer = writer.get_extra_info("peername")
    log.info("AudioSocket connection from %s (lang=%s)", peer, language)

    try:
        while True:
            kind, payload = await read_frame(reader)

            if kind == TYPE_UUID:
                call_id = payload.hex()
                session = CallSession(call_id, language, ADAPTER_CONFIG, broadcast)
                _sessions[call_id] = session
                await session.start()

            elif kind == TYPE_AUDIO and session is not None:
                await session.feed(payload)

            elif kind in (TYPE_ERROR, TYPE_TERMINATE):
                break

    except asyncio.IncompleteReadError:
        pass
    finally:
        if session is not None:
            await session.close()
            _sessions.pop(call_id, None)
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    server_ru = await asyncio.start_server(
        lambda r, w: handle_call(r, w, "ru"), AUDIOSOCKET_HOST, PORT_RU
    )
    server_ky = await asyncio.start_server(
        lambda r, w: handle_call(r, w, "ky"), AUDIOSOCKET_HOST, PORT_KY
    )
    log.info("AudioSocket listening: ru=%s:%s ky=%s:%s", AUDIOSOCKET_HOST, PORT_RU, AUDIOSOCKET_HOST, PORT_KY)
    asyncio.create_task(server_ru.serve_forever())
    asyncio.create_task(server_ky.serve_forever())
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok", "active_calls": list(_sessions.keys())}


@app.websocket("/calls/{call_id}/stream")
async def stream_call(ws: WebSocket, call_id: str):
    await ws.accept()
    q: asyncio.Queue = asyncio.Queue()
    _subscribers.setdefault(call_id, []).append(q)
    try:
        while True:
            event = await q.get()
            await ws.send_json(event)
    except WebSocketDisconnect:
        pass
    finally:
        _subscribers[call_id].remove(q)
