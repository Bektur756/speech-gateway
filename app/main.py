"""Speech Gateway — entrypoint.

AudioSocket TCP listeners — port choice is the only way to tag a connection
(the AudioSocket protocol carries just a UUID, no room for language/role):

    port 9098  ->  ru,          routed to Vosk
    port 9099  ->  ky,          routed to AiRUN with Vosk-ky fallback
    port 9100  ->  ky + client,   dual-leg capture (see below)
    port 9101  ->  ky + operator, dual-leg capture (see below)

9098/9099 are for a single AudioSocket connection carrying one call's whole
audio (simple queue/context routing in the Asterisk dialplan). 9100/9101
are for *dual-leg* capture: an ARI controller running on the Asterisk box
snoops the client's and operator's audio separately from an already-bridged
call and streams each as its own AudioSocket connection, so client and
operator get independent transcripts instead of one mixed stream. Language
for the dual-leg ports is hardcoded to ky for now — add ru variants the
same way if that becomes needed.

Downstream consumers subscribe to a call's live transcript via:

    ws://<gateway>/calls/{call_id}/stream

Client and operator legs land in separate per-call JSONL files by default
(each is its own CallSession/call_id). The ARI controller can additionally
POST /conversations/{conversation_id}/legs {"role", "call_id"} for each leg
— once linked, every "final" record for that call_id is also appended to
data/transcripts/conversations/{conversation_id}-{engine}.jsonl (one file
per STT engine — e.g. "-airun.jsonl", "-vosk.jsonl"), each holding both
client and operator lines in chronological order, for a clean side-by-side
comparison between engines at the end of a call.

A linked call's audio is also recorded (see session.py); once it hangs up,
an offline Whisper pass runs over the whole recording (no real-time
deadline, unlike the live comparison track — see adapters.py) and its
result lands in a third file, {conversation_id}-whisper.jsonl, same shape
as the other two.
"""
import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

from .adapters import transcribe_recording
from .audiosocket import read_frame, TYPE_UUID, TYPE_ERROR, TYPE_TERMINATE, AUDIO_SAMPLE_RATES
from .session import CallSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("main")

AIRUN_API_KEY = os.environ["AIRUN_API_KEY"]
VOSK_RU_URL = os.environ.get("VOSK_RU_URL", "ws://vosk-ru:2700")
VOSK_KY_URL = os.environ.get("VOSK_KY_URL", "ws://vosk-ky:2700")
AUDIOSOCKET_HOST = os.environ.get("AUDIOSOCKET_HOST", "0.0.0.0")
PORT_RU = int(os.environ.get("AUDIOSOCKET_PORT_RU", 9098))
PORT_KY = int(os.environ.get("AUDIOSOCKET_PORT_KY", 9099))
PORT_KY_CLIENT = int(os.environ.get("AUDIOSOCKET_PORT_KY_CLIENT", 9100))
PORT_KY_OPERATOR = int(os.environ.get("AUDIOSOCKET_PORT_KY_OPERATOR", 9101))
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

# Dual-leg conversations: the ARI controller registers each leg's call_id
# under a shared conversation_id (its Asterisk bridge id) so their "final"
# records can be merged per engine — one file per STT engine, each holding
# both client and operator lines in chronological order, for a clean
# side-by-side comparison at the end of a call. AudioSocket's protocol has
# no room to carry that link itself (just a UUID), so it comes in over this
# separate HTTP call instead.
_call_to_conversation: dict[str, str] = {}
CONVERSATIONS_DIR = Path("/data/transcripts/conversations")
CONVERSATIONS_DIR.mkdir(parents=True, exist_ok=True)


async def broadcast(call_id: str, event: dict):
    for q in _subscribers.get(call_id, []):
        await q.put(event)
    conversation_id = _call_to_conversation.get(call_id)
    if conversation_id and "text" in event:
        engine = event.get("engine", "unknown")
        path = CONVERSATIONS_DIR / f"{conversation_id}-{engine}.jsonl"
        with open(path, "a") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")


async def _run_offline_whisper(call_id: str, role: str, conversation_id: str, recording_path: Path):
    """Background job kicked off when a linked dual-leg call ends: transcribes
    its full recording with Whisper (no real-time deadline here, unlike the
    live comparison track) and appends the result into the third
    per-conversation file, same shape as the live airun/vosk ones."""
    try:
        chunks = await transcribe_recording(recording_path, "ky", WHISPER_MODEL_PATH)
        for offset, text in chunks:
            record = {
                "call_id": call_id, "engine": "whisper", "text": text,
                "role": role, "offset_seconds": offset, "ts": time.time(),
            }
            # broadcast() already appends to CONVERSATIONS_DIR/{id}-whisper.jsonl
            # (conversation_id is still linked) and pushes to any live WS
            # subscribers — same path every other engine's finals go through.
            await broadcast(call_id, record)
        log.info("[%s] offline whisper transcription complete (%d chunks)", call_id, len(chunks))
    except Exception:
        log.exception("[%s] offline whisper transcription failed", call_id)


async def handle_call(reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                       language: str, role: str | None = None):
    call_id = "unknown"
    session: CallSession | None = None
    peer = writer.get_extra_info("peername")
    log.info("AudioSocket connection from %s (lang=%s role=%s)", peer, language, role)

    try:
        while True:
            kind, payload = await read_frame(reader)

            if kind == TYPE_UUID:
                call_id = payload.hex()
                session = CallSession(call_id, language, ADAPTER_CONFIG, broadcast, role=role)
                _sessions[call_id] = session
                await session.start()

            elif kind in AUDIO_SAMPLE_RATES and session is not None:
                await session.feed(payload, sample_rate=AUDIO_SAMPLE_RATES[kind])

            elif kind in (TYPE_ERROR, TYPE_TERMINATE):
                break

    except asyncio.IncompleteReadError:
        pass
    finally:
        if session is not None:
            await session.close()
            _sessions.pop(call_id, None)
            conversation_id = _call_to_conversation.get(call_id)
            if conversation_id and session.recording_path is not None:
                asyncio.create_task(
                    _run_offline_whisper(call_id, role, conversation_id, session.recording_path)
                )
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
    server_ky_client = await asyncio.start_server(
        lambda r, w: handle_call(r, w, "ky", role="client"), AUDIOSOCKET_HOST, PORT_KY_CLIENT
    )
    server_ky_operator = await asyncio.start_server(
        lambda r, w: handle_call(r, w, "ky", role="operator"), AUDIOSOCKET_HOST, PORT_KY_OPERATOR
    )
    log.info(
        "AudioSocket listening: ru=%s:%s ky=%s:%s ky-client=%s:%s ky-operator=%s:%s",
        AUDIOSOCKET_HOST, PORT_RU, AUDIOSOCKET_HOST, PORT_KY,
        AUDIOSOCKET_HOST, PORT_KY_CLIENT, AUDIOSOCKET_HOST, PORT_KY_OPERATOR,
    )
    asyncio.create_task(server_ru.serve_forever())
    asyncio.create_task(server_ky.serve_forever())
    asyncio.create_task(server_ky_client.serve_forever())
    asyncio.create_task(server_ky_operator.serve_forever())
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok", "active_calls": list(_sessions.keys())}


class ConversationLeg(BaseModel):
    role: str
    call_id: str


@app.post("/conversations/{conversation_id}/legs")
async def link_conversation_leg(conversation_id: str, leg: ConversationLeg):
    _call_to_conversation[leg.call_id] = conversation_id
    return {"status": "ok"}


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
