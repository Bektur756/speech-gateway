"""Speech Gateway — entrypoint.

AudioSocket TCP listeners — port choice is the only way to tag a connection
(the AudioSocket protocol carries just a UUID, no room for language/role):

    port 9098  ->  ru,          routed to Vosk-ru
    port 9099  ->  ky,          routed to Vosk-ky
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
per STT engine, each holding both client and operator lines in
chronological order, for a clean side-by-side comparison at the end of a
call). Two files per linked call, both live (-vosk-ky, -vosk-ru — see
session.py's ROUTING/PARALLEL_ENGINES; AiRUN is defined in adapters.py but
not currently wired into either), all same shape.

A linked call's audio is also recorded (see session.py); once it hangs up,
an offline Whisper pass runs over the whole recording (no real-time
deadline, unlike the live tracks — see adapters.py / _OFFLINE_WHISPER_PASSES)
and lands in the last file.
"""
import asyncio
import audioop
import json
import logging
import os
import re
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from dotenv import load_dotenv

from fastapi import FastAPI, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from pywebpush import webpush, WebPushException

from .adapters import transcribe_recording
from .audiosocket import read_frame, TYPE_UUID, TYPE_ERROR, TYPE_TERMINATE, AUDIO_SAMPLE_RATES
from .session import CallSession

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("main")

AIRUN_API_KEY = os.environ["AIRUN_API_KEY"]
VOSK_RU_URL = os.environ.get("VOSK_RU_URL", "ws://vosk-ru:2700")
VOSK_KY_URL = os.environ.get("VOSK_KY_URL", "ws://vosk-ky:2700")
GIGAAM_URL = os.environ.get("GIGAAM_URL", "ws://gigaam:2700")
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
AUDIOSOCKET_IDLE_TIMEOUT_SECONDS = float(os.environ.get("AUDIOSOCKET_IDLE_TIMEOUT_SECONDS", "30"))
# FreeSWITCH's own audio-streaming path (see freeswitch_audio below) — a
# stereo L16 WebSocket feed, one call per connection, split into client and
# operator PCM here rather than carried over AudioSocket like the Asterisk
# side.
FREESWITCH_AUDIO_LANGUAGE = os.environ.get("FREESWITCH_AUDIO_LANGUAGE", "ky")
FREESWITCH_AUDIO_SAMPLE_RATE = int(os.environ.get("FREESWITCH_AUDIO_SAMPLE_RATE", "16000"))

ADAPTER_CONFIG = dict(vosk_ru_url=VOSK_RU_URL, vosk_ky_url=VOSK_KY_URL, airun_key=AIRUN_API_KEY,
                       gigaam_url=GIGAAM_URL, whisper_model_id=WHISPER_MODEL_PATH)

# --- Push notification on new call (browser Web Push, no app required) ---
# Standard W3C Push API: the browser itself is the client, via a service
# worker — nothing to install from an app store, just a page visited once
# to hit "Enable notifications". Delivery still goes through the browser
# vendor's own push relay (Google/Mozilla/Apple — that's how Web Push works
# for every site that uses it), but there's no account, API key, or SDK of
# ours in the loop. Generate a VAPID key pair once (see README/deploy notes)
# and set these three; leave VAPID_PUBLIC_KEY unset to disable.
VAPID_PUBLIC_KEY = os.environ.get("VAPID_PUBLIC_KEY", "")
VAPID_PRIVATE_KEY_PATH = os.environ.get("VAPID_PRIVATE_KEY_PATH", "private_key.pem")
VAPID_SUBJECT = os.environ.get("VAPID_SUBJECT", "mailto:admin@example.com")

# Subscriptions are what registration.pushManager.subscribe() hands back in
# the browser — one per browser/device that clicked "Enable notifications".
# Persisted to disk (same /data volume as everything else here) so they
# survive a gateway restart instead of forcing everyone to re-subscribe.
PUSH_SUBSCRIPTIONS_PATH = Path("/data/push_subscriptions.json")
PUSH_SUBSCRIPTIONS_PATH.parent.mkdir(parents=True, exist_ok=True)


def _load_push_subscriptions() -> list[dict]:
    try:
        return json.loads(PUSH_SUBSCRIPTIONS_PATH.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return []


_push_subscriptions: list[dict] = _load_push_subscriptions()


def _save_push_subscriptions():
    PUSH_SUBSCRIPTIONS_PATH.write_text(json.dumps(_push_subscriptions, ensure_ascii=False))

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

# Live dashboard (see LIVE_HTML / /live / /live/ws below) — every broadcast
# event also lands here regardless of per-call-id subscription, keyed by
# conversation_id where linked (so a dual-leg call's client+operator lines
# show up together) or by call_id otherwise. History is capped per key so a
# long-running call doesn't grow this unboundedly.
_live_subscribers: list[asyncio.Queue] = []
_live_history: dict[str, list[dict]] = {}
_SAFE_ID_RE = re.compile(r"[^A-Za-z0-9_.-]+")


def _write_conversation_line(path: Path, event: dict):
    # Self-healing: CONVERSATIONS_DIR is only created once at startup, so if
    # it's deleted while the gateway is running (e.g. manual cleanup),
    # recreate it rather than fail every write from here on.
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as f:
        f.write(json.dumps(event, ensure_ascii=False) + "\n")


async def broadcast(call_id: str, event: dict):
    for q in _subscribers.get(call_id, []):
        await q.put(event)
    conversation_id = _call_to_conversation.get(call_id)

    live_event = dict(event)
    if conversation_id:
        live_event["conversation_id"] = conversation_id
    live_event.setdefault("call_id", call_id)
    history_key = conversation_id or call_id
    history = _live_history.setdefault(history_key, [])
    history.append(live_event)
    del history[:-500]
    for q in list(_live_subscribers):
        await q.put(live_event)

    if conversation_id and "text" in event:
        engine = event.get("engine", "unknown")
        path = CONVERSATIONS_DIR / f"{conversation_id}-{engine}.jsonl"
        try:
            # Off the event loop — this call path runs on the same loop
            # that drains the AudioSocket TCP streams in real time, and a
            # blocking disk write here can stall it under I/O pressure. See
            # the matching fix on CallSession.feed's WAV write in
            # session.py for why that matters on this box specifically.
            await asyncio.to_thread(_write_conversation_line, path, event)
        except OSError:
            log.exception("[%s] failed to write conversation transcript %s", call_id, path)


def _safe_id(value: object, fallback: str) -> str:
    raw = str(value or fallback)
    cleaned = _SAFE_ID_RE.sub("_", raw).strip("._-")
    return cleaned[:128] or fallback


def _active_call_ids() -> list[str]:
    """Collapse the client and operator sessions into one visible call UUID."""
    return sorted({_call_to_conversation.get(call_id, call_id) for call_id in _sessions})


def _send_push(title: str, body: str):
    # Synchronous by design (pywebpush/requests has no async variant) —
    # always called via asyncio.to_thread from notify_new_call, never
    # directly on the event loop.
    if not _push_subscriptions:
        return
    payload = json.dumps({"title": title, "body": body})
    stale = []
    for sub in list(_push_subscriptions):
        try:
            webpush(
                subscription_info=sub,
                data=payload,
                vapid_private_key=VAPID_PRIVATE_KEY_PATH,
                vapid_claims={"sub": VAPID_SUBJECT},
            )
        except WebPushException as ex:
            status = ex.response.status_code if ex.response is not None else None
            if status in (404, 410):
                # Browser/OS says this subscription is gone for good
                # (uninstalled, permission revoked, etc.) — drop it rather
                # than retrying it on every future call.
                stale.append(sub)
            else:
                log.warning("push notification failed: %s", ex)
        except Exception:
            log.exception("push notification failed")
    if stale:
        for sub in stale:
            _push_subscriptions.remove(sub)
        _save_push_subscriptions()


def notify_new_call(call_id: str, language: str, role: str | None = None):
    """Fire a push notification for a newly-started call. Schedules the
    (blocking) HTTP POST(s) on a background thread and returns immediately —
    never awaited, never raises into the caller, and a no-op if no VAPID key
    is configured."""
    if not VAPID_PUBLIC_KEY:
        return
    detail = f"{call_id} ({language}{', ' + role if role else ''})"
    asyncio.create_task(asyncio.to_thread(_send_push, "New call", detail))


PUSH_SERVICE_WORKER_JS = """
self.addEventListener('push', (event) => {
  let data = {};
  try { data = event.data ? event.data.json() : {}; } catch (e) { /* ignore */ }
  event.waitUntil(self.registration.showNotification(data.title || 'New call', {
    body: data.body || '',
    tag: 'new-call',
  }));
});

self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  event.waitUntil(clients.openWindow('/live'));
});
"""


class PushSubscription(BaseModel):
    endpoint: str
    keys: dict


async def _start_freeswitch_stereo_sessions(metadata: dict) -> tuple[str, CallSession, CallSession]:
    base_id = _safe_id(
        metadata.get("uuid") or metadata.get("call_id") or metadata.get("Unique-ID"),
        f"fs-{int(time.time() * 1000)}",
    )
    language = str(metadata.get("language") or FREESWITCH_AUDIO_LANGUAGE)
    client_id = f"{base_id}-client"
    operator_id = f"{base_id}-operator"

    client_session = CallSession(client_id, language, ADAPTER_CONFIG, broadcast, role="client")
    operator_session = CallSession(operator_id, language, ADAPTER_CONFIG, broadcast, role="operator")
    _sessions[client_id] = client_session
    _sessions[operator_id] = operator_session
    _call_to_conversation[client_id] = base_id
    _call_to_conversation[operator_id] = base_id

    await client_session.start()
    await operator_session.start()
    notify_new_call(base_id, language)
    log.info(
        "[%s] FreeSWITCH stereo stream started: client=%s operator=%s metadata=%s",
        base_id, client_id, operator_id, metadata,
    )
    return base_id, client_session, operator_session


def _split_stereo_l16(frame: bytes) -> tuple[bytes, bytes]:
    if len(frame) % 4:
        log.warning("FreeSWITCH stereo frame had odd byte count=%d; trimming", len(frame))
        frame = frame[:len(frame) - (len(frame) % 4)]
    return audioop.tomono(frame, 2, 1, 0), audioop.tomono(frame, 2, 0, 1)


_offline_whisper_lock = asyncio.Lock()


# lang passed to transcribe_recording -> tag used for the "engine" field
# (and thus the merged filename). Just the model's native mode: "ky" means
# unforced/auto-detect (the model's generation_config has no "ky" tag — see
# adapters.WhisperAdapter). A forced-"ru" second pass was tried and dropped
# — same model weights either way, so it's a much smaller comparison than
# vosk-ky vs vosk-ru (genuinely different models), and it doubled offline
# processing time (RTF~3x per pass) for little payoff.
#
# Left empty: WHISPER_MODEL_PATH's volume mount is commented out in
# docker-compose.yml (RTF=3.06 on this CPU-only box — see that file), so
# turning this on would just fail on every call that ends. Restore
# [("ky", "whisper-ky")] together with mounting a model volume.
_OFFLINE_WHISPER_PASSES = []


async def _run_offline_whisper(call_id: str, role: str, conversation_id: str, recording_path: Path):
    """Background job kicked off when a linked dual-leg call ends: runs both
    Whisper passes (see _OFFLINE_WHISPER_PASSES) over its full recording (no
    real-time deadline here, unlike the live comparison track) and appends
    each into its own per-conversation file, same shape as the live
    airun/vosk ones.

    Both legs of a call normally end within moments of each other, so their
    jobs would otherwise run concurrently — confirmed in practice to trip a
    thread-safety bug in transformers' lazy module import the first time two
    threads hit it at once (ImportError: cannot import name 'pipeline').
    Serializing with a lock avoids that race, and avoids heavy CPU-bound
    inference jobs (RTF~3x each, and now two passes per leg) contending for
    the same cores anyway — at the cost of taking longer to appear overall.
    """
    for lang, tag in _OFFLINE_WHISPER_PASSES:
        try:
            async with _offline_whisper_lock:
                chunks = await transcribe_recording(recording_path, lang, WHISPER_MODEL_PATH)
            for offset, text in chunks:
                record = {
                    "call_id": call_id, "engine": tag, "text": text,
                    "role": role, "offset_seconds": offset, "ts": time.time(),
                }
                # broadcast() already appends to
                # CONVERSATIONS_DIR/{id}-{tag}.jsonl (conversation_id is
                # still linked) and pushes to any live WS subscribers —
                # same path every other engine's finals go through.
                await broadcast(call_id, record)
            log.info("[%s] offline %s transcription complete (%d chunks)", call_id, tag, len(chunks))
        except Exception:
            log.exception("[%s] offline %s transcription failed", call_id, tag)


async def handle_call(reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                       language: str, role: str | None = None):
    call_id = "unknown"
    session: CallSession | None = None
    peer = writer.get_extra_info("peername")
    log.info("AudioSocket connection from %s (lang=%s role=%s)", peer, language, role)

    try:
        while True:
            if session is None:
                kind, payload = await read_frame(reader)
            else:
                try:
                    kind, payload = await asyncio.wait_for(
                        read_frame(reader), timeout=AUDIOSOCKET_IDLE_TIMEOUT_SECONDS
                    )
                except asyncio.TimeoutError:
                    log.warning(
                        "[%s] no AudioSocket frames for %.1fs (role=%s); closing stream",
                        call_id, AUDIOSOCKET_IDLE_TIMEOUT_SECONDS, role,
                    )
                    break

            if kind == TYPE_UUID:
                call_id = payload.hex()
                session = CallSession(call_id, language, ADAPTER_CONFIG, broadcast, role=role)
                _sessions[call_id] = session
                await session.start()
                # Skip the "operator" leg of a dual-leg call: the ARI
                # controller opens the client leg (port 9100) alongside it
                # for the same physical call, so notifying on that leg only
                # keeps this to one push per call instead of two. Single-leg
                # ru/ky calls (role is None) always notify.
                if role != "operator":
                    notify_new_call(call_id, language, role)

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
            if _OFFLINE_WHISPER_PASSES and conversation_id and session.recording_path is not None:
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


@app.get("/sw.js")
async def push_service_worker():
    return Response(content=PUSH_SERVICE_WORKER_JS, media_type="application/javascript")


@app.get("/push/vapid-public-key", response_class=PlainTextResponse)
async def push_vapid_public_key():
    return VAPID_PUBLIC_KEY


@app.post("/push/subscribe")
async def push_subscribe(sub: PushSubscription):
    entry = sub.dict()
    if entry not in _push_subscriptions:
        _push_subscriptions.append(entry)
        await asyncio.to_thread(_save_push_subscriptions)
    return {"status": "ok"}


@app.post("/push/test")
async def push_test():
    """Diagnostic only — unlike notify_new_call (fire-and-forget, used on
    the real call-start path so it never blocks audio handling), this
    awaits each send and reports the real per-subscription result, so you
    can actually see *why* nothing arrived instead of always getting back
    a blind "ok"."""
    if not VAPID_PUBLIC_KEY:
        return {"error": "VAPID_PUBLIC_KEY is not set — push is disabled"}
    if not _push_subscriptions:
        return {"error": "no saved subscriptions — open /live and click "
                         "'Enable notifications' first"}

    def _send_one(sub: dict):
        webpush(
            subscription_info=sub,
            data=json.dumps({"title": "Test push", "body": "It works"}),
            vapid_private_key=VAPID_PRIVATE_KEY_PATH,
            vapid_claims={"sub": VAPID_SUBJECT},
        )

    results = []
    for sub in _push_subscriptions:
        endpoint = sub.get("endpoint", "?")[:60] + "..."
        try:
            await asyncio.to_thread(_send_one, sub)
            results.append({"endpoint": endpoint, "status": "sent"})
        except WebPushException as ex:
            status = ex.response.status_code if ex.response is not None else None
            results.append({
                "endpoint": endpoint, "status": "failed",
                "http_status": status, "error": str(ex),
            })
        except Exception as ex:
            # e.g. VAPID_PRIVATE_KEY_PATH not readable — fails before any
            # network call is even made, never a WebPushException.
            results.append({
                "endpoint": endpoint, "status": "failed",
                "error": f"{type(ex).__name__}: {ex}",
            })
    return {"results": results}

class ConversationLeg(BaseModel):
    role: str
    call_id: str


@app.post("/conversations/{conversation_id}/legs")
async def link_conversation_leg(conversation_id: str, leg: ConversationLeg):
    _call_to_conversation[leg.call_id] = conversation_id
    return {"status": "ok"}


@app.post("/conversations/{conversation_id}/hold")
async def hold_conversation(conversation_id: str):
    await broadcast(conversation_id, {
        "type": "channel_hold",
        "hold": True,
        "conversation_id": conversation_id,
        "role": "system",
        "text": "[Call placed on hold]",
        "ts": time.time(),
    })
    return {"status": "ok", "action": "hold", "conversation_id": conversation_id}


@app.post("/conversations/{conversation_id}/unhold")
async def unhold_conversation(conversation_id: str):
    await broadcast(conversation_id, {
        "type": "channel_unhold",
        "hold": False,
        "conversation_id": conversation_id,
        "role": "system",
        "text": "[Call resumed from hold]",
        "ts": time.time(),
    })
    return {"status": "ok", "action": "unhold", "conversation_id": conversation_id}



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

STATIC_DIR = Path(__file__).resolve().parent / "static"
if (STATIC_DIR / "assets").exists():
    app.mount("/live/assets", StaticFiles(directory=STATIC_DIR / "assets"), name="static_assets")

@app.get("/live", response_class=HTMLResponse)
async def live_dashboard():
    index_file = STATIC_DIR / "index.html"
    return HTMLResponse(content=index_file.read_text(encoding="utf-8"))


@app.websocket("/live/ws")
async def live_events(ws: WebSocket):
    await ws.accept()
    q: asyncio.Queue = asyncio.Queue()
    _live_subscribers.append(q)
    try:
        await ws.send_json({
            "system": "connected",
            "call_id": "live",
            "ts": time.time(),
            "active_calls": _active_call_ids(),
        })
        for call_id in _active_call_ids():
            for event in _live_history.get(call_id, []):
                await ws.send_json(event)
        while True:
            await ws.send_json(await q.get())
    except WebSocketDisconnect:
        pass
    finally:
        if q in _live_subscribers:
            _live_subscribers.remove(q)


@app.websocket("/freeswitch/audio")
async def freeswitch_audio(ws: WebSocket):
    await ws.accept()
    metadata: dict = {}
    base_id = "unknown"
    client_session: CallSession | None = None
    operator_session: CallSession | None = None

    try:
        while True:
            message = await ws.receive()
            if message.get("type") == "websocket.disconnect":
                break

            if message.get("text") is not None:
                text = message["text"]
                try:
                    parsed = json.loads(text)
                    if isinstance(parsed, dict):
                        metadata.update(parsed)
                except json.JSONDecodeError:
                    log.info("FreeSWITCH stream text frame: %s", text)
                continue

            frame = message.get("bytes")
            if frame is None:
                continue

            if client_session is None or operator_session is None:
                base_id, client_session, operator_session = await _start_freeswitch_stereo_sessions(metadata)

            client_pcm, operator_pcm = _split_stereo_l16(frame)
            await client_session.feed(client_pcm, sample_rate=FREESWITCH_AUDIO_SAMPLE_RATE)
            await operator_session.feed(operator_pcm, sample_rate=FREESWITCH_AUDIO_SAMPLE_RATE)

    except WebSocketDisconnect:
        pass
    except Exception:
        log.exception("[%s] FreeSWITCH audio stream failed", base_id)
    finally:
        for session in (client_session, operator_session):
            if session is None:
                continue
            await broadcast(session.call_id, {
                "type": "call_ended",
                "conversation_id": base_id,
                "call_id": session.call_id,
                "ts": time.time(),
            })
            await session.close()
            _sessions.pop(session.call_id, None)
            _call_to_conversation.pop(session.call_id, None)
        log.info("[%s] FreeSWITCH stereo stream closed", base_id)


_controller_ws: WebSocket | None = None

@app.websocket("/controller/ws")
async def controller_control_socket(ws: WebSocket):
    global _controller_ws
    await ws.accept()
    _controller_ws = ws
    log.info("ARI controller command channel connected")
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        if _controller_ws is ws:
            _controller_ws = None

@app.post("/conversations/{conversation_id}/hold")
async def hold_conversation(conversation_id: str):
    if not _controller_ws:
        return Response(content='{"error": "ARI controller offline"}', status_code=503, media_type="application/json")
    await _controller_ws.send_json({"command": "hold", "conversation_id": conversation_id})
    return {"status": "ok"}

@app.post("/conversations/{conversation_id}/unhold")
async def unhold_conversation(conversation_id: str):
    if not _controller_ws:
        return Response(content='{"error": "ARI controller offline"}', status_code=503, media_type="application/json")
    await _controller_ws.send_json({"command": "unhold", "conversation_id": conversation_id})
    return {"status": "ok"}