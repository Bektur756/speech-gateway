# ТЗ: Speech Gateway для колл-центра (Vosk + AiRUN)

## Контекст

Колл-центр в Бишкеке (~12 операторов) использует Asterisk (Sangoma FreePBX 17,
Asterisk 22.10.1). Нужен отдельный сервис — Speech Gateway — который получает
аудио звонка через AudioSocket, распознаёт речь клиента в реальном времени
через один из двух движков STT (в зависимости от языка) и отдаёт финальный
текст наружу по WebSocket.

Ниже — уже написанный и синтаксически проверенный код (см. раздел
"Стартовый код"), плюс список того, что осталось сделать, плюс важные
технические детали, добытые сегодня отладкой на живом звонке — их не нужно
переоткрывать заново.

## Целевая архитектура

```
Asterisk (AudioSocket)
  ├─ порт 9098 → CallSession(язык=ru) → VoskAdapter (vosk-ru, Kaldi, self-hosted)
  └─ порт 9099 → CallSession(язык=ky) → AiRUNAdapter (cloud)
                                          └─ fallback → VoskAdapter (vosk-ky)

CallSession → persist JSONL → broadcast → WS /calls/{call_id}/stream (клиенты подписываются)
```

**Почему два порта, а не один с параметром языка**: AudioSocket передаёт
только UUID звонка (валидный по формату UUID), места под тег языка в
протоколе нет. Поэтому язык звонка кодируется выбором порта — правится
на уровне дialplan (какая очередь/маршрут — тот и порт), не в самом
протоколе.

## Целевая среда

- Ubuntu 24.04 LTS, Python 3.12
- Docker Engine + Compose plugin (официальный репозиторий, не `docker.io` из apt)
- Проект живёт в `/opt/speech-gateway/`
- Уже поднято на сервере: см. `docker-compose.yml` ниже — сервисы `vosk-ru`,
  `vosk-ky`, `gateway`

## Важные технические детали (не переоткрывать через отладку — уже проверено вживую)

1. **Авторизация AiRUN — только через заголовок**, не через query-параметр.
   `?api_key=...` в URL нестабильно возвращал HTTP 403 при подключении из
   Python (`websockets`), заголовок `Authorization: Bearer <key>` работает
   стабильно на длинных сессиях (проверено на звонке 1642 аудио-кадра без
   разрывов). Endpoint для этого сервиса: `wss://api.airun.kg/v1/airun-asr-realtime/stream?language={ky|ru}`.
   Отдельный ключ от батчевого `airun-asr` — старый ключ вернёт 404.

2. **AiRUN WS close-коды, требующие фолбэка**: `4402` (баланс исчерпан),
   `4403` (ключ заблокирован), `4408` (превышена макс. длительность сессии,
   1 час). При любом из них — переключать звонок на `vosk-ky`, не обрывать
   транскрипцию молча.

3. **Лимит AiRUN: 2 параллельных стрима на ключ.** Это означает, что один
   кыргызский звонок с обоими каналами (клиент+оператор) съедает весь лимит
   ключа. Если в будущем понадобится вести оба канала одновременно — нужны
   отдельные ключи от BDigital на каждый одновременный звонок.

4. **AudioSocket отдаёт `slin` — 16-bit signed LE PCM, mono, 8kHz.** AiRUN и
   Vosk оба ждут 16kHz — ресемплинг через `audioop.ratecv(payload, 2, 1, 8000, 16000, state)`.
   `audioop` есть в stdlib до Python 3.12 включительно, в 3.13 убран (не
   актуально для этого сервера, но если версия рантайма когда-то поменяется —
   учитывать `audioop-lts` из pip).

5. **Vosk без готового кыргызского Docker-образа.** У alphacep нет
   `kaldi-ky`. Решение: взять любой существующий образ (используется
   `alphacep/kaldi-en:latest`) и подменить путь модели volume-мountом на
   `/opt/vosk-model-en/model` — сервер внутри не привязан к языку, грузит
   что лежит по этому пути. Модель `vosk-model-ky-0.42` скачивается отдельно
   с `https://alphacephei.com/vosk/models/vosk-model-ky-0.42.zip`.

6. **AudioSocket dialplan-синтаксис**: `AudioSocket(uuid,host:port)`,
   приложение не отвечает на звонок само — перед ним обязателен `Answer()`.
   Модули на этом Asterisk уже подтверждены рабочими:
   `app_audiosocket`, `chan_audiosocket`, `res_audiosocket` (с версии 18.0.0).

7. **FreePBX не хранит кастомный дialplan в `extensions.conf`** — GUI
   перезаписывает этот файл при каждом Apply Config. Кастомные контексты —
   только в `/etc/asterisk/extensions_custom.conf`, который FreePBX не
   трогает. Чтобы дозвониться из внутреннего номера до кастомного
   контекста — нужны **Custom Destination** (Admin → Custom Destinations,
   Target: `context,exten,priority`) + **Misc Application** (Applications →
   Misc Applications, feature code → указывает на Custom Destination).

8. **Python на Debian/Ubuntu — PEP 668 «externally managed».** Голый
   `pip install` в системный Python откажет. Использовать venv
   (`python3 -m venv`) или `--break-system-packages` для разовых тестов.
   В Docker-контейнере (см. `Dockerfile` ниже) это не актуально — там
   `pip install` в изолированном образе работает штатно.

## Стартовый код (уже написан, синтаксически проверен `py_compile`)

Структура:
```
/opt/speech-gateway/
  app/
    __init__.py       (пустой)
    audiosocket.py
    adapters.py
    session.py
    main.py
  requirements.txt
  Dockerfile
  docker-compose.yml
```

### app/audiosocket.py
```python
"""AudioSocket protocol frame parsing (Asterisk res_audiosocket)."""
import asyncio
import struct

TYPE_TERMINATE = 0x00
TYPE_UUID = 0x01
TYPE_AUDIO = 0x10
TYPE_ERROR = 0xFF


async def read_frame(reader: asyncio.StreamReader):
    """Read one AudioSocket frame: 1 byte type + 2 byte big-endian length + payload."""
    header = await reader.readexactly(3)
    kind = header[0]
    length = struct.unpack(">H", header[1:3])[0]
    payload = await reader.readexactly(length) if length else b""
    return kind, payload
```

### app/adapters.py
```python
"""STT engine adapters — Vosk (self-hosted) and AiRUN (cloud).

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
"""
import abc
import asyncio
import json
import logging

import websockets

log = logging.getLogger("adapters")

AIRUN_FATAL_CODES = {4402, 4403, 4408}


class STTAdapter(abc.ABC):
    name: str = "base"

    @abc.abstractmethod
    async def stream(self, pcm_queue: asyncio.Queue, event_queue: asyncio.Queue) -> None:
        ...


class VoskAdapter(STTAdapter):
    name = "vosk"

    def __init__(self, ws_url: str):
        self.ws_url = ws_url

    async def stream(self, pcm_queue, event_queue):
        try:
            async with websockets.connect(self.ws_url, open_timeout=5) as ws:
                await ws.send(json.dumps({"config": {"sample_rate": 16000}}))
                await event_queue.put({"type": "ready"})

                async def sender():
                    while True:
                        chunk = await pcm_queue.get()
                        if chunk is None:
                            await ws.send(json.dumps({"eof": 1}))
                            return
                        await ws.send(chunk)

                async def receiver():
                    async for raw in ws:
                        msg = json.loads(raw)
                        if "partial" in msg and msg["partial"]:
                            await event_queue.put({"type": "partial", "text": msg["partial"]})
                        elif "text" in msg and msg["text"]:
                            await event_queue.put({"type": "final", "text": msg["text"]})

                await asyncio.gather(sender(), receiver())
                await event_queue.put({"type": "done"})
        except Exception as e:
            log.warning("vosk stream failed (%s): %s", self.ws_url, e)
            await event_queue.put({"type": "fatal", "reason": str(e)})


class AiRUNAdapter(STTAdapter):
    name = "airun"
    URL_TMPL = "wss://api.airun.kg/v1/airun-asr-realtime/stream?language={lang}"

    def __init__(self, api_key: str, lang: str):
        self.api_key = api_key
        self.lang = lang

    async def stream(self, pcm_queue, event_queue):
        url = self.URL_TMPL.format(lang=self.lang)
        headers = {"Authorization": f"Bearer {self.api_key}"}  # NOT query param — see spec notes
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


def build_adapter(name: str, lang: str, *, vosk_ru_url: str, vosk_ky_url: str, airun_key: str) -> STTAdapter:
    if name == "vosk":
        url = vosk_ru_url if lang == "ru" else vosk_ky_url
        return VoskAdapter(url)
    if name == "airun":
        return AiRUNAdapter(airun_key, lang)
    raise ValueError(f"unknown engine: {name}")
```

### app/session.py
```python
"""CallSession — owns one call end-to-end: audio in, routed STT, phrase
buffering, persistence, and live fan-out to subscribed WebSocket clients.

MVP-level phrase assembly: forwards only "final" events downstream, tagged
with the producing engine, persisted to a per-call JSONL file. Dedup /
word-order-repair beyond what the engine itself provides is NOT implemented —
see "Оставшиеся задачи" below.
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

ROUTING = {
    "ru": [("vosk", "ru")],
    "ky": [("airun", "ky"), ("vosk", "ky")],
}


class CallSession:
    def __init__(self, call_id: str, language: str, adapter_config: dict, broadcaster):
        self.call_id = call_id
        self.language = language
        self.chain = ROUTING.get(language, ROUTING["ru"])
        self.adapter_config = adapter_config
        self.broadcaster = broadcaster

        self._resample_state = None
        self._active_pcm_queue: asyncio.Queue | None = None
        self._event_queue: asyncio.Queue = asyncio.Queue()
        self._engine_task: asyncio.Task | None = None
        self._dispatcher_task: asyncio.Task | None = None
        self._chain_idx = 0
        self._audio_frames = 0
        self._transcript_path = TRANSCRIPT_DIR / f"{call_id}.jsonl"

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
        self._log(f"engine started: {name} ({lang})")
        self._engine_task = asyncio.create_task(
            adapter.stream(self._active_pcm_queue, self._event_queue)
        )

    async def feed(self, raw_8k: bytes):
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
                record = {"call_id": self.call_id, "engine": name, "text": ev["text"], "ts": time.time()}
                self._persist(record)
                await self.broadcaster(self.call_id, record)
            elif t == "partial":
                await self.broadcaster(self.call_id, {
                    "call_id": self.call_id, "engine": name, "partial": ev["text"], "ts": time.time(),
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
        log.info(f"[{self.call_id}] {msg}")
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
```

### app/main.py
```python
"""Speech Gateway — entrypoint.

Two AudioSocket TCP listeners (one per language):
    port 9098  ->  routed to Vosk (ru)
    port 9099  ->  routed to AiRUN with Vosk-ky fallback

Downstream consumers subscribe to a call's live transcript via:
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

ADAPTER_CONFIG = dict(vosk_ru_url=VOSK_RU_URL, vosk_ky_url=VOSK_KY_URL, airun_key=AIRUN_API_KEY)

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
    server_ru = await asyncio.start_server(lambda r, w: handle_call(r, w, "ru"), AUDIOSOCKET_HOST, PORT_RU)
    server_ky = await asyncio.start_server(lambda r, w: handle_call(r, w, "ky"), AUDIOSOCKET_HOST, PORT_KY)
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
```

### requirements.txt
```
fastapi==0.115.0
uvicorn[standard]==0.30.6
websockets==13.1
```

### Dockerfile
```dockerfile
FROM python:3.12-slim

WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

EXPOSE 9098 9099 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

### docker-compose.yml

```yaml
services:
   vosk-ru:
      image: alphacep/kaldi-ru:latest
      restart: unless-stopped

   vosk-ky:
      image: alphacep/kaldi-en:latest
      restart: unless-stopped
      volumes:
         - ./models/vosk-model-ky-0.42:/opt/vosk-model-en/model

   gateway:
      build: app
      restart: unless-stopped
      depends_on:
         - vosk-ru
         - vosk-ky
      environment:
         AIRUN_API_KEY: ${AIRUN_API_KEY}
         VOSK_RU_URL: ws://vosk-ru:2700
         VOSK_KY_URL: ws://vosk-ky:2700
         AUDIOSOCKET_PORT_RU: "9098"
         AUDIOSOCKET_PORT_KY: "9099"
      ports:
         - "9098:9098"
         - "9099:9099"
         - "8000:8000"
      volumes:
         - ./data/transcripts:/data/transcripts
```

## Оставшиеся задачи

1. **Проверить фолбэк вживую.** Логика переключения на резервный движок при
   `fatal`-событии написана, но не протестирована на реальном обрыве AiRUN
   (например, искусственно передав неверный ключ на середине звонка) —
   убедиться, что переключение реально происходит и что в JSONL-транскрипте
   видно системную запись о переключении.

2. **Улучшить сборку реплик (phrase assembly).** Сейчас в поток идёт «одна
   финальная фраза как есть от движка» без дедупликации и без восстановления
   порядка слов. По ТЗ проекта требуется: убирать повторы между
   последовательными partial/final одного и того же сегмента речи,
   гарантировать, что в оркестратор/downstream уходит только чистая финальная
   фраза.

3. **Добавить Whisper как третий адаптер** (опционально, для сравнения
   качества на смешанной кыргызско-русской речи). Модель:
   `nineninesix/kyrgyz-whisper-small` или `-medium` с Hugging Face —
   мультиязычная (ky/ru/en), обучена под этот конкретный случай. В отличие
   от Vosk/AiRUN, Whisper не потоковый нативно (работает окнами, обычно
   30 сек) — адаптеру нужна собственная логика нарезки аудио на
   перекрывающиеся окна, чтобы получить псевдо-realtime partial-эффект.
   Проверить реальный RTF на CPU этого сервера — если инференс не
   укладывается в приемлемую задержку, может понадобиться GPU (сейчас его
   на сервере нет).

4. **nginx reverse proxy перед портом 8000** (WS API `/calls/{id}/stream`)
   — для внешних потребителей текста. AudioSocket-порты (9098/9099) через
   nginx НЕ проксируются — это сырой TCP, Asterisk подключается к ним
   напрямую.

5. **Дialplan на Asterisk** — два маршрута вместо одного, указывающие на
   разные порты (9098 для ru-очереди, 9099 для ky-очереди). Через
   `/etc/asterisk/extensions_custom.conf` + Custom Destination + Misc
   Application в FreePBX GUI (см. пункт 7 в разделе "Важные детали").

6. **Нагрузочное тестирование.** Целевая нагрузка — до 12 одновременных
   звонков (по числу операторов, точных данных о трафике от заказчика нет).
   Замерить реальный RTF декодирования Vosk на этом железе, скорректировать
   финальный сайзинг сервера (сейчас на тестовых мощностях: 4 CPU / 8GB RAM
   / 80GB диск).

7. **Мониторинг баланса AiRUN** — алерт до исчерпания секунд, не постфактум
   через WS 4402.

## Приёмочные критерии

- Звонок через дialplan-маршрут `ru` → в JSONL и по WS видны final-фразы от
  Vosk на русском.
- Звонок через маршрут `ky` → final-фразы от AiRUN на кыргызском.
- Искусственный обрыв AiRUN посреди `ky`-звонка → звонок не прерывается,
  транскрипция продолжается через `vosk-ky`, в логе виден системный
  маркер фолбэка.
- `GET /health` отвечает `{"status":"ok",...}`.
- Подключение к `ws://.../calls/{call_id}/stream` во время активного звонка
  показывает текст практически в реальном времени (задержка — секунды, не
  минуты).
- Отказ STT (любой движок недоступен) не приводит к падению самого звонка
  на Asterisk — звонок и запись идут штатно независимо от состояния
  гейтвея.
