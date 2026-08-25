# speech-gateway

Speech Gateway for a Bishkek call center running Asterisk (FreePBX 17,
Asterisk 22). Receives call audio over AudioSocket, transcribes it in real
time — Vosk (self-hosted) for Russian, AiRUN (cloud) with a Vosk fallback
for Kyrgyz — and streams the transcript out over WebSocket.

Full spec, architecture rationale, and field-tested implementation notes
live in [`speech-gateway-TZ.md`](speech-gateway-TZ.md).

## Quick start

1. Copy `.env.example` to `.env` and set `AIRUN_API_KEY` (the realtime
   streaming key, not the batch `airun-asr` key).
2. Download models into `models/` (not tracked in git — see below).
3. `docker compose up -d`
4. `curl localhost/health`

## Models

Not tracked in git — fetch these into `models/` before starting the stack:

- **`vosk-model-ky-0.42`** (Vosk Kyrgyz, required):
  ```
  curl -fLo /tmp/vosk-ky.zip https://alphacephei.com/vosk/models/vosk-model-ky-0.42.zip
  unzip /tmp/vosk-ky.zip -d models/
  ```
- **`kyrgyz-whisper-small`** / **`kyrgyz-whisper-medium`** (comparison
  adapter, not in the production ru/ky fallback chain):
  ```python
  from huggingface_hub import snapshot_download
  snapshot_download("nineninesix/kyrgyz-whisper-small", local_dir="models/kyrgyz-whisper-small")
  snapshot_download("nineninesix/kyrgyz-whisper-medium", local_dir="models/kyrgyz-whisper-medium")
  ```

Russian Vosk (`vosk-ru`) needs no manual download — the model ships inside
the `alphacep/kaldi-ru` image.

## Architecture

```
Asterisk (AudioSocket)
  ├─ port 9098 → CallSession(ru) → VoskAdapter (vosk-ru, self-hosted)
  └─ port 9099 → CallSession(ky) → AiRUNAdapter (cloud)
                                     └─ fallback → VoskAdapter (vosk-ky)

CallSession → persist JSONL → broadcast → WS /calls/{call_id}/stream
```

nginx fronts the WS API (`/calls/{id}/stream`, `/health`). AudioSocket
ports 9098/9099 are raw TCP and are not proxied — Asterisk connects to them
directly.
