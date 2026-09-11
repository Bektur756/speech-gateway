# Speech Gateway Frontend (Vue 3 + Pinia + Tailwind CSS)

Modern real-time transcription dashboard replacing the inline `LIVE_HTML`.

## Tech Stack
- **Framework**: Vue 3 (Composition API, `<script setup>`)
- **State Management**: Pinia
- **Styling**: Tailwind CSS v3 with custom theme colors & animations
- **Build Tool**: Vite

## Project Structure
```
frontend/
├── index.html
├── package.json
├── postcss.config.js
├── tailwind.config.js
├── vite.config.js
└── src/
    ├── main.js                     # Application entrypoint & Pinia initialization
    ├── App.vue                     # Layout container (Two-island responsive layout)
    ├── assets/
    │   └── main.css                # Tailwind directives & global styling
    ├── stores/
    │   ├── callsStore.js           # Active calls, event buffer (500 max), selection & hold status
    │   └── pushStore.js            # W3C Web Push registration & VAPID subscription state
    ├── composables/
    │   └── useWebSocket.js         # Realtime WebSocket (/live/ws) lifecycle & auto-reconnection
    └── components/
        ├── AppHeader.vue           # Header with title, push notifications, clear button
        ├── StatusBadge.vue         # Live indicator badge (connecting, connected, reconnecting)
        ├── CallsSidebar.vue        # Sidebar list of active calls with counter
        ├── CallItem.vue            # Individual call card with status & hold pill
        ├── ChatArea.vue            # Transcript view, auto-scrolling & call hold/resume control
        ├── ChatBubble.vue          # Chat bubble with role coloring, partial animation, engine tag
        └── PlaceholderPanel.vue    # Right island panel for analytics / call telemetry
```

## Development
Run the development server with Hot Module Replacement (HMR):
```bash
npm install
npm run dev
```

## Production Build
Build optimized production bundle to `../app/static/`:
```bash
npm run build
```
FastAPI automatically serves `app/static/index.html` and assets under `/live/`.

## Docker Usage

### 1. Production (Multi-stage build)
The root `Dockerfile` automatically builds the Vue frontend in a lightweight `node:20-alpine` builder stage and copies the compiled assets into the Python gateway container:
```bash
docker compose build gateway
docker compose up -d gateway
```

### 2. Live Development with Docker
To run Vite dev server with hot reload inside Docker:
```bash
docker compose --profile dev up frontend-dev
```
The dev server will be available at `http://localhost:5173`.

