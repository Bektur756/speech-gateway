import { onMounted, onUnmounted } from 'vue'
import { useCallsStore } from '../stores/callsStore'

export function useWebSocket() {
  const callsStore = useCallsStore()
  let socket = null
  let reconnectTimer = null
  let isIntentionallyClosed = false

  function connect() {
    if (isIntentionallyClosed) return

    const scheme = window.location.protocol === 'https:' ? 'wss' : 'ws'
    const wsUrl = `${scheme}://${window.location.host}/live/ws`

    callsStore.setConnectionStatus('connecting')

    try {
      socket = new WebSocket(wsUrl)

      socket.onopen = () => {
        callsStore.setConnectionStatus('connected')
      }

      socket.onmessage = (message) => {
        try {
          const event = JSON.parse(message.data)
          callsStore.handleEvent(event)
        } catch (err) {
          console.error('Failed to parse WS message:', err)
        }
      }

      socket.onclose = () => {
        if (isIntentionallyClosed) return
        callsStore.setConnectionStatus('reconnecting')
        clearTimeout(reconnectTimer)
        reconnectTimer = setTimeout(connect, 1500)
      }

      socket.onerror = (err) => {
        console.warn('WebSocket error, closing to reconnect:', err)
        socket?.close()
      }
    } catch (e) {
      callsStore.setConnectionStatus('reconnecting')
      reconnectTimer = setTimeout(connect, 1500)
    }
  }

  function disconnect() {
    isIntentionallyClosed = true
    clearTimeout(reconnectTimer)
    if (socket) {
      socket.close()
      socket = null
    }
    callsStore.setConnectionStatus('disconnected')
  }

  onMounted(() => {
    isIntentionallyClosed = false
    connect()
  })

  onUnmounted(() => {
    disconnect()
  })

  return {
    connect,
    disconnect,
  }
}
