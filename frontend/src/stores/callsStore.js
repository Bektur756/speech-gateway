import { defineStore } from 'pinia'
import { ref, computed } from 'vue'

export const useCallsStore = defineStore('calls', () => {
  const calls = ref(new Map())
  const selectedCallId = ref(null)
  const connectionStatus = ref('connecting') // 'connecting' | 'connected' | 'reconnecting'
  const holdingCalls = ref(new Set())

  // Getters
  const activeCalls = computed(() => {
    return Array.from(calls.value.values()).filter(c => c.active)
  })

  const activeCallsCount = computed(() => activeCalls.value.length)

  const selectedCall = computed(() => {
    if (!selectedCallId.value) return null
    return calls.value.get(selectedCallIdIdOrFallback()) || null
  })

  function selectedCallIdIdOrFallback() {
    return selectedCallId.value
  }

  const selectedCallEvents = computed(() => {
    const call = calls.value.get(selectedCallId.value)
    if (!call) return []
    // Filter out internal control events that shouldn't display as speech bubbles
    return call.events.filter(e => e.type !== 'call_started' && e.type !== 'call_ended')
  })

  const isCurrentCallOnHold = computed(() => {
    return selectedCallId.value ? holdingCalls.value.has(selectedCallId.value) : false
  })

  // Actions
  function setConnectionStatus(status) {
    connectionStatus.value = status
  }

  function callKey(event) {
    return event.conversation_id || event.call_id || 'unknown'
  }

  function handleEvent(event) {
    if (event.system === 'connected') {
      const activeIds = event.active_calls || []
      for (const id of activeIds) {
        if (!calls.value.has(id)) {
          calls.value.set(id, { id, events: [], active: true })
        } else {
          calls.value.get(id).active = true
        }
      }
      if (!selectedCallId.value && activeIds.length > 0) {
        selectedCallId.value = activeIds[0]
      }
      return
    }

    const id = callKey(event)
    let call = calls.value.get(id)
    if (!call) {
      call = { id, events: [], active: true }
      calls.value.set(id, call)
    }

    if (event.type === 'call_ended') {
      call.active = false
      holdingCalls.value.delete(id)
    } else {
      call.active = true
    }

    // Check for hold/unhold notifications
    if (event.type === 'channel_hold' || event.hold === true) {
      holdingCalls.value.add(id)
    } else if (event.type === 'channel_unhold' || event.hold === false) {
      holdingCalls.value.delete(id)
    }

    call.events.push(event)
    if (call.events.length > 500) {
      call.events.shift()
    }

    // Auto-select if first call
    if (!selectedCallId.value) {
      selectedCallId.value = id
    }
  }

  function selectCall(id) {
    selectedCallId.value = id
  }

  function clearTranscript(id = null) {
    const targetId = id || selectedCallId.value
    if (!targetId) return
    const call = calls.value.get(targetId)
    if (call) {
      call.events = []
    }
  }

  async function toggleHold(id = null) {
    const targetId = id || selectedCallId.value
    if (!targetId) return
    const isHeld = holdingCalls.value.has(targetId)
    const action = isHeld ? 'unhold' : 'hold'
    try {
      const res = await fetch(`/conversations/${targetId}/${action}`, { method: 'POST' })
      if (res.ok) {
        if (isHeld) {
          holdingCalls.value.delete(targetId)
        } else {
          holdingCalls.value.add(targetId)
        }
      }
      return res.ok
    } catch (err) {
      console.error(`Failed to ${action} call:`, err)
      return false
    }
  }

  return {
    calls,
    selectedCallId,
    connectionStatus,
    holdingCalls,
    activeCalls,
    activeCallsCount,
    selectedCall,
    selectedCallEvents,
    isCurrentCallOnHold,
    setConnectionStatus,
    handleEvent,
    selectCall,
    clearTranscript,
    toggleHold,
  }
})
