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

  // --- Bubble merging logic ---
  // Thresholds for deciding when to start a new bubble
  const WORD_THRESHOLD = 10          // min words before allowing a new bubble
  const SILENCE_GAP_THRESHOLD = 2.0 // seconds of silence between segments to force a new bubble

  /**
   * Returns the time gap (in seconds) between two consecutive speech events.
   * Uses audio_start_timestamp of the new event minus speech_end_timestamp
   * (or audio_end_timestamp) of the previous event.
   * Returns Infinity if timestamps are missing so a new bubble is started.
   */
  function getGapBetween(prevEvent, newEvent) {
    const prevEnd = prevEvent.speech_end_timestamp
      || prevEvent.audio_end_timestamp
      || prevEvent.ts
    const newStart = newEvent.audio_start_timestamp || newEvent.ts
    if (prevEnd == null || newStart == null) return Infinity
    return newStart - prevEnd
  }

  function wordCount(text) {
    if (!text) return 0
    return text.trim().split(/\s+/).filter(Boolean).length
  }

  /**
   * Merged bubbles computed property.
   * Groups consecutive same-role events into combined bubbles.
   * Each merged bubble has:
   *   - role, engine (from first event)
   *   - text: concatenated texts
   *   - ts: timestamp of the first event in the group
   *   - tsEnd: timestamp of the last event in the group
   *   - events: array of original events in this group
   *   - isPartial: true if the last event in the group is partial
   */
  const selectedCallMergedBubbles = computed(() => {
    const events = selectedCallEvents.value
    if (!events.length) return []

    const bubbles = []
    let current = null

    for (const event of events) {
      const role = event.role || 'system'
      const text = event.text || event.partial || event.system || ''
      const isPartial = Boolean(event.partial)
      const isSpeechEvent = role === 'client' || role === 'operator'

      // Non-speech events (system, hold, etc.) always get their own bubble
      if (!isSpeechEvent) {
        if (current) {
          bubbles.push(current)
          current = null
        }
        bubbles.push({
          role,
          engine: event.engine || 'system',
          text,
          ts: event.ts,
          tsEnd: event.ts,
          events: [event],
          isPartial,
        })
        continue
      }

      // Decide whether to merge into current bubble or start a new one
      let startNew = true

      if (current && current.role === role) {
        const currentWords = wordCount(current.text)
        const gap = getGapBetween(current.events[current.events.length - 1], event)

        // Merge if word count is still below threshold OR silence gap is small
        if (currentWords < WORD_THRESHOLD || gap < SILENCE_GAP_THRESHOLD) {
          startNew = false
        }
      }

      if (startNew) {
        if (current) {
          bubbles.push(current)
        }
        current = {
          role,
          engine: event.engine || 'system',
          text,
          ts: event.ts,
          tsEnd: event.ts,
          events: [event],
          isPartial,
        }
      } else {
        // Append to current bubble
        current.text = current.text + ' ' + text
        current.tsEnd = event.ts
        current.events.push(event)
        current.isPartial = isPartial
      }
    }

    if (current) {
      bubbles.push(current)
    }

    return bubbles
  })

  const isCurrentCallOnHold = computed(() => {
    return selectedCallId.value ? holdingCalls.value.has(selectedCallId.value) : false
  })

  const isCurrentCallActive = computed(() => {
    return calls.value.get(selectedCallId.value).active || false
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
    selectedCallMergedBubbles,
    isCurrentCallOnHold,
    isCurrentCallActive,
    setConnectionStatus,
    handleEvent,
    selectCall,
    clearTranscript,
    toggleHold,
  }
})
