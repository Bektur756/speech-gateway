<script setup>
import { computed } from 'vue'
import { useCallsStore } from '../stores/callsStore'
import CallItem from './CallItem.vue'

const emit = defineEmits(['select'])
const callsStore = useCallsStore()

const summaryText = computed(() => {
  const count = callsStore.activeCallsCount
  if (count === 0) return 'Нет активных звонков'
  return `${count} active call${count === 1 ? '' : 's'}`
})

function handleSelectCall(callId) {
  callsStore.selectCall(callId)
  emit('select', callId)
}
</script>

<template>
  <aside class="flex flex-col h-full min-h-0 md:border-r border-[#d6dae1] dark:border-[#303844]">
    <div class="flex-none flex items-baseline justify-between px-4 py-3 border-b border-[#d6dae1] dark:border-[#303844]">
      <span class="text-xs font-semibold uppercase tracking-wider text-slate-700 dark:text-slate-300">Активные звонки</span>
      <span class="text-xs text-[#667085] dark:text-[#9aa4b2]">{{ summaryText }}</span>
    </div>

    <div class="flex-1 overflow-y-auto theme-scrollbar p-2 space-y-1 overscroll-contain">
      <template v-if="callsStore.activeCalls.length > 0">
        <CallItem
          v-for="call in callsStore.activeCalls"
          :key="call.id"
          :call="call"
          :is-selected="call.id === callsStore.selectedCallId"
          :is-on-hold="callsStore.holdingCalls.has(call.id)"
          @select="handleSelectCall"
        />
      </template>

      <div
        v-else
        class="h-full flex flex-col items-center justify-center p-6 text-center text-[#667085] dark:text-[#9aa4b2] text-xs"
      >
        <p>Ожидание звонка...</p>
      </div>
    </div>
  </aside>
</template>
