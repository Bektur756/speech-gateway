<script setup>
import { computed } from 'vue'

const props = defineProps({
  event: {
    type: Object,
    required: true
  }
})

function formatTime(ts) {
  if (!ts) return new Date().toLocaleTimeString()
  return new Date(ts * 1000).toLocaleTimeString()
}

const role = computed(() => props.event.role || 'system')
const text = computed(() => props.event.text || props.event.partial || props.event.system || '')
const engine = computed(() => props.event.engine || 'system')
const isPartial = computed(() => Boolean(props.event.partial))

const bubbleClasses = computed(() => {
  if (role.value === 'client') {
    return 'self-start max-w-[78%]'
  } else if (role.value === 'operator') {
    return 'self-end max-w-[78%]'
  }
  return 'self-center max-w-[90%]'
})

const bubbleBoxClasses = computed(() => {
  if (role.value === 'client') {
    return 'bg-[#0f766e]/15 dark:bg-[#0f766e]/25 text-slate-900 dark:text-slate-100 rounded-2xl rounded-bl-[4px] border border-[#0f766e]/20'
  } else if (role.value === 'operator') {
    return 'bg-[#7c3aed]/15 dark:bg-[#7c3aed]/25 text-slate-900 dark:text-slate-100 rounded-2xl rounded-br-[4px] border border-[#7c3aed]/20'
  }
  return 'bg-transparent border border-dashed border-[#d6dae1] dark:border-[#303844] text-center rounded-xl'
})
</script>

<template>
  <article :class="['flex flex-col animate-bubble-in group', bubbleClasses]">
    <!-- Timestamp & Role header -->
    <div
      v-if="role !== 'system'"
      :class="[
        'flex items-center gap-2 mb-1 px-1 text-[11px]',
        role === 'operator' ? 'justify-end' : 'justify-start'
      ]"
    >
      <span
        :class="[
          'font-bold tracking-wider uppercase text-[10px]',
          role === 'client' ? 'text-teal-700 dark:text-teal-400' : 'text-purple-600 dark:text-purple-400'
        ]"
      >
        {{ role }}
      </span>
      <span class="text-[#667085] dark:text-[#9aa4b2] text-[10px] opacity-75">
        {{ formatTime(event.ts) }}
      </span>
    </div>

    <!-- Bubble body -->
    <div
      :class="[
        'p-3 transition-all',
        bubbleBoxClasses,
        isPartial ? 'animate-pulse-subtle italic text-slate-600 dark:text-slate-300' : ''
      ]"
    >
      <div class="text-[14px] leading-relaxed whitespace-pre-wrap break-words">
        {{ text }}
      </div>

      <!-- Engine tag -->
      <div
        v-if="role !== 'system'"
        :class="[
          'mt-1 text-[10px] font-mono tracking-tight text-[#667085] dark:text-[#9aa4b2] opacity-60',
          role === 'operator' ? 'text-right' : 'text-left'
        ]"
      >
        {{ engine }}
      </div>
    </div>
  </article>
</template>
