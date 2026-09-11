<script setup>
import { computed } from 'vue'

const props = defineProps({
  status: {
    type: String,
    required: true,
    default: 'connecting'
  }
})

const badgeConfig = computed(() => {
  switch (props.status) {
    case 'connected':
      return {
        dotClass: 'bg-emerald-500',
        textClass: 'text-emerald-600 dark:text-emerald-400',
        label: 'connected'
      }
    case 'reconnecting':
      return {
        dotClass: 'bg-amber-500 animate-ping',
        textClass: 'text-amber-600 dark:text-amber-400',
        label: 'reconnecting'
      }
    case 'connecting':
    default:
      return {
        dotClass: 'bg-sky-500 animate-pulse',
        textClass: 'text-sky-600 dark:text-sky-400',
        label: 'connecting'
      }
  }
})
</script>

<template>
  <div class="inline-flex items-center gap-1.5 text-xs font-medium px-2.5 py-1 rounded-full bg-slate-100 dark:bg-slate-800/80 border border-slate-200 dark:border-slate-700">
    <span class="relative flex h-2 w-2">
      <span :class="[badgeConfig.dotClass, 'relative inline-flex rounded-full h-2 w-2']"></span>
    </span>
    <span :class="badgeConfig.textClass">{{ badgeConfig.label }}</span>
  </div>
</template>
