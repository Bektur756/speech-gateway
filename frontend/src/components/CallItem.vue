<script setup>
defineProps({
  call: {
    type: Object,
    required: true
  },
  isSelected: {
    type: Boolean,
    default: false
  },
  isOnHold: {
    type: Boolean,
    default: false
  }
})

defineEmits(['select'])
</script>

<template>
  <button
    type="button"
    @click="$emit('select', call.id)"
    :class="[
      'w-full text-left p-3 rounded-xl border transition-all duration-150 relative group',
      isSelected
        ? 'bg-[#0f766e]/10 border-[#0f766e]/30 dark:bg-[#0f766e]/20 dark:border-[#0f766e]/40'
        : 'bg-transparent border-transparent hover:bg-slate-50 dark:hover:bg-slate-800/60'
    ]"
  >
    <div class="flex items-center justify-between gap-2">
      <span class="font-mono text-xs font-medium truncate max-w-[180px] text-slate-800 dark:text-slate-200">
        {{ call.id }}
      </span>
      <span
        v-if="isOnHold"
        class="inline-block px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider rounded bg-amber-500/15 text-amber-600 dark:text-amber-400 border border-amber-500/30"
      >
        Hold
      </span>
    </div>
    <div class="flex items-center justify-between text-[11px] text-[#667085] dark:text-[#9aa4b2] mt-1.5">
      <span>{{ call.events.length }} events</span>
      <span class="flex items-center gap-1">
        <span class="inline-block w-1.5 h-1.5 rounded-full bg-emerald-500"></span>
        Active
      </span>
    </div>
  </button>
</template>
