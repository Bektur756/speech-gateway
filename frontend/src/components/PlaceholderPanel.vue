<script setup>
import { useCallsStore } from '../stores/callsStore'

const callsStore = useCallsStore()
</script>

<template>
  <section class="flex flex-col h-full bg-white dark:bg-[#181c22] border border-[#d6dae1] dark:border-[#303844] rounded-2xl overflow-hidden shadow-sm">
    <div class="flex items-center justify-between px-5 py-3.5 border-b border-[#d6dae1] dark:border-[#303844]">
      <span class="text-xs font-semibold uppercase tracking-wider text-slate-700 dark:text-slate-300">Онлайн заказ</span>
      <span class="text-xs text-[#667085] dark:text-[#9aa4b2]">Analytics</span>
    </div>

    <div class="flex-1 flex flex-col items-center justify-center p-6 text-center text-[#667085] dark:text-[#9aa4b2]">
      <div class="max-w-xs space-y-2">
        <div class="w-10 h-10 mx-auto rounded-xl bg-slate-100 dark:bg-slate-800 flex items-center justify-center text-slate-400">
          <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.7" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
          </svg>
        </div>
        <h3 class="text-sm font-medium text-slate-700 dark:text-slate-200">Call Insights & Telemetry</h3>
        <p class="text-xs text-[#667085] dark:text-[#9aa4b2]">
          Информация о заказе клиента и подсказки по заказу будут появляться здесь.
        </p>
      </div>
    </div>
  </section>
</template>
