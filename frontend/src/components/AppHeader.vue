<script setup>
import { useCallsStore } from '../stores/callsStore'
import { usePushStore } from '../stores/pushStore'
import StatusBadge from './StatusBadge.vue'
import ThemeToggle from './ThemeToggle.vue'

const callsStore = useCallsStore()
const pushStore = usePushStore()

function handleClear() {
  callsStore.clearTranscript()
}
</script>

<template>
  <header class="flex-none flex items-center justify-between gap-3 px-3.5 sm:px-5 py-3 border-b border-[#d6dae1] dark:border-[#303844] bg-white dark:bg-[#181c22] transition-colors">
    <div class="flex items-center gap-2">
      <h1 class="text-base sm:text-lg font-semibold tracking-tight whitespace-nowrap">
        Live Transcription
      </h1>
    </div>

    <div class="flex items-center gap-1.5 sm:gap-2.5">
      <!-- Push notification button -->
      <button
        type="button"
        :disabled="pushStore.isSubscribed || pushStore.isLoading || !pushStore.isSupported"
        @click="pushStore.enableNotifications()"
        :title="pushStore.statusText"
        class="inline-flex items-center justify-center px-2.5 sm:px-3 py-1.5 text-xs font-medium rounded-lg border border-[#d6dae1] dark:border-[#303844] bg-white dark:bg-[#181c22] text-[#17202a] dark:text-[#eceff3] hover:bg-slate-50 dark:hover:bg-slate-800 disabled:opacity-60 disabled:cursor-not-allowed transition-colors"
      >
        <svg class="w-3.5 h-3.5 sm:mr-1.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 17h5l-1.405-1.405A2.032 2.032 0 0118 14.158V11a6.002 6.002 0 00-4-5.659V5a2 2 0 10-4 0v.341C7.67 6.165 6 8.388 6 11v3.159c0 .538-.214 1.055-.595 1.436L4 17h5m6 0v1a3 3 0 11-6 0v-1m6 0H9" />
        </svg>
        <span class="hidden sm:inline">
          {{ pushStore.isLoading ? 'Загрузка...' : pushStore.statusText }}
        </span>
      </button>

      <!-- Clear transcript button -->
      <button
        type="button"
        :disabled="!callsStore.selectedCall"
        @click="handleClear"
        title="Clear transcript"
        class="inline-flex items-center justify-center px-2.5 sm:px-3 py-1.5 text-xs font-medium rounded-lg border border-[#d6dae1] dark:border-[#303844] bg-white dark:bg-[#181c22] text-[#17202a] dark:text-[#eceff3] hover:bg-slate-50 dark:hover:bg-slate-800 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
      >
        <svg class="w-3.5 h-3.5 sm:mr-1.5 text-slate-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
        </svg>
        <span class="hidden sm:inline">Очистить</span>
      </button>

      <!-- Dark / White theme toggler -->
      <ThemeToggle />

      <!-- Status badge -->
      <StatusBadge :status="callsStore.connectionStatus" />
    </div>
  </header>
</template>
