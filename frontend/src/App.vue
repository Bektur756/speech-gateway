<script setup>
import { ref, onMounted, watch } from 'vue'
import AppHeader from './components/AppHeader.vue'
import CallsSidebar from './components/CallsSidebar.vue'
import ChatArea from './components/ChatArea.vue'
import PlaceholderPanel from './components/PlaceholderPanel.vue'
import { useWebSocket } from './composables/useWebSocket'
import { usePushStore } from './stores/pushStore'
import { useThemeStore } from './stores/themeStore'
import { useCallsStore } from './stores/callsStore'

// Initialize WebSocket connection & Pinia stores
useWebSocket()
const pushStore = usePushStore()
const themeStore = useThemeStore()
const callsStore = useCallsStore()

// Mobile view tab: 'calls' or 'chat'
const mobileTab = ref('calls')

function handleCallSelected() {
  // On mobile, automatically open the chat when a call is selected
  mobileTab.value = 'chat'
}

// If on mobile and a call is selected initially, switch to chat
watch(
  () => callsStore.selectedCallId,
  (newId) => {
    if (newId && mobileTab.value === 'calls' && window.innerWidth < 768) {
      // Don't forcefully switch unless user selects, but if no active view, can default to chat
    }
  }
)

onMounted(() => {
  themeStore.initTheme()
  pushStore.checkExistingSubscription()
  // Default to chat on mobile if there's already a selected call
  if (callsStore.selectedCallId && window.innerWidth < 768) {
    mobileTab.value = 'chat'
  }
})
</script>

<template>
  <div class="flex flex-col h-[100dvh] overflow-hidden bg-[#f6f7f9] dark:bg-[#101317] text-[#17202a] dark:text-[#eceff3] transition-colors duration-150">
    <!-- Top Bar -->
    <AppHeader />

    <!-- Mobile view switcher tabs (visible only on screens < md) -->
    <div class="md:hidden flex-none px-3 pt-2">
      <div class="flex rounded-xl bg-slate-200/80 dark:bg-slate-800/80 p-1 text-xs font-medium border border-slate-300/60 dark:border-slate-700">
        <button
          type="button"
          @click="mobileTab = 'calls'"
          :class="[
            'flex-1 py-1.5 rounded-lg transition-all flex items-center justify-center gap-1.5',
            mobileTab === 'calls'
              ? 'bg-white dark:bg-[#181c22] text-slate-900 dark:text-white shadow-sm font-semibold'
              : 'text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white'
          ]"
        >
          <span>Calls</span>
          <span
            v-if="callsStore.activeCallsCount > 0"
            class="px-1.5 py-0.2 text-[10px] rounded-full bg-[#0f766e]/15 text-[#0f766e] dark:bg-[#0f766e]/30 dark:text-teal-300 font-bold"
          >
            {{ callsStore.activeCallsCount }}
          </span>
        </button>

        <button
          type="button"
          @click="mobileTab = 'chat'"
          :class="[
            'flex-1 py-1.5 rounded-lg transition-all flex items-center justify-center gap-1.5',
            mobileTab === 'chat'
              ? 'bg-white dark:bg-[#181c22] text-slate-900 dark:text-white shadow-sm font-semibold'
              : 'text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white'
          ]"
        >
          <span>Transcript</span>
          <span
            v-if="callsStore.selectedCall"
            class="w-2 h-2 rounded-full bg-emerald-500"
          ></span>
        </button>
      </div>
    </div>

    <!-- Main Content Area -->
    <main class="flex-1 min-h-0 grid grid-cols-1 lg:grid-cols-2 gap-3 sm:gap-4 p-2 sm:p-4 max-w-[1920px] w-full mx-auto">
      <!-- Left Panel: Calls + Chat -->
      <section class="flex bg-white dark:bg-[#181c22] border border-[#d6dae1] dark:border-[#303844] rounded-xl sm:rounded-2xl overflow-hidden shadow-sm min-h-0 relative">
        <!-- Sidebar: Always visible on >= md; On mobile toggled by mobileTab -->
        <div
          :class="[
            'w-64 flex-none h-full',
            'max-md:w-full',
            mobileTab === 'calls' ? 'max-md:flex max-md:flex-col' : 'max-md:hidden'
          ]"
        >
          <CallsSidebar @select="handleCallSelected" />
        </div>

        <!-- Chat Area: Always visible on >= md; On mobile toggled by mobileTab -->
        <div
          :class="[
            'flex-1 flex flex-col min-h-0 min-w-0 h-full',
            mobileTab === 'chat' ? 'max-md:flex' : 'max-md:hidden'
          ]"
        >
          <ChatArea
            :show-back-button="true"
            @back="mobileTab = 'calls'"
          />
        </div>
      </section>

      <!-- Right Panel: Reserved / Analytics (desktop only) -->
      <section class="min-h-0 max-lg:hidden">
        <PlaceholderPanel />
      </section>
    </main>
  </div>
</template>
