<script setup>
import { ref, watch, nextTick, onMounted } from 'vue'
import { useCallsStore } from '../stores/callsStore'
import ChatBubble from './ChatBubble.vue'

defineProps({
  showBackButton: {
    type: Boolean,
    default: false
  }
})

defineEmits(['back'])

const callsStore = useCallsStore()
const eventsContainer = ref(null)
const isHoldingLoading = ref(false)
const isScrolledUp = ref(false)

function checkScrollPosition() {
  if (!eventsContainer.value) return
  const { scrollTop, scrollHeight, clientHeight } = eventsContainer.value
  isScrolledUp.value = scrollHeight - scrollTop - clientHeight > 80
}

function scrollToBottom(smooth = false) {
  nextTick(() => {
    if (!eventsContainer.value) return
    if (smooth) {
      eventsContainer.value.scrollTo({
        top: eventsContainer.value.scrollHeight,
        behavior: 'smooth'
      })
    } else {
      eventsContainer.value.scrollTop = eventsContainer.value.scrollHeight
    }
  })
}

// Auto-scroll when new events arrive if user isn't scrolled up
watch(
  () => callsStore.selectedCallEvents.length,
  () => {
    if (!isScrolledUp.value) {
      scrollToBottom(false)
    }
  }
)

// Always scroll to bottom when selecting a different call
watch(
  () => callsStore.selectedCallId,
  () => {
    isScrolledUp.value = false
    scrollToBottom(false)
  }
)

onMounted(() => {
  scrollToBottom(false)
})

async function handleToggleHold() {
  if (!callsStore.selectedCallId) return
  isHoldingLoading.value = true
  try {
    await callsStore.toggleHold()
  } finally {
    isHoldingLoading.value = false
  }
}
</script>

<template>
  <div class="flex flex-col flex-1 min-h-0 relative h-full">
    <!-- Active call header & controls -->
    <div
      v-if="callsStore.selectedCall"
      class="flex-none flex items-center justify-between px-3 sm:px-5 py-2.5 sm:py-3 border-b border-[#d6dae1] dark:border-[#303844] bg-slate-50/70 dark:bg-slate-900/40 backdrop-blur-sm z-10"
    >
      <div class="flex items-center gap-2 min-w-0">
        <!-- Back button on mobile screens -->
        <button
          v-if="showBackButton"
          type="button"
          @click="$emit('back')"
          class="md:hidden inline-flex items-center gap-1 -ml-1 mr-1 px-2 py-1 text-xs font-medium rounded-lg text-slate-700 dark:text-slate-200 hover:bg-slate-200/60 dark:hover:bg-slate-800 transition-colors"
        >
          <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 19l-7-7 7-7" />
          </svg>
          <span>Calls</span>
        </button>

        <span class="text-xs text-[#667085] dark:text-[#9aa4b2] hidden sm:inline">Звонок:</span>
        <span class="font-mono text-xs font-semibold text-slate-800 dark:text-slate-200 truncate max-w-[130px] sm:max-w-[260px]">
          {{ callsStore.selectedCallId }}
        </span>
        <span
          v-if="callsStore.isCurrentCallOnHold"
          class="px-1.5 py-0.5 text-[10px] font-bold uppercase rounded bg-amber-500/20 text-amber-600 dark:text-amber-400 border border-amber-500/30 animate-pulse flex-none"
        >
          Hold
        </span>
      </div>

      <div class="flex items-center gap-2 flex-none">
        <button
          type="button"
          :disabled="isHoldingLoading"
          @click="handleToggleHold"
          :class="[
            'inline-flex items-center px-2.5 sm:px-3 py-1 sm:py-1.5 text-xs font-medium rounded-lg border transition-all',
            callsStore.isCurrentCallOnHold
              ? 'bg-amber-500/10 border-amber-500/30 text-amber-700 dark:text-amber-300 hover:bg-amber-500/20'
              : 'bg-white dark:bg-[#181c22] border-[#d6dae1] dark:border-[#303844] hover:bg-slate-50 dark:hover:bg-slate-800 text-slate-700 dark:text-slate-200'
          ]"
        >
          <span v-if="isHoldingLoading">...</span>
          <span v-else>{{ callsStore.isCurrentCallOnHold ? 'Resume' : 'Hold Call' }}</span>
        </button>
      </div>
    </div>

    <!-- Scrollable transcript events container -->
    <div
      ref="eventsContainer"
      @scroll="checkScrollPosition"
      class="flex-1 min-h-0 overflow-y-auto theme-scrollbar p-3 sm:p-5 flex flex-col gap-3.5 overscroll-contain"
    >
      <template v-if="callsStore.selectedCall">
        <template v-if="callsStore.selectedCallEvents.length > 0">
          <ChatBubble
            v-for="(event, idx) in callsStore.selectedCallEvents"
            :key="idx"
            :event="event"
          />
        </template>
        <div
          v-else
          class="m-auto text-xs text-[#667085] dark:text-[#9aa4b2] text-center p-4"
        >
          Listening for speech...
        </div>
      </template>

      <div
        v-else
        class="m-auto text-xs text-[#667085] dark:text-[#9aa4b2] text-center p-4"
      >
        Выберите звонок чтобы посмотреть транскрибцию.
      </div>
    </div>

    <!-- Floating Scroll-To-Bottom Button -->
    <transition
      enter-active-class="transition duration-150 ease-out"
      enter-from-class="transform translate-y-2 opacity-0"
      enter-to-class="transform translate-y-0 opacity-100"
      leave-active-class="transition duration-100 ease-in"
      leave-from-class="transform translate-y-0 opacity-100"
      leave-to-class="transform translate-y-2 opacity-0"
    >
      <button
        v-if="isScrolledUp && callsStore.selectedCallEvents.length > 0"
        type="button"
        @click="scrollToBottom(true)"
        class="absolute bottom-4 right-4 sm:right-6 px-3 py-1.5 bg-slate-900/85 dark:bg-slate-100/90 text-white dark:text-slate-900 text-xs font-medium rounded-full shadow-lg backdrop-blur-sm flex items-center gap-1.5 hover:bg-slate-900 dark:hover:bg-white transition-all z-20"
      >
        <span>Latest</span>
        <svg class="w-3.5 h-3.5 animate-bounce" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M19 14l-7 7m0 0l-7-7m7 7V3" />
        </svg>
      </button>
    </transition>
  </div>
</template>
