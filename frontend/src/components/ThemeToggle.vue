<script setup>
import { useThemeStore } from '../stores/themeStore'

const themeStore = useThemeStore()
</script>

<template>
  <button
    type="button"
    @click="themeStore.toggleTheme"
    :title="themeStore.isDark ? 'Switch to light mode' : 'Switch to dark mode'"
    aria-label="Toggle theme"
    class="inline-flex items-center justify-center w-8 h-8 rounded-lg border border-[#d6dae1] dark:border-[#303844] bg-white dark:bg-[#181c22] text-slate-700 dark:text-slate-200 hover:bg-slate-50 dark:hover:bg-slate-800 transition-colors focus:outline-none focus:ring-2 focus:ring-slate-400"
  >
    <!-- Sun icon (shown when dark to switch to light) -->
    <svg
      v-if="themeStore.isDark"
      class="w-4 h-4 text-amber-400 transform transition-transform duration-200 rotate-0 hover:rotate-45"
      fill="none"
      stroke="currentColor"
      stroke-width="2"
      viewBox="0 0 24 24"
    >
      <path stroke-linecap="round" stroke-linejoin="round" d="M12 3v1m0 16v1m9-9h-1M4 12H3m15.364 6.364l-.707-.707M6.343 6.343l-.707-.707m12.728 0l-.707.707M6.343 17.657l-.707.707M16 12a4 4 0 11-8 0 4 4 0 018 0z" />
    </svg>

    <!-- Moon icon (shown when light to switch to dark) -->
    <svg
      v-else
      class="w-4 h-4 text-slate-600 transform transition-transform duration-200 -rotate-12 hover:rotate-0"
      fill="none"
      stroke="currentColor"
      stroke-width="2"
      viewBox="0 0 24 24"
    >
      <path stroke-linecap="round" stroke-linejoin="round" d="M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z" />
    </svg>
  </button>
</template>
