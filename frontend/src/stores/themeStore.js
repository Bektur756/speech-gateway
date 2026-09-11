import { defineStore } from 'pinia'
import { ref } from 'vue'

export const useThemeStore = defineStore('theme', () => {
  const isDark = ref(false)

  function applyTheme(dark) {
    isDark.value = dark
    if (dark) {
      document.documentElement.classList.add('dark')
    } else {
      document.documentElement.classList.remove('dark')
    }
  }

  function initTheme() {
    const savedTheme = localStorage.getItem('theme')
    if (savedTheme) {
      applyTheme(savedTheme === 'dark')
    } else {
      const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches
      applyTheme(prefersDark)
    }

    // Listen for OS-level theme changes when no manual override is set
    window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', (e) => {
      if (!localStorage.getItem('theme')) {
        applyTheme(e.matches)
      }
    })
  }

  function toggleTheme() {
    const nextState = !isDark.value
    applyTheme(nextState)
    localStorage.setItem('theme', nextState ? 'dark' : 'light')
  }

  return {
    isDark,
    initTheme,
    toggleTheme,
  }
})
