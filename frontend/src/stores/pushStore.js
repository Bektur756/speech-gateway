import { defineStore } from 'pinia'
import { ref } from 'vue'

export const usePushStore = defineStore('push', () => {
  const isSupported = ref(false)
  const isSubscribed = ref(false)
  const statusText = ref('Включить уведомления')
  const isLoading = ref(false)

  function urlBase64ToUint8Array(base64String) {
    const padding = '='.repeat((4 - (base64String.length % 4)) % 4)
    const base64 = (base64String + padding).replace(/-/g, '+').replace(/_/g, '/')
    const rawData = window.atob(base64)
    return Uint8Array.from([...rawData].map((c) => c.charCodeAt(0)))
  }

  async function checkExistingSubscription() {
    if (!('serviceWorker' in navigator) || !('PushManager' in window)) {
      isSupported.value = false
      statusText.value = 'Уведомления не поддерживаются'
      return
    }
    isSupported.value = true

    if (Notification.permission === 'denied') {
      statusText.value = 'Уведомления заблокированы.'
      return
    }

    try {
      const registration = await navigator.serviceWorker.register('/sw.js')
      const subscription = await registration.pushManager.getSubscription()
      if (subscription) {
        isSubscribed.value = true
        statusText.value = 'Включены уведомления'
      }
    } catch (err) {
      console.error('Push subscription check failed:', err)
    }
  }

  async function enableNotifications() {
    if (!isSupported.value) return
    isLoading.value = true
    try {
      const registration = await navigator.serviceWorker.register('/sw.js')
      const permission = await Notification.requestPermission()
      if (permission !== 'granted') {
        statusText.value = 'Уведомления заблокированы.'
        isLoading.value = false
        return
      }

      const res = await fetch('/push/vapid-public-key')
      const vapidPublicKey = await res.text()
      const subscription = await registration.pushManager.subscribe({
        userVisibleOnly: true,
        applicationServerKey: urlBase64ToUint8Array(vapidPublicKey),
      })

      await fetch('/push/subscribe', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(subscription.toJSON()),
      })

      isSubscribed.value = true
      statusText.value = 'Notifications on'
    } catch (err) {
      console.error('Push subscribe failed:', err)
      statusText.value = "Couldn't enable"
    } finally {
      isLoading.value = false
    }
  }

  return {
    isSupported,
    isSubscribed,
    statusText,
    isLoading,
    checkExistingSubscription,
    enableNotifications,
  }
})
