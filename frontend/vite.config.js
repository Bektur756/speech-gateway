import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import { resolve } from 'path'

export default defineConfig({
  plugins: [vue()],
  base: '/live/',
  build: {
    outDir: resolve(__dirname, '../app/static'),
    emptyOutDir: true,
  },
  server: {
    host: '0.0.0.0',
    port: 5173,
    proxy: {
      '/live/ws': {
        target: 'ws://localhost:8000',
        ws: true,
      },
      '/push': 'http://localhost:8000',
      '/conversations': 'http://localhost:8000',
    }
  }
})
