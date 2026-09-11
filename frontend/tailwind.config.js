/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{vue,js,ts,jsx,tsx}",
  ],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        bg: {
          light: '#f6f7f9',
          dark: '#101317',
        },
        panel: {
          light: '#ffffff',
          dark: '#181c22',
        },
        border: {
          light: '#d6dae1',
          dark: '#303844',
        },
        client: '#0f766e',
        operator: '#7c3aed',
        system: '#6b7280',
      },
      keyframes: {
        'bubble-in': {
          from: { opacity: '0', transform: 'translateY(6px)' },
          to: { opacity: '1', transform: 'translateY(0)' },
        },
        pulse: {
          '0%, 100%': { opacity: '1' },
          '50%': { opacity: '0.75' },
        }
      },
      animation: {
        'bubble-in': 'bubble-in 0.22s ease-out',
        'pulse-subtle': 'pulse 1.6s ease-in-out infinite',
      }
    },
  },
  plugins: [],
}
