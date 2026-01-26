import type { Config } from 'tailwindcss'

const config: Config = {
  darkMode: ['class'],
  content: [
    './pages/**/*.{js,ts,jsx,tsx,mdx}',
    './components/**/*.{js,ts,jsx,tsx,mdx}',
    './app/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  theme: {
    extend: {
      colors: {
        // Bloomberg Terminal inspired colors
        terminal: {
          bg: '#0a0a0a',
          card: '#141414',
          border: '#2a2a2a',
          text: '#e5e5e5',
          muted: '#737373',
        },
        // Neon accents
        neon: {
          green: '#00ff88',
          red: '#ff4444',
          blue: '#00d4ff',
          purple: '#a855f7',
          gold: '#ffd700',
          orange: '#ff9500',
        },
        // Grade colors
        grade: {
          aaa: '#ffd700',  // Gold
          aa: '#c0c0c0',   // Silver
          a: '#10b981',    // Emerald
          b: '#3b82f6',    // Blue
          c: '#6b7280',    // Gray
          f: '#ef4444',    // Red
        },
      },
      fontFamily: {
        mono: ['JetBrains Mono', 'Fira Code', 'monospace'],
        sans: ['Inter', 'system-ui', 'sans-serif'],
      },
      animation: {
        'pulse-neon': 'pulse-neon 2s ease-in-out infinite',
        'glow': 'glow 2s ease-in-out infinite alternate',
      },
      keyframes: {
        'pulse-neon': {
          '0%, 100%': { opacity: '1' },
          '50%': { opacity: '0.5' },
        },
        'glow': {
          'from': { boxShadow: '0 0 5px currentColor, 0 0 10px currentColor' },
          'to': { boxShadow: '0 0 10px currentColor, 0 0 20px currentColor' },
        },
      },
    },
  },
  plugins: [require('tailwindcss-animate')],
}

export default config
