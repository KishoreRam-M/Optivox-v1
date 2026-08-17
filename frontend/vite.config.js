import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  // base: './' ensures correct asset paths on Vercel and subdirectory deployments
  base: '/',
  server: {
    port: 5173,
    // Dev proxy: forwards /api requests to the local backend
    // This lets the frontend use '/api/...' without hardcoding localhost in dev
    proxy: {
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true,
        rewrite: (path) => path,
      },
      '/ws': {
        target: 'ws://localhost:8000',
        ws: true,
        changeOrigin: true,
      },
    },
  },
  build: {
    // Generate source maps for easier production debugging
    sourcemap: false,
    // Optimize chunk splitting for better caching (Vite 8 requires function syntax)
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (id.includes('node_modules/react') || id.includes('node_modules/react-dom')) {
            return 'vendor';
          }
          if (id.includes('node_modules/react-markdown') || id.includes('node_modules/remark-gfm')) {
            return 'markdown';
          }
        },
      },
    },
  },
})
