import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The API runs on :8000; the dev server proxies /api so the browser only talks to :5173.
export default defineConfig({
  plugins: [react()],
  server: { proxy: { "/api": "http://127.0.0.1:8000" } },
  build: { chunkSizeWarningLimit: 800 },
});
