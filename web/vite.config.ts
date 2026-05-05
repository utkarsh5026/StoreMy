import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Dev server proxies /api → the Rust backend on :7878 so the browser
// makes same-origin requests and we don't have to deal with CORS during
// development. Override with STOREMY_API_URL when running against a
// non-default backend.
const backend = process.env.STOREMY_API_URL ?? "http://127.0.0.1:7878";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: backend,
        changeOrigin: true,
      },
    },
  },
});
