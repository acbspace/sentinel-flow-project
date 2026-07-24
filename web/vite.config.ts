import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The dashboard is served from the same origin as the API in production (nginx
// proxies /v1 through), so the dev server mirrors that with a proxy rather than
// making the browser talk cross-origin. That keeps the app's fetch paths
// identical in both modes and means the API needs no CORS handling at all.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,
    proxy: {
      "/v1": {
        target: process.env.INCIDENTS_API_URL ?? "http://localhost:8084",
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: "dist",
    sourcemap: true,
  },
});
