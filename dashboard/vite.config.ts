import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

// The Express server (server.ts) serves the production build under /app, so assets
// must resolve relative to that base. In dev, Vite proxies /api to the crawler server.
const API_TARGET = process.env.VITE_API_TARGET ?? "http://localhost:3456";

export default defineConfig({
  base: "/app/",
  plugins: [react(), tailwindcss()],
  server: {
    port: 5173,
    proxy: {
      "/api": { target: API_TARGET, changeOrigin: true },
    },
  },
  build: {
    outDir: "dist",
    sourcemap: true,
  },
});
