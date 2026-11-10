/// <reference types="vitest/config" />
import { defineConfig } from "vite";

// In development `ripple serve` runs separately on 9201; in production it serves web/dist itself.
export default defineConfig({
  server: { proxy: { "/api": "http://127.0.0.1:9201", "/metrics": "http://127.0.0.1:9201" } },
  test: { environment: "node" },
});
