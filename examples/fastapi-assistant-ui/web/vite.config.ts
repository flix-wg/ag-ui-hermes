import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [react()],
  build: { chunkSizeWarningLimit: 1500 },
  // ag-ui-hermes is linked from ../../../js, which has its own node_modules: use one copy.
  resolve: { dedupe: ["@ag-ui/client", "rxjs", "react", "react-dom"] },
  server: {
    proxy: {
      "/agent": `http://127.0.0.1:${process.env.SERVER_PORT ?? 8000}`,
    },
  },
});
