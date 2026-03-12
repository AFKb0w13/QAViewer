import cors from "cors";
import express from "express";

import { config } from "./config.js";
import { authenticateRequest } from "./lib/auth.js";
import { pool, waitForDatabase, withClient, withTransaction } from "./lib/db.js";
import { ensureSchema } from "./lib/schema.js";
import { ensureSeedData } from "./lib/seed.js";
import authRoutes from "./routes/auth.js";
import dashboardRoutes from "./routes/dashboard.js";
import layerRoutes from "./routes/layers.js";
import questionAreaRoutes from "./routes/questionAreas.js";

async function bootstrap(): Promise<void> {
  await waitForDatabase();
  await withClient(ensureSchema);
  await withTransaction(ensureSeedData);

  const app = express();

  app.use(
    cors({
      origin: config.frontendOrigin,
      credentials: false,
    }),
  );
  app.use(express.json({ limit: "10mb" }));
  app.use(express.urlencoded({ extended: true }));

  app.get("/api/health", (_req, res) => {
    res.json({ status: "ok" });
  });

  app.use("/api/auth", authRoutes);
  app.use("/api/dashboard", authenticateRequest, dashboardRoutes);
  app.use("/api/layers", authenticateRequest, layerRoutes);
  app.use("/api/question-areas", authenticateRequest, questionAreaRoutes);

  app.use((error: unknown, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
    console.error(error);
    res.status(500).json({ message: "Unexpected server error." });
  });

  app.listen(config.apiPort, () => {
    console.log(`API listening on http://localhost:${config.apiPort}`);
  });
}

bootstrap()
  .catch((error) => {
    console.error("Failed to start API", error);
    process.exitCode = 1;
  })
  .finally(async () => {
    // Keep the pool open while the server is running.
    if (process.exitCode) {
      await pool.end();
    }
  });
