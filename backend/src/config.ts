import dotenv from "dotenv";
import path from "node:path";

dotenv.config({ path: path.resolve(process.cwd(), "..", ".env") });
dotenv.config();

const backendDir = process.cwd();
const repoRoot = path.resolve(backendDir, "..");

export const config = {
  apiPort: Number(process.env.API_PORT ?? 3001),
  databaseUrl:
    process.env.DATABASE_URL ?? "postgres://qaviewer:qaviewer@localhost:5432/qaviewer",
  jwtSecret: process.env.JWT_SECRET ?? "change-me",
  frontendOrigin: process.env.FRONTEND_ORIGIN ?? "http://localhost:5173",
  backendDir,
  repoRoot,
  seedDir: path.join(repoRoot, "data", "generated"),
  uploadsDir: path.join(backendDir, "uploads"),
};
