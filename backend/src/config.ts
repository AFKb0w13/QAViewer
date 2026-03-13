import dotenv from "dotenv";
import path from "node:path";

dotenv.config({ path: path.resolve(process.cwd(), "..", ".env") });
dotenv.config();

const backendDir = process.cwd();
const repoRoot = path.resolve(backendDir, "..");

const jwtSecret = process.env.JWT_SECRET ?? "change-me";
if (jwtSecret === "change-me") {
  throw new Error(
    "JWT_SECRET is set to the insecure default. Set a strong secret via the JWT_SECRET environment variable.",
  );
}

export const config = {
  apiPort: Number(process.env.API_PORT ?? 3001),
  databaseUrl:
    process.env.DATABASE_URL ?? "postgres://qaviewer:qaviewer@localhost:5432/qaviewer",
  jwtSecret,
  demoMode: process.env.DEMO_MODE === "true",
  frontendOrigin: process.env.FRONTEND_ORIGIN ?? "http://localhost:5173",
  backendDir,
  repoRoot,
  seedDir: path.join(repoRoot, "data", "generated"),
  uploadsDir: path.join(backendDir, "uploads"),
};
