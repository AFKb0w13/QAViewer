import { Router } from "express";
import { z } from "zod";

import type { Role } from "../lib/auth.js";
import { authenticateRequest, comparePassword, signToken } from "../lib/auth.js";
import { query } from "../lib/db.js";

const router = Router();

const loginSchema = z.object({
  email: z.email(),
  password: z.string().min(1),
});

router.post("/login", async (req, res) => {
  const parsed = loginSchema.safeParse(req.body);
  if (!parsed.success) {
    res.status(400).json({ message: "Invalid login payload." });
    return;
  }

  const { rows } = await query<{
    id: number;
    name: string;
    email: string;
    password_hash: string;
    role: string;
  }>(
    `SELECT id, name, email, password_hash, role FROM users WHERE email = $1`,
    [parsed.data.email],
  );

  const user = rows[0];
  if (!user) {
    res.status(401).json({ message: "Invalid credentials." });
    return;
  }

  const passwordMatches = await comparePassword(parsed.data.password, user.password_hash);
  if (!passwordMatches) {
    res.status(401).json({ message: "Invalid credentials." });
    return;
  }

  const token = signToken({
    id: user.id,
    email: user.email,
    name: user.name,
    role: user.role as Role,
  });

  res.json({
    token,
    user: {
      id: user.id,
      email: user.email,
      name: user.name,
      role: user.role,
    },
  });
});

router.get("/me", authenticateRequest, (req, res) => {
  if (!req.user) {
    res.status(401).json({ message: "Not authenticated." });
    return;
  }

  res.json({ user: req.user });
});

export default router;
