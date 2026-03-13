import bcrypt from "bcryptjs";
import type { NextFunction, Request, Response } from "express";
import jwt from "jsonwebtoken";

import { config } from "../config.js";

export type Role = "admin" | "reviewer" | "client";

export type AuthUser = {
  id: number;
  email: string;
  name: string;
  role: Role;
};

type TokenPayload = AuthUser;

export async function hashPassword(password: string): Promise<string> {
  return bcrypt.hash(password, 10);
}

export async function comparePassword(password: string, hash: string): Promise<boolean> {
  return bcrypt.compare(password, hash);
}

export function signToken(user: AuthUser): string {
  return jwt.sign(user, config.jwtSecret, { expiresIn: "12h" });
}

export function authenticateRequest(req: Request, res: Response, next: NextFunction): void {
  const header = req.headers.authorization;
  if (!header?.startsWith("Bearer ")) {
    res.status(401).json({ message: "Missing bearer token." });
    return;
  }

  const token = header.slice("Bearer ".length);

  try {
    const payload = jwt.verify(token, config.jwtSecret) as TokenPayload;
    req.user = payload;
    next();
  } catch {
    res.status(401).json({ message: "Invalid or expired token." });
  }
}

export function requireRole(...allowed: Role[]) {
  return (req: Request, res: Response, next: NextFunction): void => {
    const user = req.user;
    if (!user) {
      res.status(401).json({ message: "Not authenticated." });
      return;
    }
    if (!allowed.includes(user.role as Role)) {
      res.status(403).json({ message: "Insufficient permissions." });
      return;
    }
    next();
  };
}
