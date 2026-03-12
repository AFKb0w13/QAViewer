import { useState } from "react";

import { LoginScreen } from "./components/LoginScreen";
import { MapWorkspace } from "./components/MapWorkspace";
import { apiRequest } from "./lib/api";

export type Session = {
  token: string;
  user: {
    id: number;
    email: string;
    name: string;
    role: string;
  };
};

const STORAGE_KEY = "qaviewer.session";

function loadSession(): Session | null {
  const raw = window.localStorage.getItem(STORAGE_KEY);
  if (!raw) {
    return null;
  }

  try {
    return JSON.parse(raw) as Session;
  } catch {
    window.localStorage.removeItem(STORAGE_KEY);
    return null;
  }
}

export default function App() {
  const [session, setSession] = useState<Session | null>(() => loadSession());

  async function handleLogin(credentials: { email: string; password: string }) {
    const payload = await apiRequest<Session>("/auth/login", {
      method: "POST",
      body: credentials,
    });
    setSession(payload);
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(payload));
  }

  function handleLogout() {
    setSession(null);
    window.localStorage.removeItem(STORAGE_KEY);
  }

  if (!session) {
    return <LoginScreen onLogin={handleLogin} />;
  }

  return <MapWorkspace session={session} onLogout={handleLogout} />;
}
