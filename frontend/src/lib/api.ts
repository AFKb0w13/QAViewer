const API_BASE = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:3001/api";

type RequestOptions = {
  method?: string;
  token?: string;
  body?: unknown;
  formData?: FormData;
};

export async function apiRequest<T>(path: string, options: RequestOptions = {}): Promise<T> {
  const headers = new Headers();
  if (options.token) {
    headers.set("Authorization", `Bearer ${options.token}`);
  }
  if (!options.formData) {
    headers.set("Content-Type", "application/json");
  }

  const response = await fetch(`${API_BASE}${path}`, {
    method: options.method ?? "GET",
    headers,
    body: options.formData ?? (options.body ? JSON.stringify(options.body) : undefined),
  });

  if (!response.ok) {
    const payload = await response.json().catch(() => ({ message: "Request failed." }));
    throw new Error(payload.message ?? "Request failed.");
  }

  return response.json() as Promise<T>;
}

export async function apiDownload(path: string, token: string): Promise<Blob> {
  const response = await fetch(`${API_BASE}${path}`, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });

  if (!response.ok) {
    throw new Error("Download failed.");
  }

  return response.blob();
}
