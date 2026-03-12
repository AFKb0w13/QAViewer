import { FormEvent, useState } from "react";

type LoginScreenProps = {
  onLogin: (credentials: { email: string; password: string }) => Promise<void>;
};

const DEMO_ACCOUNTS = [
  { label: "Admin", email: "admin@qaviewer.local", password: "admin123!" },
  { label: "Reviewer", email: "reviewer@qaviewer.local", password: "review123!" },
  { label: "Client", email: "client@qaviewer.local", password: "client123!" },
];

export function LoginScreen({ onLogin }: LoginScreenProps) {
  const [email, setEmail] = useState(DEMO_ACCOUNTS[1].email);
  const [password, setPassword] = useState(DEMO_ACCOUNTS[1].password);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSubmitting(true);
    setError(null);

    try {
      await onLogin({ email, password });
    } catch (submitError) {
      setError(submitError instanceof Error ? submitError.message : "Login failed.");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <main className="login-shell">
      <section className="login-card">
        <p className="eyebrow">Question Area Review Web App</p>
        <h1>Review spatial mismatches without leaving the map.</h1>
        <p className="lead">
          This workspace is seeded from the provided geodatabase and centered on question areas
          derived from the mismatch layers.
        </p>

        <div className="demo-grid">
          {DEMO_ACCOUNTS.map((account) => (
            <button
              key={account.email}
              className="demo-pill"
              type="button"
              onClick={() => {
                setEmail(account.email);
                setPassword(account.password);
              }}
            >
              <span>{account.label}</span>
              <small>{account.email}</small>
            </button>
          ))}
        </div>

        <form className="login-form" onSubmit={handleSubmit}>
          <label>
            Email
            <input value={email} onChange={(event) => setEmail(event.target.value)} />
          </label>
          <label>
            Password
            <input
              type="password"
              value={password}
              onChange={(event) => setPassword(event.target.value)}
            />
          </label>
          {error ? <p className="form-error">{error}</p> : null}
          <button className="primary-button" disabled={submitting} type="submit">
            {submitting ? "Signing in..." : "Enter reviewer workspace"}
          </button>
        </form>
      </section>
    </main>
  );
}
