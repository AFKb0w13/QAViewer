import { useEffect, useState } from "react";
import type { FormEvent } from "react";

import type { Session, UserRole } from "../App";
import { apiRequest } from "../lib/api";

type ManagedUser = {
  id: number;
  name: string;
  email: string;
  role: UserRole;
  createdAt: string;
  commentCount: number;
  documentCount: number;
};

type UserPayload = {
  users: ManagedUser[];
};

type UserResponse = {
  user: ManagedUser;
};

type UserDraft = {
  name: string;
  email: string;
  role: UserRole;
  password: string;
};

type FeedbackState = {
  message: string;
  type: "success" | "error" | "info";
};

type AdminTab = "users" | "create" | "details";

type AdminWorkspaceProps = {
  session: Session;
  onLogout: () => void;
  onOpenReview: () => void;
  onSessionUpdate: (user: Session["user"]) => void;
};

const ROLE_OPTIONS: UserRole[] = ["admin", "client"];

function emptyUserDraft(role: UserRole = "client"): UserDraft {
  return {
    name: "",
    email: "",
    role,
    password: "",
  };
}

function labelRole(role: UserRole): string {
  return role.charAt(0).toUpperCase() + role.slice(1);
}

function summarizeActivity(user: ManagedUser): string {
  const parts: string[] = [];

  if (user.commentCount > 0) {
    parts.push(`${user.commentCount} comments`);
  }
  if (user.documentCount > 0) {
    parts.push(`${user.documentCount} documents`);
  }

  return parts.length > 0 ? parts.join(" | ") : "No authored activity yet";
}

function getInitials(name: string) {
  return name
    .split(" ")
    .map((segment) => segment[0] ?? "")
    .join("")
    .toUpperCase()
    .slice(0, 2);
}

export function AdminWorkspace({
  session,
  onLogout,
  onOpenReview,
  onSessionUpdate,
}: AdminWorkspaceProps) {
  const [users, setUsers] = useState<ManagedUser[]>([]);
  const [selectedUserId, setSelectedUserId] = useState<number | null>(null);
  const [searchTerm, setSearchTerm] = useState("");
  const [activeTab, setActiveTab] = useState<AdminTab>("users");
  const [createDraft, setCreateDraft] = useState<UserDraft>(() => emptyUserDraft());
  const [editDraft, setEditDraft] = useState<UserDraft>(() => emptyUserDraft());
  const [feedback, setFeedback] = useState<FeedbackState | null>(null);
  const [busy, setBusy] = useState({
    loading: false,
    creating: false,
    saving: false,
    deleting: false,
  });

  const selectedUser = users.find((user) => user.id === selectedUserId) ?? null;
  const adminCount = users.filter((user) => user.role === "admin").length;
  const filteredUsers = users.filter((user) => {
    const query = searchTerm.trim().toLowerCase();
    if (!query) {
      return true;
    }

    return (
      user.name.toLowerCase().includes(query) ||
      user.email.toLowerCase().includes(query)
    );
  });

  async function loadUsers(preferredSelection?: number | null) {
    setBusy((current) => ({ ...current, loading: true }));

    try {
      const payload = await apiRequest<UserPayload>("/admin/users", {
        token: session.token,
      });

      setUsers(payload.users);
      setSelectedUserId((current) => {
        if (preferredSelection !== undefined) {
          return payload.users.some((user) => user.id === preferredSelection)
            ? preferredSelection
            : null;
        }

        return current && payload.users.some((user) => user.id === current) ? current : null;
      });
    } catch (error) {
      setFeedback({
        message: error instanceof Error ? error.message : "Failed to load users.",
        type: "error",
      });
    } finally {
      setBusy((current) => ({ ...current, loading: false }));
    }
  }

  useEffect(() => {
    void loadUsers();
  }, [session.token]);

  useEffect(() => {
    if (!selectedUser) {
      setEditDraft(emptyUserDraft());
      return;
    }

    setEditDraft({
      name: selectedUser.name,
      email: selectedUser.email,
      role: selectedUser.role,
      password: "",
    });
  }, [selectedUser]);

  useEffect(() => {
    if (!selectedUser && activeTab === "details") {
      setActiveTab("users");
    }
  }, [activeTab, selectedUser]);

  useEffect(() => {
    if (!feedback) {
      return;
    }

    const timer = window.setTimeout(() => setFeedback(null), 5000);
    return () => window.clearTimeout(timer);
  }, [feedback]);

  async function handleCreateUser(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setBusy((current) => ({ ...current, creating: true }));

    try {
      const payload = await apiRequest<UserResponse>("/admin/users", {
        method: "POST",
        token: session.token,
        body: createDraft,
      });

      setCreateDraft(emptyUserDraft());
      await loadUsers(payload.user.id);
      setFeedback({ message: `Created ${payload.user.name}.`, type: "success" });
      setActiveTab("details");
    } catch (error) {
      setFeedback({
        message: error instanceof Error ? error.message : "Failed to create user.",
        type: "error",
      });
    } finally {
      setBusy((current) => ({ ...current, creating: false }));
    }
  }

  async function handleSaveUser(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    if (!selectedUser) {
      return;
    }

    setBusy((current) => ({ ...current, saving: true }));

    const body: Partial<UserDraft> = {
      name: editDraft.name,
      email: editDraft.email,
      role: editDraft.role,
    };
    if (editDraft.password.trim()) {
      body.password = editDraft.password;
    }

    try {
      const payload = await apiRequest<UserResponse>(`/admin/users/${selectedUser.id}`, {
        method: "PATCH",
        token: session.token,
        body,
      });

      if (selectedUser.id === session.user.id) {
        onSessionUpdate(payload.user);
      }

      await loadUsers(payload.user.id);
      setFeedback({ message: `Updated ${payload.user.name}.`, type: "success" });
    } catch (error) {
      setFeedback({
        message: error instanceof Error ? error.message : "Failed to update user.",
        type: "error",
      });
    } finally {
      setBusy((current) => ({ ...current, saving: false }));
    }
  }

  async function handleDeleteUser() {
    if (!selectedUser) {
      return;
    }

    const confirmed = window.confirm(`Delete ${selectedUser.name}? This cannot be undone.`);
    if (!confirmed) {
      return;
    }

    setBusy((current) => ({ ...current, deleting: true }));

    try {
      await apiRequest<void>(`/admin/users/${selectedUser.id}`, {
        method: "DELETE",
        token: session.token,
      });

      const deletedName = selectedUser.name;
      await loadUsers(null);
      setActiveTab("users");
      setFeedback({ message: `Deleted ${deletedName}.`, type: "success" });
    } catch (error) {
      setFeedback({
        message: error instanceof Error ? error.message : "Failed to delete user.",
        type: "error",
      });
    } finally {
      setBusy((current) => ({ ...current, deleting: false }));
    }
  }

  const deleteBlocked =
    !selectedUser ||
    selectedUser.id === session.user.id ||
    selectedUser.commentCount > 0 ||
    selectedUser.documentCount > 0 ||
    (selectedUser.role === "admin" && adminCount <= 1);

  return (
    <main className="workspace-shell">
      <header className="workspace-header">
        <div className="header-brand">
          <p className="eyebrow">QAViewer</p>
          <h1>Administration</h1>
        </div>
        <div className="header-actions">
          <div className="header-actions-layout">
            <div className="header-button-row">
              <button className="ghost-button" onClick={onOpenReview} type="button">
                Review workspace
              </button>
              <button className="ghost-button" onClick={onLogout} type="button">
                Sign out
              </button>
            </div>
            <span className="user-name-sub">{session.user.name}</span>
          </div>
        </div>
      </header>


      {feedback ? <div className={`toast toast-${feedback.type}`}>{feedback.message}</div> : null}

      <section className="admin-grid">
        <section className="workspace-panel">
          <nav className="tab-nav" aria-label="Admin navigation">
            <button
              className={`tab-link ${activeTab === "users" ? "active" : ""}`}
              onClick={() => setActiveTab("users")}
              type="button"
            >
              Users
            </button>
            <button
              className={`tab-link ${activeTab === "create" ? "active" : ""}`}
              onClick={() => setActiveTab("create")}
              type="button"
            >
              Create user
            </button>
            {selectedUser ? (
              <button
                className={`tab-link ${activeTab === "details" ? "active" : ""}`}
                onClick={() => setActiveTab("details")}
                type="button"
              >
                User details
              </button>
            ) : null}
          </nav>

          <div className="tab-content">
            {activeTab === "users" ? (
              <section className="panel-section admin-tab-section">
                <div className="section-heading">
                  <h2>Users</h2>
                  <span>{busy.loading ? "Refreshing..." : `${filteredUsers.length} shown`}</span>
                </div>
                <div className="search-box admin-search-box">
                  <input
                    className="search-input"
                    onChange={(event) => setSearchTerm(event.target.value)}
                    placeholder="Search users..."
                    type="text"
                    value={searchTerm}
                  />
                </div>
                <div className="result-list admin-user-list">
                  {filteredUsers.map((user) => (
                    <button
                      key={user.id}
                      className={`list-card user-card ${user.id === selectedUserId ? "selected" : ""}`}
                      onClick={() => {
                        setSelectedUserId(user.id);
                        setActiveTab("details");
                      }}
                      type="button"
                    >
                      <div className="user-avatar">{getInitials(user.name)}</div>
                      <div className="user-info">
                        <div className="user-card-head">
                          <strong>{user.name}</strong>
                          <span className={`badge role-badge role-${user.role}`}>{labelRole(user.role)}</span>
                        </div>
                        <span className="user-email">{user.email}</span>
                        <small className="user-activity">
                          {user.id === session.user.id ? "Current account | " : ""}
                          {summarizeActivity(user)}
                        </small>
                      </div>
                    </button>
                  ))}
                  {!busy.loading && filteredUsers.length === 0 ? (
                    <p className="empty-state">No users match your search.</p>
                  ) : null}
                </div>
              </section>
            ) : null}

            {activeTab === "create" ? (
              <section className="panel-section admin-tab-section">
                <div className="section-heading">
                  <h2>Create user</h2>
                  <span>New account credentials</span>
                </div>
                <form className="form-stack" onSubmit={handleCreateUser}>
                  <label>
                    Name
                    <input
                      placeholder="Full name"
                      required
                      value={createDraft.name}
                      onChange={(event) =>
                        setCreateDraft((current) => ({ ...current, name: event.target.value }))
                      }
                    />
                  </label>
                  <div className="admin-form-grid">
                    <label>
                      Email
                      <input
                        placeholder="email@example.com"
                        required
                        type="email"
                        value={createDraft.email}
                        onChange={(event) =>
                          setCreateDraft((current) => ({ ...current, email: event.target.value }))
                        }
                      />
                    </label>
                    <label>
                      Role
                      <select
                        value={createDraft.role}
                        onChange={(event) =>
                          setCreateDraft((current) => ({
                            ...current,
                            role: event.target.value as UserRole,
                          }))
                        }
                      >
                        {ROLE_OPTIONS.map((role) => (
                          <option key={role} value={role}>
                            {labelRole(role)}
                          </option>
                        ))}
                      </select>
                    </label>
                  </div>
                  <label>
                    Password
                    <input
                      placeholder="Minimum 8 characters"
                      required
                      type="password"
                      value={createDraft.password}
                      onChange={(event) =>
                        setCreateDraft((current) => ({ ...current, password: event.target.value }))
                      }
                    />
                  </label>
                  <div className="form-actions">
                    <button className="primary-button" disabled={busy.creating} type="submit">
                      {busy.creating ? "Creating..." : "Create user"}
                    </button>
                  </div>
                </form>
              </section>
            ) : null}

            {activeTab === "details" && selectedUser ? (
              <section className="panel-section admin-tab-section">
                <div className="section-heading">
                  <h2>User details</h2>
                  <span>{selectedUser.email}</span>
                </div>

                <div className="admin-user-meta-v2">
                  <div className="meta-item">
                    <small>Member since</small>
                    <span>{new Date(selectedUser.createdAt).toLocaleDateString()}</span>
                  </div>
                  <div className="meta-item">
                    <small>Authored content</small>
                    <span>{summarizeActivity(selectedUser)}</span>
                  </div>
                </div>

                <form className="form-stack" onSubmit={handleSaveUser}>
                  <label>
                    Name
                    <input
                      value={editDraft.name}
                      onChange={(event) =>
                        setEditDraft((current) => ({ ...current, name: event.target.value }))
                      }
                    />
                  </label>
                  <div className="admin-form-grid">
                    <label>
                      Email
                      <input
                        type="email"
                        value={editDraft.email}
                        onChange={(event) =>
                          setEditDraft((current) => ({ ...current, email: event.target.value }))
                        }
                      />
                    </label>
                    <label>
                      Role
                      <select
                        value={editDraft.role}
                        onChange={(event) =>
                          setEditDraft((current) => ({
                            ...current,
                            role: event.target.value as UserRole,
                          }))
                        }
                      >
                        {ROLE_OPTIONS.map((role) => (
                          <option key={role} value={role}>
                            {labelRole(role)}
                          </option>
                        ))}
                      </select>
                    </label>
                  </div>
                  <label>
                    Update password
                    <input
                      placeholder="Leave blank to keep current"
                      type="password"
                      value={editDraft.password}
                      onChange={(event) =>
                        setEditDraft((current) => ({ ...current, password: event.target.value }))
                      }
                    />
                  </label>
                  <div className="form-actions">
                    <button className="primary-button" disabled={busy.saving} type="submit">
                      {busy.saving ? "Saving changes..." : "Save user changes"}
                    </button>
                  </div>
                </form>

                <div className="admin-danger-zone">
                  <div className="danger-info">
                    <h3>Delete user</h3>
                    <p>
                      Removing a user is permanent. This action is disabled for the current admin,
                      the last admin, and users with authored activity.
                    </p>
                  </div>
                  <button
                    className="danger-button"
                    disabled={deleteBlocked}
                    onClick={handleDeleteUser}
                    type="button"
                  >
                    {busy.deleting ? "Deleting..." : "Delete user"}
                  </button>
                </div>
              </section>
            ) : null}
          </div>
        </section>
      </section>
    </main>
  );
}
