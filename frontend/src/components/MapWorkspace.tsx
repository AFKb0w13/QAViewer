import { startTransition, useDeferredValue, useEffect, useState } from "react";
import type { ChangeEvent, FormEvent } from "react";

import type { Feature, FeatureCollection, Geometry } from "geojson";
import L from "leaflet";
import {
  GeoJSON,
  MapContainer,
  Pane,
  TileLayer,
  useMap,
  useMapEvents,
} from "react-leaflet";

import type { Session } from "../App";
import { apiDownload, apiRequest } from "../lib/api";

type SearchResult = {
  type: "question_area" | "parcel";
  id: string;
  label: string;
  subtitle: string;
  sourceGroup?: string;
};

type SummaryPayload = {
  questionAreas: number;
  comments: number;
  documents: number;
  statuses: Record<string, number>;
  severities: Record<string, number>;
};

type LayerKey =
  | "primary_parcels"
  | "parcel_points"
  | "management_tracts"
  | "tax_counties"
  | "management_counties";

type QuestionAreaFeature = Feature<
  Geometry,
  {
    code: string;
    sourceGroup: string;
    status: string;
    severity: string;
    title: string;
    summary: string;
    county: string | null;
    state: string | null;
    primaryParcelNumber: string | null;
    primaryParcelCode: string | null;
    primaryOwnerName: string | null;
    propertyName: string | null;
    analysisName: string | null;
    tractName: string | null;
    assignedReviewer: string | null;
  }
>;

type QuestionAreaCollection = FeatureCollection<Geometry, QuestionAreaFeature["properties"]>;

type QuestionAreaDetail = {
  id: number;
  code: string;
  sourceLayer: string;
  sourceGroup: string;
  status: string;
  severity: string;
  title: string;
  summary: string;
  description: string | null;
  county: string | null;
  state: string | null;
  primaryParcelNumber: string | null;
  primaryParcelCode: string | null;
  primaryOwnerName: string | null;
  propertyName: string | null;
  analysisName: string | null;
  tractName: string | null;
  assignedReviewer: string | null;
  sourceLayers: string[];
  relatedParcels: Array<{
    parcelNumber: string | null;
    parcelCode: string | null;
    ownerName: string | null;
    county: string | null;
    state: string | null;
    propertyName: string | null;
    analysisName: string | null;
    tractName: string | null;
    source: string;
  }>;
  metrics: Record<string, number | null>;
  geometry: Geometry;
  centroid: Geometry;
  comments: Array<{
    id: number;
    body: string;
    createdAt: string;
    authorName: string;
    authorRole: string;
  }>;
  documents: Array<{
    id: number;
    originalName: string;
    mimeType: string | null;
    sizeBytes: number;
    createdAt: string;
    downloadUrl: string;
  }>;
};

type EditDraft = {
  status: string;
  summary: string;
  description: string;
  assignedReviewer: string;
};

type MapWorkspaceProps = {
  session: Session;
  onLogout: () => void;
};

const STATUS_OPTIONS = ["review", "active", "resolved", "hold"];

const initialLayers: Record<LayerKey, boolean> = {
  primary_parcels: true,
  parcel_points: false,
  management_tracts: false,
  tax_counties: false,
  management_counties: true,
};

export function MapWorkspace({ session, onLogout }: MapWorkspaceProps) {
  const [summary, setSummary] = useState<SummaryPayload | null>(null);
  const [questionAreas, setQuestionAreas] = useState<QuestionAreaCollection | null>(null);
  const [mapBbox, setMapBbox] = useState("-126,24,-66,49");
  const [layerVisibility, setLayerVisibility] = useState(initialLayers);
  const [layerData, setLayerData] = useState<Partial<Record<LayerKey, FeatureCollection>>>({});
  const [searchInput, setSearchInput] = useState("");
  const [searchFilter, setSearchFilter] = useState("");
  const [searchResults, setSearchResults] = useState<SearchResult[]>([]);
  const [statusFilter, setStatusFilter] = useState("all");
  const [selectedCode, setSelectedCode] = useState<string | null>(null);
  const [selectedDetail, setSelectedDetail] = useState<QuestionAreaDetail | null>(null);
  const [editDraft, setEditDraft] = useState<EditDraft>({
    status: "review",
    summary: "",
    description: "",
    assignedReviewer: "",
  });
  const [commentDraft, setCommentDraft] = useState("");
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [feedback, setFeedback] = useState<string | null>(null);
  const [busy, setBusy] = useState({
    summary: false,
    questionAreas: false,
    detail: false,
    saving: false,
    commenting: false,
    uploading: false,
  });
  const deferredSearch = useDeferredValue(searchInput);

  useEffect(() => {
    let alive = true;
    setBusy((current) => ({ ...current, summary: true }));

    apiRequest<SummaryPayload>("/dashboard/summary", { token: session.token })
      .then((payload) => {
        if (alive) {
          setSummary(payload);
        }
      })
      .catch((error) => {
        if (alive) {
          setFeedback(error instanceof Error ? error.message : "Failed to load summary.");
        }
      })
      .finally(() => {
        if (alive) {
          setBusy((current) => ({ ...current, summary: false }));
        }
      });

    return () => {
      alive = false;
    };
  }, [session.token]);

  useEffect(() => {
    const query = deferredSearch.trim();
    if (query.length < 2) {
      setSearchResults([]);
      return;
    }

    let alive = true;
    apiRequest<{ results: SearchResult[] }>(`/dashboard/search?q=${encodeURIComponent(query)}`, {
      token: session.token,
    })
      .then((payload) => {
        if (alive) {
          startTransition(() => setSearchResults(payload.results));
        }
      })
      .catch(() => {
        if (alive) {
          setSearchResults([]);
        }
      });

    return () => {
      alive = false;
    };
  }, [deferredSearch, session.token]);

  useEffect(() => {
    let alive = true;
    const params = new URLSearchParams({
      bbox: mapBbox,
      limit: "600",
    });
    if (searchFilter) {
      params.set("search", searchFilter);
    }
    if (statusFilter !== "all") {
      params.set("status", statusFilter);
    }

    setBusy((current) => ({ ...current, questionAreas: true }));
    apiRequest<QuestionAreaCollection>(`/question-areas?${params.toString()}`, {
      token: session.token,
    })
      .then((payload) => {
        if (alive) {
          setQuestionAreas(payload);
          if (
            searchFilter &&
            payload.features.length === 1 &&
            payload.features[0].properties?.code !== selectedCode
          ) {
            setSelectedCode(payload.features[0].properties?.code ?? null);
          }
        }
      })
      .catch((error) => {
        if (alive) {
          setFeedback(error instanceof Error ? error.message : "Failed to load question areas.");
        }
      })
      .finally(() => {
        if (alive) {
          setBusy((current) => ({ ...current, questionAreas: false }));
        }
      });

    return () => {
      alive = false;
    };
  // eslint-disable-next-line react-hooks/exhaustive-deps -- selectedCode intentionally excluded to avoid redundant refetch on selection
  }, [mapBbox, searchFilter, session.token, statusFilter]);

  useEffect(() => {
    const visibleLayers = (Object.keys(layerVisibility) as LayerKey[]).filter(
      (layer) => layerVisibility[layer],
    );
    let alive = true;

    Promise.all(
      visibleLayers.map(async (layer) => {
        const payload = await apiRequest<FeatureCollection>(
          `/layers/${layer}?bbox=${encodeURIComponent(mapBbox)}`,
          {
            token: session.token,
          },
        );
        return [layer, payload] as const;
      }),
    )
      .then((entries) => {
        if (!alive) {
          return;
        }
        const nextLayerData: Partial<Record<LayerKey, FeatureCollection>> = {};
        entries.forEach(([layer, payload]) => {
          nextLayerData[layer] = payload;
        });
        setLayerData(nextLayerData);
      })
      .catch((error) => {
        if (alive) {
          setFeedback(error instanceof Error ? error.message : "Failed to load map layers.");
        }
      });

    return () => {
      alive = false;
    };
  }, [layerVisibility, mapBbox, session.token]);

  useEffect(() => {
    if (!selectedCode) {
      setSelectedDetail(null);
      return;
    }

    let alive = true;
    setBusy((current) => ({ ...current, detail: true }));

    apiRequest<QuestionAreaDetail>(`/question-areas/${selectedCode}`, { token: session.token })
      .then((payload) => {
        if (!alive) {
          return;
        }
        setSelectedDetail(payload);
        setEditDraft({
          status: payload.status,
          summary: payload.summary,
          description: payload.description ?? "",
          assignedReviewer: payload.assignedReviewer ?? "",
        });
      })
      .catch((error) => {
        if (alive) {
          setFeedback(error instanceof Error ? error.message : "Failed to load details.");
        }
      })
      .finally(() => {
        if (alive) {
          setBusy((current) => ({ ...current, detail: false }));
        }
      });

    return () => {
      alive = false;
    };
  }, [selectedCode, session.token]);

  async function refreshSummary() {
    const payload = await apiRequest<SummaryPayload>("/dashboard/summary", { token: session.token });
    setSummary(payload);
  }

  async function reloadDetail() {
    if (!selectedCode) {
      return;
    }
    const payload = await apiRequest<QuestionAreaDetail>(`/question-areas/${selectedCode}`, {
      token: session.token,
    });
    setSelectedDetail(payload);
    setEditDraft({
      status: payload.status,
      summary: payload.summary,
      description: payload.description ?? "",
      assignedReviewer: payload.assignedReviewer ?? "",
    });
  }

  async function handleSaveDetail() {
    if (!selectedCode) {
      return;
    }

    setBusy((current) => ({ ...current, saving: true }));
    setFeedback(null);

    try {
      await apiRequest(`/question-areas/${selectedCode}`, {
        method: "PATCH",
        token: session.token,
        body: {
          status: editDraft.status,
          summary: editDraft.summary,
          description: editDraft.description || null,
          assignedReviewer: editDraft.assignedReviewer || null,
        },
      });
      await Promise.all([reloadDetail(), refreshSummary()]);
      setFeedback("Question area updated.");
    } catch (error) {
      setFeedback(error instanceof Error ? error.message : "Update failed.");
    } finally {
      setBusy((current) => ({ ...current, saving: false }));
    }
  }

  async function handleCommentSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!selectedCode || !commentDraft.trim()) {
      return;
    }

    setBusy((current) => ({ ...current, commenting: true }));
    setFeedback(null);

    try {
      await apiRequest(`/question-areas/${selectedCode}/comments`, {
        method: "POST",
        token: session.token,
        body: { body: commentDraft.trim() },
      });
      setCommentDraft("");
      await Promise.all([reloadDetail(), refreshSummary()]);
    } catch (error) {
      setFeedback(error instanceof Error ? error.message : "Comment failed.");
    } finally {
      setBusy((current) => ({ ...current, commenting: false }));
    }
  }

  async function handleUploadDocument() {
    if (!selectedCode || !selectedFile) {
      return;
    }

    const formData = new FormData();
    formData.append("file", selectedFile);

    setBusy((current) => ({ ...current, uploading: true }));
    setFeedback(null);

    try {
      await apiRequest(`/question-areas/${selectedCode}/documents`, {
        method: "POST",
        token: session.token,
        formData,
      });
      setSelectedFile(null);
      await Promise.all([reloadDetail(), refreshSummary()]);
    } catch (error) {
      setFeedback(error instanceof Error ? error.message : "Upload failed.");
    } finally {
      setBusy((current) => ({ ...current, uploading: false }));
    }
  }

  async function handleDownloadDocument(fileRecord: QuestionAreaDetail["documents"][number]) {
    try {
      const blob = await apiDownload(fileRecord.downloadUrl.replace("/api", ""), session.token);
      const url = window.URL.createObjectURL(blob);
      const link = window.document.createElement("a");
      link.href = url;
      link.download = fileRecord.originalName;
      link.click();
      window.URL.revokeObjectURL(url);
    } catch (error) {
      setFeedback(error instanceof Error ? error.message : "Download failed.");
    }
  }

  function handleSearchSelection(result: SearchResult) {
    setSearchInput(result.label);
    setSearchResults([]);

    if (result.type === "question_area") {
      setSelectedCode(result.id);
      return;
    }

    setSearchFilter(result.label);
  }

  const questionAreaList = questionAreas?.features.slice(0, 12) ?? [];
  const activeCount = summary?.statuses.active ?? 0;
  const reviewCount = summary?.statuses.review ?? summary?.questionAreas ?? 0;
  const highSeverityCount = summary?.severities.high ?? 0;

  return (
    <main className="workspace-shell">
      <header className="workspace-header">
        <div>
          <p className="eyebrow">QAViewer</p>
          <h1>Question area review console</h1>
        </div>
        <div className="header-actions">
          <div className="user-chip">
            <span>{session.user.name}</span>
            <small>{session.user.role}</small>
          </div>
          <button className="ghost-button" onClick={onLogout} type="button">
            Sign out
          </button>
        </div>
      </header>

      <section className="workspace-grid">
        <aside className="workspace-panel left-panel">
          <section className="panel-section stats-section">
            <div className="stat-card">
              <span>Question areas</span>
              <strong>{summary?.questionAreas ?? "..."}</strong>
            </div>
            <div className="stat-card">
              <span>Under review</span>
              <strong>{busy.summary ? "..." : reviewCount}</strong>
            </div>
            <div className="stat-card">
              <span>High severity</span>
              <strong>{busy.summary ? "..." : highSeverityCount}</strong>
            </div>
            <div className="stat-card">
              <span>Active</span>
              <strong>{busy.summary ? "..." : activeCount}</strong>
            </div>
          </section>

          <section className="panel-section">
            <div className="section-heading">
              <h2>Search and filter</h2>
              <span>{busy.questionAreas ? "Refreshing..." : `${questionAreas?.features.length ?? 0} visible`}</span>
            </div>
            <form
              className="search-stack"
              onSubmit={(event) => {
                event.preventDefault();
                setSearchFilter(searchInput.trim());
              }}
            >
              <input
                className="search-input"
                placeholder="Search by QA ID, parcel, owner, project, keyword"
                value={searchInput}
                onChange={(event) => setSearchInput(event.target.value)}
              />
              {searchResults.length > 0 ? (
                <div className="search-results">
                  {searchResults.map((result) => (
                    <button
                      key={`${result.type}-${result.id}`}
                      className="search-result"
                      type="button"
                      onClick={() => handleSearchSelection(result)}
                    >
                      <strong>{result.label}</strong>
                      <span>{result.subtitle || result.type}</span>
                    </button>
                  ))}
                </div>
              ) : null}
              <div className="filter-row">
                <select value={statusFilter} onChange={(event) => setStatusFilter(event.target.value)}>
                  <option value="all">All statuses</option>
                  {STATUS_OPTIONS.map((status) => (
                    <option key={status} value={status}>
                      {status}
                    </option>
                  ))}
                </select>
                <button className="primary-button" type="submit">
                  Search map
                </button>
                <button
                  className="ghost-button"
                  type="button"
                  onClick={() => {
                    setSearchInput("");
                    setSearchFilter("");
                    setSearchResults([]);
                  }}
                >
                  Clear
                </button>
              </div>
            </form>
          </section>

          <section className="panel-section">
            <div className="section-heading">
              <h2>Layer stack</h2>
              <span>Leaflet controls</span>
            </div>
            <div className="layer-list">
              {(Object.keys(layerVisibility) as LayerKey[]).map((layer) => (
                <label key={layer} className="layer-toggle">
                  <input
                    checked={layerVisibility[layer]}
                    onChange={() =>
                      setLayerVisibility((current) => ({
                        ...current,
                        [layer]: !current[layer],
                      }))
                    }
                    type="checkbox"
                  />
                  <span>{layer.replaceAll("_", " ")}</span>
                </label>
              ))}
            </div>
          </section>

          <section className="panel-section result-list">
            <div className="section-heading">
              <h2>Visible question areas</h2>
              <span>{questionAreas?.features.length ?? 0} in viewport</span>
            </div>
            {questionAreaList.map((feature) => (
              <button
                key={feature.properties?.code}
                className={`list-card ${selectedCode === feature.properties?.code ? "selected" : ""}`}
                onClick={() => setSelectedCode(feature.properties?.code ?? null)}
                type="button"
              >
                <div>
                  <strong>{feature.properties?.title}</strong>
                  <span>{feature.properties?.code}</span>
                </div>
                <small>{feature.properties?.primaryOwnerName ?? feature.properties?.county}</small>
              </button>
            ))}
          </section>
        </aside>

        <section className="map-panel">
          <div className="map-statusbar">
            <span>{feedback ?? "Map synced to PostGIS-backed question area records."}</span>
          </div>
          <MapContainer center={[39.5, -95]} zoom={4} className="leaflet-shell" zoomControl={false}>
            <TileLayer
              attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
              url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
            />
            <MapViewportWatcher onChange={setMapBbox} />
            <MapFocus detail={selectedDetail} />

            <Pane name="counties" style={{ zIndex: 350 }}>
              {layerVisibility.management_counties && layerData.management_counties ? (
                <GeoJSON
                  data={layerData.management_counties}
                  style={{ color: "#2f6c6a", weight: 1.3, fillOpacity: 0 }}
                />
              ) : null}
              {layerVisibility.tax_counties && layerData.tax_counties ? (
                <GeoJSON
                  data={layerData.tax_counties}
                  style={{ color: "#8a7f6a", weight: 1.1, fillOpacity: 0 }}
                />
              ) : null}
            </Pane>

            <Pane name="management" style={{ zIndex: 370 }}>
              {layerVisibility.management_tracts && layerData.management_tracts ? (
                <GeoJSON
                  data={layerData.management_tracts}
                  style={{ color: "#4f7c4d", weight: 1.2, fillOpacity: 0.04, fillColor: "#6e9d5f" }}
                />
              ) : null}
            </Pane>

            <Pane name="parcels" style={{ zIndex: 390 }}>
              {layerVisibility.primary_parcels && layerData.primary_parcels ? (
                <GeoJSON
                  data={layerData.primary_parcels}
                  style={{ color: "#19324d", weight: 1.1, fillOpacity: 0.03, fillColor: "#7fa4b8" }}
                />
              ) : null}
            </Pane>

            <Pane name="points" style={{ zIndex: 410 }}>
              {layerVisibility.parcel_points && layerData.parcel_points ? (
                <GeoJSON
                  data={layerData.parcel_points}
                  pointToLayer={(_feature, latlng) =>
                    L.circleMarker(latlng, {
                      radius: 4,
                      color: "#b75e2b",
                      weight: 1,
                      fillColor: "#f6b26b",
                      fillOpacity: 0.9,
                    })
                  }
                />
              ) : null}
            </Pane>

            <Pane name="question-areas" style={{ zIndex: 430 }}>
              {questionAreas ? (
                <GeoJSON
                  data={questionAreas}
                  style={(feature) => questionAreaStyle(feature as QuestionAreaFeature, selectedCode)}
                  onEachFeature={(feature, layer) => {
                    layer.on("click", () => {
                      setSelectedCode((feature as QuestionAreaFeature).properties?.code ?? null);
                    });
                  }}
                />
              ) : null}
            </Pane>
          </MapContainer>
        </section>

        <aside className="workspace-panel right-panel">
          {busy.detail ? <p className="empty-state">Loading question area details...</p> : null}
          {!busy.detail && !selectedDetail ? (
            <p className="empty-state">
              Select a question area from the map or the result list to open its review dossier.
            </p>
          ) : null}

          {selectedDetail ? (
            <>
              <section className="panel-section">
                <div className="section-heading">
                  <h2>{selectedDetail.title}</h2>
                  <span>{selectedDetail.code}</span>
                </div>
                <div className="badge-row">
                  <span className={`badge severity-${selectedDetail.severity}`}>{selectedDetail.severity}</span>
                  <span className="badge neutral">{selectedDetail.status}</span>
                  <span className="badge neutral">{selectedDetail.sourceGroup}</span>
                </div>
                <dl className="detail-grid">
                  <DetailItem label="Parcel">{selectedDetail.primaryParcelCode ?? "None"}</DetailItem>
                  <DetailItem label="Owner">{selectedDetail.primaryOwnerName ?? "Unknown"}</DetailItem>
                  <DetailItem label="County">{selectedDetail.county ?? "Unknown"}</DetailItem>
                  <DetailItem label="State">{selectedDetail.state ?? "Unknown"}</DetailItem>
                  <DetailItem label="Property">{selectedDetail.propertyName ?? "None"}</DetailItem>
                  <DetailItem label="Analysis">{selectedDetail.analysisName ?? "None"}</DetailItem>
                </dl>
              </section>

              <section className="panel-section">
                <div className="section-heading">
                  <h2>Review controls</h2>
                  <span>Editable</span>
                </div>
                <div className="form-stack">
                  <label>
                    Status
                    <select
                      value={editDraft.status}
                      onChange={(event) =>
                        setEditDraft((current) => ({ ...current, status: event.target.value }))
                      }
                    >
                      {STATUS_OPTIONS.map((status) => (
                        <option key={status} value={status}>
                          {status}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label>
                    Assigned reviewer
                    <input
                      value={editDraft.assignedReviewer}
                      onChange={(event) =>
                        setEditDraft((current) => ({
                          ...current,
                          assignedReviewer: event.target.value,
                        }))
                      }
                    />
                  </label>
                  <label>
                    Summary
                    <textarea
                      rows={3}
                      value={editDraft.summary}
                      onChange={(event) =>
                        setEditDraft((current) => ({ ...current, summary: event.target.value }))
                      }
                    />
                  </label>
                  <label>
                    Notes
                    <textarea
                      rows={4}
                      value={editDraft.description}
                      onChange={(event) =>
                        setEditDraft((current) => ({ ...current, description: event.target.value }))
                      }
                    />
                  </label>
                  <button className="primary-button" disabled={busy.saving} onClick={handleSaveDetail} type="button">
                    {busy.saving ? "Saving..." : "Save review state"}
                  </button>
                </div>
              </section>

              <section className="panel-section">
                <div className="section-heading">
                  <h2>Metrics</h2>
                  <span>GIS-derived</span>
                </div>
                <dl className="detail-grid">
                  {Object.entries(selectedDetail.metrics).map(([key, value]) => (
                    <DetailItem key={key} label={humanize(key)}>
                      {formatMetric(value)}
                    </DetailItem>
                  ))}
                </dl>
              </section>

              <section className="panel-section">
                <div className="section-heading">
                  <h2>Related parcels</h2>
                  <span>{selectedDetail.relatedParcels.length} linked</span>
                </div>
                <div className="parcel-list">
                  {selectedDetail.relatedParcels.map((parcel) => (
                    <article key={`${parcel.parcelNumber}-${parcel.source}`} className="related-card">
                      <strong>{parcel.parcelCode ?? parcel.parcelNumber ?? "Parcel record"}</strong>
                      <span>{parcel.ownerName ?? "Unknown owner"}</span>
                      <small>
                        {[parcel.county, parcel.state, parcel.source].filter(Boolean).join(" | ")}
                      </small>
                    </article>
                  ))}
                </div>
              </section>

              <section className="panel-section">
                <div className="section-heading">
                  <h2>Discussion</h2>
                  <span>{selectedDetail.comments.length} comments</span>
                </div>
                <div className="comment-list">
                  {selectedDetail.comments.map((comment) => (
                    <article key={comment.id} className="comment-card">
                      <div>
                        <strong>{comment.authorName}</strong>
                        <small>{comment.authorRole}</small>
                      </div>
                      <p>{comment.body}</p>
                      <span>{new Date(comment.createdAt).toLocaleString()}</span>
                    </article>
                  ))}
                </div>
                <form className="form-stack" onSubmit={handleCommentSubmit}>
                  <label>
                    Add comment
                    <textarea
                      rows={3}
                      value={commentDraft}
                      onChange={(event) => setCommentDraft(event.target.value)}
                    />
                  </label>
                  <button className="primary-button" disabled={busy.commenting} type="submit">
                    {busy.commenting ? "Posting..." : "Post comment"}
                  </button>
                </form>
              </section>

              <section className="panel-section">
                <div className="section-heading">
                  <h2>Documents</h2>
                  <span>{selectedDetail.documents.length} attached</span>
                </div>
                <div className="document-list">
                  {selectedDetail.documents.map((document) => (
                    <article key={document.id} className="document-card">
                      <div>
                        <strong>{document.originalName}</strong>
                        <small>{formatFileSize(document.sizeBytes)}</small>
                      </div>
                      <button
                        className="ghost-button"
                        onClick={() => handleDownloadDocument(document)}
                        type="button"
                      >
                        Download
                      </button>
                    </article>
                  ))}
                </div>
                <div className="upload-row">
                  <input
                    onChange={(event: ChangeEvent<HTMLInputElement>) =>
                      setSelectedFile(event.target.files?.[0] ?? null)
                    }
                    type="file"
                  />
                  <button
                    className="primary-button"
                    disabled={!selectedFile || busy.uploading}
                    onClick={handleUploadDocument}
                    type="button"
                  >
                    {busy.uploading ? "Uploading..." : "Upload"}
                  </button>
                </div>
              </section>
            </>
          ) : null}
        </aside>
      </section>
    </main>
  );
}

function MapViewportWatcher({ onChange }: { onChange: (bbox: string) => void }) {
  const map = useMap();

  useEffect(() => {
    onChange(map.getBounds().toBBoxString());
  }, [map, onChange]);

  useMapEvents({
    moveend(event) {
      onChange(event.target.getBounds().toBBoxString());
    },
  });

  return null;
}

function MapFocus({ detail }: { detail: QuestionAreaDetail | null }) {
  const map = useMap();
  const code = detail?.code ?? null;

  useEffect(() => {
    if (!detail) {
      return;
    }

    const bounds = L.geoJSON(detail.geometry as never).getBounds();
    if (bounds.isValid()) {
      map.fitBounds(bounds.pad(0.35));
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps -- only re-center when a different QA is selected
  }, [code, map]);

  return null;
}

function DetailItem({ label, children }: { label: string; children: string }) {
  return (
    <div>
      <dt>{label}</dt>
      <dd>{children}</dd>
    </div>
  );
}

function questionAreaStyle(feature: QuestionAreaFeature | undefined, selectedCode: string | null) {
  const severity = feature?.properties?.severity ?? "medium";
  const active = feature?.properties?.code === selectedCode;
  const palette =
    severity === "high"
      ? { color: "#8c3b33", fillColor: "#c75b46" }
      : severity === "low"
        ? { color: "#7d6b48", fillColor: "#d8b974" }
        : { color: "#9f5a24", fillColor: "#e19347" };

  return {
    color: active ? "#111111" : palette.color,
    weight: active ? 3 : 2,
    fillColor: palette.fillColor,
    fillOpacity: active ? 0.45 : 0.28,
  };
}

function humanize(value: string) {
  return value
    .replace(/([a-z])([A-Z])/g, "$1 $2")
    .replace(/_/g, " ")
    .replace(/\b\w/g, (match) => match.toUpperCase());
}

function formatMetric(value: number | null | undefined) {
  if (value === null || value === undefined) {
    return "None";
  }
  return Number(value).toLocaleString(undefined, {
    maximumFractionDigits: 2,
  });
}

function formatFileSize(bytes: number) {
  if (bytes < 1024) {
    return `${bytes} B`;
  }
  if (bytes < 1024 * 1024) {
    return `${(bytes / 1024).toFixed(1)} KB`;
  }
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}
