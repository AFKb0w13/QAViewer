import fs from "node:fs/promises";
import path from "node:path";

import { Router } from "express";
import multer from "multer";
import { z } from "zod";

import { config } from "../config.js";
import { query } from "../lib/db.js";
import { featureCollection, parseBbox } from "../lib/utils.js";

const router = Router();

const upload = multer({
  storage: multer.diskStorage({
    destination: (_req, _file, callback) => {
      callback(null, config.uploadsDir);
    },
    filename: (_req, file, callback) => {
      const extension = path.extname(file.originalname);
      callback(null, `${Date.now()}-${Math.random().toString(36).slice(2)}${extension}`);
    },
  }),
  limits: {
    fileSize: 25 * 1024 * 1024,
  },
});

const updateSchema = z.object({
  status: z.string().min(1).optional(),
  summary: z.string().min(1).optional(),
  description: z.string().nullable().optional(),
  assignedReviewer: z.string().nullable().optional(),
});

const commentSchema = z.object({
  body: z.string().min(3).max(2000),
});

router.get("/", async (req, res) => {
  const clauses: string[] = [];
  const params: unknown[] = [];

  const search = String(req.query.search ?? "").trim();
  if (search) {
    params.push(`%${search}%`);
    const placeholder = `$${params.length}`;
    clauses.push(`
      (
        code ILIKE ${placeholder}
        OR title ILIKE ${placeholder}
        OR summary ILIKE ${placeholder}
        OR COALESCE(primary_parcel_number, '') ILIKE ${placeholder}
        OR COALESCE(primary_parcel_code, '') ILIKE ${placeholder}
        OR COALESCE(primary_owner_name, '') ILIKE ${placeholder}
        OR COALESCE(property_name, '') ILIKE ${placeholder}
        OR COALESCE(analysis_name, '') ILIKE ${placeholder}
        OR COALESCE(tract_name, '') ILIKE ${placeholder}
        OR COALESCE(search_keywords, '') ILIKE ${placeholder}
      )
    `);
  }

  const status = String(req.query.status ?? "").trim();
  if (status) {
    params.push(status);
    clauses.push(`status = $${params.length}`);
  }

  const bbox = parseBbox(String(req.query.bbox ?? ""));
  if (bbox) {
    const [west, south, east, north] = bbox;
    params.push(west, south, east, north);
    clauses.push(
      `geom && ST_MakeEnvelope($${params.length - 3}, $${params.length - 2}, $${params.length - 1}, $${params.length}, 4326)`,
    );
  }

  const limit = Math.min(Number(req.query.limit ?? 500), 1000);
  const whereClause = clauses.length > 0 ? `WHERE ${clauses.join(" AND ")}` : "";

  const result = await query<{
    code: string;
    source_group: string;
    status: string;
    severity: string;
    title: string;
    summary: string;
    county: string | null;
    state: string | null;
    primary_parcel_number: string | null;
    primary_parcel_code: string | null;
    primary_owner_name: string | null;
    property_name: string | null;
    analysis_name: string | null;
    tract_name: string | null;
    assigned_reviewer: string | null;
    geometry: object;
  }>(
    `
      SELECT
        code,
        source_group,
        status,
        severity,
        title,
        summary,
        county,
        state,
        primary_parcel_number,
        primary_parcel_code,
        primary_owner_name,
        property_name,
        analysis_name,
        tract_name,
        assigned_reviewer,
        ST_AsGeoJSON(geom, 5)::jsonb AS geometry
      FROM question_areas
      ${whereClause}
      ORDER BY code
      LIMIT ${limit}
    `,
    params,
  );

  res.json(
    featureCollection(
      result.rows.map((row) => ({
        type: "Feature",
        geometry: row.geometry as never,
        properties: {
          code: row.code,
          sourceGroup: row.source_group,
          status: row.status,
          severity: row.severity,
          title: row.title,
          summary: row.summary,
          county: row.county,
          state: row.state,
          primaryParcelNumber: row.primary_parcel_number,
          primaryParcelCode: row.primary_parcel_code,
          primaryOwnerName: row.primary_owner_name,
          propertyName: row.property_name,
          analysisName: row.analysis_name,
          tractName: row.tract_name,
          assignedReviewer: row.assigned_reviewer,
        },
      })),
    ),
  );
});

router.get("/:code", async (req, res) => {
  const result = await query<{
    id: number;
    code: string;
    source_layer: string;
    source_group: string;
    status: string;
    severity: string;
    title: string;
    summary: string;
    description: string | null;
    county: string | null;
    state: string | null;
    primary_parcel_number: string | null;
    primary_parcel_code: string | null;
    primary_owner_name: string | null;
    property_name: string | null;
    analysis_name: string | null;
    tract_name: string | null;
    assigned_reviewer: string | null;
    source_layers: unknown;
    related_parcels: unknown;
    metrics: unknown;
    geometry: object;
    centroid: object;
  }>(
    `
      SELECT
        id,
        code,
        source_layer,
        source_group,
        status,
        severity,
        title,
        summary,
        description,
        county,
        state,
        primary_parcel_number,
        primary_parcel_code,
        primary_owner_name,
        property_name,
        analysis_name,
        tract_name,
        assigned_reviewer,
        source_layers,
        related_parcels,
        metrics,
        ST_AsGeoJSON(geom, 6)::jsonb AS geometry,
        ST_AsGeoJSON(centroid, 6)::jsonb AS centroid
      FROM question_areas
      WHERE code = $1
    `,
    [req.params.code],
  );

  const row = result.rows[0];
  if (!row) {
    res.status(404).json({ message: "Question area not found." });
    return;
  }

  const [comments, documents] = await Promise.all([
    query<{
      id: number;
      body: string;
      created_at: string;
      author_name: string;
      author_role: string;
    }>(
      `
        SELECT c.id, c.body, c.created_at, u.name AS author_name, u.role AS author_role
        FROM comments c
        JOIN users u ON u.id = c.author_id
        WHERE c.question_area_id = $1
        ORDER BY c.created_at ASC
      `,
      [row.id],
    ),
    query<{
      id: number;
      original_name: string;
      mime_type: string | null;
      size_bytes: number;
      created_at: string;
    }>(
      `
        SELECT id, original_name, mime_type, size_bytes, created_at
        FROM documents
        WHERE question_area_id = $1
        ORDER BY created_at DESC
      `,
      [row.id],
    ),
  ]);

  res.json({
    id: row.id,
    code: row.code,
    sourceLayer: row.source_layer,
    sourceGroup: row.source_group,
    status: row.status,
    severity: row.severity,
    title: row.title,
    summary: row.summary,
    description: row.description,
    county: row.county,
    state: row.state,
    primaryParcelNumber: row.primary_parcel_number,
    primaryParcelCode: row.primary_parcel_code,
    primaryOwnerName: row.primary_owner_name,
    propertyName: row.property_name,
    analysisName: row.analysis_name,
    tractName: row.tract_name,
    assignedReviewer: row.assigned_reviewer,
    sourceLayers: row.source_layers,
    relatedParcels: row.related_parcels,
    metrics: row.metrics,
    geometry: row.geometry,
    centroid: row.centroid,
    comments: comments.rows.map((comment) => ({
      id: comment.id,
      body: comment.body,
      createdAt: comment.created_at,
      authorName: comment.author_name,
      authorRole: comment.author_role,
    })),
    documents: documents.rows.map((document) => ({
      id: document.id,
      originalName: document.original_name,
      mimeType: document.mime_type,
      sizeBytes: document.size_bytes,
      createdAt: document.created_at,
      downloadUrl: `/api/question-areas/documents/${document.id}/download`,
    })),
  });
});

router.patch("/:code", async (req, res) => {
  const parsed = updateSchema.safeParse(req.body);
  if (!parsed.success) {
    res.status(400).json({ message: "Invalid update payload." });
    return;
  }

  const updates = parsed.data;
  const assignments: string[] = [];
  const values: unknown[] = [];

  if (updates.status !== undefined) {
    values.push(updates.status);
    assignments.push(`status = $${values.length}`);
  }
  if (updates.summary !== undefined) {
    values.push(updates.summary);
    assignments.push(`summary = $${values.length}`);
  }
  if (updates.description !== undefined) {
    values.push(updates.description);
    assignments.push(`description = $${values.length}`);
  }
  if (updates.assignedReviewer !== undefined) {
    values.push(updates.assignedReviewer);
    assignments.push(`assigned_reviewer = $${values.length}`);
  }

  if (assignments.length === 0) {
    res.status(400).json({ message: "No updatable fields provided." });
    return;
  }

  values.push(req.params.code);

  const result = await query<{ code: string; status: string; summary: string; assigned_reviewer: string | null }>(
    `
      UPDATE question_areas
      SET ${assignments.join(", ")}, updated_at = NOW()
      WHERE code = $${values.length}
      RETURNING code, status, summary, assigned_reviewer
    `,
    values,
  );

  const row = result.rows[0];
  if (!row) {
    res.status(404).json({ message: "Question area not found." });
    return;
  }

  res.json({
    code: row.code,
    status: row.status,
    summary: row.summary,
    assignedReviewer: row.assigned_reviewer,
  });
});

router.post("/:code/comments", async (req, res) => {
  const parsed = commentSchema.safeParse(req.body);
  if (!parsed.success) {
    res.status(400).json({ message: "Comment body is required." });
    return;
  }

  const questionArea = await query<{ id: number }>(
    `SELECT id FROM question_areas WHERE code = $1`,
    [req.params.code],
  );

  const area = questionArea.rows[0];
  if (!area || !req.user) {
    res.status(404).json({ message: "Question area not found." });
    return;
  }

  const result = await query<{
    id: number;
    body: string;
    created_at: string;
  }>(
    `
      INSERT INTO comments (question_area_id, author_id, body)
      VALUES ($1, $2, $3)
      RETURNING id, body, created_at
    `,
    [area.id, req.user.id, parsed.data.body],
  );

  res.status(201).json({
    id: result.rows[0].id,
    body: result.rows[0].body,
    createdAt: result.rows[0].created_at,
    authorName: req.user.name,
    authorRole: req.user.role,
  });
});

router.post("/:code/documents", upload.single("file"), async (req, res) => {
  if (!req.file || !req.user) {
    res.status(400).json({ message: "A file is required." });
    return;
  }

  const questionArea = await query<{ id: number }>(
    `SELECT id FROM question_areas WHERE code = $1`,
    [req.params.code],
  );

  const area = questionArea.rows[0];
  if (!area) {
    await fs.unlink(req.file.path).catch(() => undefined);
    res.status(404).json({ message: "Question area not found." });
    return;
  }

  const result = await query<{
    id: number;
    original_name: string;
    mime_type: string | null;
    size_bytes: number;
    created_at: string;
  }>(
    `
      INSERT INTO documents (question_area_id, original_name, stored_name, mime_type, size_bytes, uploaded_by)
      VALUES ($1, $2, $3, $4, $5, $6)
      RETURNING id, original_name, mime_type, size_bytes, created_at
    `,
    [
      area.id,
      req.file.originalname,
      req.file.filename,
      req.file.mimetype,
      req.file.size,
      req.user.id,
    ],
  );

  res.status(201).json({
    id: result.rows[0].id,
    originalName: result.rows[0].original_name,
    mimeType: result.rows[0].mime_type,
    sizeBytes: result.rows[0].size_bytes,
    createdAt: result.rows[0].created_at,
    downloadUrl: `/api/question-areas/documents/${result.rows[0].id}/download`,
  });
});

router.get("/documents/:id/download", async (req, res) => {
  const result = await query<{
    original_name: string;
    stored_name: string;
  }>(
    `SELECT original_name, stored_name FROM documents WHERE id = $1`,
    [req.params.id],
  );

  const document = result.rows[0];
  if (!document) {
    res.status(404).json({ message: "Document not found." });
    return;
  }

  res.download(path.join(config.uploadsDir, document.stored_name), document.original_name);
});

export default router;
