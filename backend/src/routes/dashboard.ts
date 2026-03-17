import { Router } from "express";

import { query } from "../lib/db.js";

const router = Router();

router.get("/summary", async (_req, res) => {
  const [statusCounts, severityCounts, questionAreas, comments, documents] = await Promise.all([
    query<{ status: string; count: string }>(
      `SELECT status, COUNT(*)::text AS count FROM question_areas GROUP BY status ORDER BY status`,
    ),
    query<{ severity: string; count: string }>(
      `SELECT severity, COUNT(*)::text AS count FROM question_areas GROUP BY severity ORDER BY severity`,
    ),
    query<{ count: string }>(`SELECT COUNT(*)::text AS count FROM question_areas`),
    query<{ count: string }>(`SELECT COUNT(*)::text AS count FROM comments`),
    query<{ count: string }>(`SELECT COUNT(*)::text AS count FROM documents`),
  ]);

  res.json({
    questionAreas: Number(questionAreas.rows[0]?.count ?? 0),
    comments: Number(comments.rows[0]?.count ?? 0),
    documents: Number(documents.rows[0]?.count ?? 0),
    statuses: Object.fromEntries(
      statusCounts.rows.map((row) => [row.status, Number(row.count)]),
    ),
    severities: Object.fromEntries(
      severityCounts.rows.map((row) => [row.severity, Number(row.count)]),
    ),
  });
});

router.get("/search", async (req, res) => {
  const rawQuery = String(req.query.q ?? "").trim();
  if (rawQuery.length < 2) {
    res.json({ results: [] });
    return;
  }

  const searchValue = `%${rawQuery}%`;
  const [questionAreas, parcels] = await Promise.all([
    query<{
      code: string;
      title: string;
      county: string | null;
      state: string | null;
      primary_parcel_code: string | null;
      source_group: string;
    }>(
      `
        SELECT code, title, county, state, primary_parcel_code, source_group
        FROM question_areas
        WHERE code ILIKE $1
           OR title ILIKE $1
           OR summary ILIKE $1
           OR COALESCE(primary_parcel_number, '') ILIKE $1
           OR COALESCE(primary_parcel_code, '') ILIKE $1
           OR COALESCE(primary_owner_name, '') ILIKE $1
           OR COALESCE(search_keywords, '') ILIKE $1
        ORDER BY code
        LIMIT 8
      `,
      [searchValue],
    ),
    query<{
      id: number;
      parcel_number: string | null;
      owner_name: string | null;
      county: string | null;
      state: string | null;
    }>(
      `
        SELECT id, parcel_number, owner_name, county, state
        FROM parcel_features
        WHERE COALESCE(parcel_number, '') ILIKE $1
           OR COALESCE(owner_name, '') ILIKE $1
           OR COALESCE(county, '') ILIKE $1
           OR COALESCE(state, '') ILIKE $1
        ORDER BY parcel_number NULLS LAST
        LIMIT 8
      `,
      [searchValue],
    ),
  ]);

  res.json({
    results: [
      ...questionAreas.rows.map((row) => ({
        type: "question_area",
        id: row.code,
        label: row.title,
        subtitle: [row.primary_parcel_code, row.county, row.state].filter(Boolean).join(" | "),
        sourceGroup: row.source_group,
      })),
      ...parcels.rows.map((row) => ({
        type: "parcel",
        id: String(row.id),
        label: row.parcel_number ?? "Unnamed parcel",
        subtitle: [row.owner_name, row.county, row.state].filter(Boolean).join(" | "),
      })),
    ].slice(0, 10),
  });
});

export default router;
