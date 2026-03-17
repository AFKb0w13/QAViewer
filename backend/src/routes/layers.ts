import { Router } from "express";

import { query } from "../lib/db.js";
import { featureCollection, parseBbox } from "../lib/utils.js";

const router = Router();

const layerConfig = {
  primary_parcels: {
    table: "parcel_features",
    geometryExpression: "ST_AsGeoJSON(geom, 5)::jsonb",
    wherePrefix: "",
    orderBy: "id",
    limit: 6000,
  },
  parcel_points: {
    table: "parcel_points",
    geometryExpression: "ST_AsGeoJSON(geom, 5)::jsonb",
    wherePrefix: "",
    orderBy: "id",
    limit: 6000,
  },
  management_tracts: {
    table: "management_tracts",
    geometryExpression: "ST_AsGeoJSON(geom, 5)::jsonb",
    wherePrefix: "",
    orderBy: "id",
    limit: 3000,
  },
} as const;

router.get("/:layerKey", async (req, res) => {
  const layerKey = String(req.params.layerKey) as keyof typeof layerConfig;
  const layer = layerConfig[layerKey];

  if (!layer) {
    res.status(404).json({ message: "Unknown layer." });
    return;
  }

  const clauses: string[] = [];
  const params: number[] = [];

  if (layer.wherePrefix) {
    clauses.push(layer.wherePrefix);
  }

  const bbox = parseBbox(String(req.query.bbox ?? ""));
  if (bbox) {
    const [west, south, east, north] = bbox;
    params.push(west, south, east, north);
    clauses.push(
      `geom && ST_MakeEnvelope($${params.length - 3}, $${params.length - 2}, $${params.length - 1}, $${params.length}, 4326)`,
    );
  }

  const whereClause = clauses.length > 0 ? `WHERE ${clauses.join(" AND ")}` : "";
  const result = await query<{ id: number; properties: Record<string, unknown>; geometry: object }>(
    `
      SELECT id, raw_properties AS properties, ${layer.geometryExpression} AS geometry
      FROM ${layer.table}
      ${whereClause}
      ORDER BY ${layer.orderBy}
      LIMIT ${layer.limit}
    `,
    params,
  );

  res.json(
    featureCollection(
      result.rows.map((row) => ({
        type: "Feature",
        geometry: row.geometry as never,
        properties: {
          id: row.id,
          ...row.properties,
        },
      })),
    ),
  );
});

export default router;
