import fs from "node:fs/promises";

import type { Feature, GeoJsonProperties, Geometry } from "geojson";
import type { PoolClient } from "pg";

import { config } from "../config.js";
import { hashPassword } from "./auth.js";
import { loadFeatureCollection, parseBoolean } from "./utils.js";

const DEMO_USERS = [
  {
    name: "Avery Counsel",
    email: "admin@qaviewer.local",
    password: "admin123!",
    role: "admin",
  },
  {
    name: "Morgan Review",
    email: "reviewer@qaviewer.local",
    password: "review123!",
    role: "reviewer",
  },
  {
    name: "Cameron Client",
    email: "client@qaviewer.local",
    password: "client123!",
    role: "client",
  },
];

export async function ensureSeedData(client: PoolClient): Promise<void> {
  await fs.mkdir(config.uploadsDir, { recursive: true });

  await seedUsers(client);

  const existingCount = await client.query<{ count: string }>(
    `SELECT COUNT(*)::text AS count FROM question_areas`,
  );

  if (Number(existingCount.rows[0]?.count ?? 0) > 0) {
    return;
  }

  const questionAreas = await loadFeatureCollection("question_areas.geojson");
  const parcelFeatures = await loadFeatureCollection("primary_parcels.geojson");
  const parcelPoints = await loadFeatureCollection("parcel_points.geojson");
  const managementTracts = await loadFeatureCollection("management_tracts.geojson");
  const taxCounties = await loadFeatureCollection("tax_counties.geojson");
  const managementCounties = await loadFeatureCollection("management_counties.geojson");

  for (const feature of questionAreas.features) {
    await insertQuestionArea(client, feature);
  }

  for (const feature of parcelFeatures.features) {
    await insertParcelFeature(client, feature);
  }

  for (const feature of parcelPoints.features) {
    await insertParcelPoint(client, feature);
  }

  for (const feature of managementTracts.features) {
    await insertManagementTract(client, feature);
  }

  for (const feature of taxCounties.features) {
    await insertCountyBoundary(client, feature, "tax_counties");
  }

  for (const feature of managementCounties.features) {
    await insertCountyBoundary(client, feature, "management_counties");
  }

  await seedComments(client);
}

async function seedUsers(client: PoolClient): Promise<void> {
  for (const user of DEMO_USERS) {
    const passwordHash = await hashPassword(user.password);
    await client.query(
      `
        INSERT INTO users (name, email, password_hash, role)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (email) DO UPDATE
        SET name = EXCLUDED.name,
            password_hash = EXCLUDED.password_hash,
            role = EXCLUDED.role
      `,
      [user.name, user.email, passwordHash, user.role],
    );
  }
}

async function insertQuestionArea(
  client: PoolClient,
  feature: Feature<Geometry, GeoJsonProperties>,
): Promise<void> {
  if (!feature.geometry) {
    return;
  }
  const properties = feature.properties ?? {};
  await client.query(
    `
      INSERT INTO question_areas (
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
        search_keywords,
        source_layers,
        related_parcels,
        metrics,
        geom,
        centroid
      )
      VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
        $17, $18, $19::jsonb, $20::jsonb, $21::jsonb,
        ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON($22), 4326)),
        ST_SetSRID(ST_MakePoint($23, $24), 4326)
      )
    `,
    [
      properties.question_area_code,
      properties.source_layer,
      properties.source_group,
      properties.status,
      properties.severity,
      properties.title,
      properties.summary,
      properties.description,
      properties.county,
      properties.state,
      properties.primary_parcel_number,
      properties.primary_parcel_code,
      properties.primary_owner_name,
      properties.property_name,
      properties.analysis_name,
      properties.tract_name,
      properties.assigned_reviewer,
      properties.search_keywords,
      JSON.stringify(properties.source_layers ?? []),
      JSON.stringify(properties.related_parcels ?? []),
      JSON.stringify(properties.metrics ?? {}),
      JSON.stringify(feature.geometry),
      properties.centroid_lng,
      properties.centroid_lat,
    ],
  );
}

async function insertParcelFeature(
  client: PoolClient,
  feature: Feature<Geometry, GeoJsonProperties>,
): Promise<void> {
  if (!feature.geometry) {
    return;
  }
  const properties = feature.properties ?? {};
  await client.query(
    `
      INSERT INTO parcel_features (
        parcel_number,
        county,
        state,
        owner_name,
        property_name,
        analysis_name,
        tract_name,
        qa_status,
        ptv_parcel,
        exists_in_mgt,
        exists_in_ptv,
        gis_acres,
        raw_properties,
        geom
      )
      VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb,
        ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON($14), 4326))
      )
    `,
    [
      properties.parcelnumb,
      properties.County,
      properties.State,
      properties.RegridOwner,
      properties.PropertyName,
      properties.AnalysisName,
      properties.TractName,
      properties.QA_Status,
      properties.PTVParcel,
      parseBoolean(properties.Exists_in_Mgt),
      parseBoolean(properties.Exists_in_PTV),
      properties.GIS_Acres,
      JSON.stringify(properties),
      JSON.stringify(feature.geometry),
    ],
  );
}

async function insertParcelPoint(
  client: PoolClient,
  feature: Feature<Geometry, GeoJsonProperties>,
): Promise<void> {
  if (!feature.geometry) {
    return;
  }
  const properties = feature.properties ?? {};
  await client.query(
    `
      INSERT INTO parcel_points (
        parcel_id,
        parcel_code,
        owner_name,
        county,
        state,
        description,
        tract_name,
        land_use_type,
        latitude,
        longitude,
        raw_properties,
        geom
      )
      VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb,
        ST_SetSRID(ST_GeomFromGeoJSON($12), 4326)
      )
    `,
    [
      properties.ParcelID,
      properties.ParcelCode,
      properties.OwnerName,
      properties.County,
      properties.State,
      properties.Descriptio,
      properties.TractName,
      properties.LandUseTyp,
      properties.Latitude,
      properties.Longitude,
      JSON.stringify(properties),
      JSON.stringify(feature.geometry),
    ],
  );
}

async function insertManagementTract(
  client: PoolClient,
  feature: Feature<Geometry, GeoJsonProperties>,
): Promise<void> {
  if (!feature.geometry) {
    return;
  }
  const properties = feature.properties ?? {};
  await client.query(
    `
      INSERT INTO management_tracts (
        fund,
        pu_number,
        pu,
        tract_number,
        tract_name,
        ownership,
        comment,
        book_area,
        raw_properties,
        geom
      )
      VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb,
        ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON($10), 4326))
      )
    `,
    [
      properties.Fund,
      properties.PU_Number,
      properties.PU,
      properties.Tract_Numb,
      properties.Tract_Name,
      properties.Ownership,
      properties.Comment,
      properties.Book_Area,
      JSON.stringify(properties),
      JSON.stringify(feature.geometry),
    ],
  );
}

async function insertCountyBoundary(
  client: PoolClient,
  feature: Feature<Geometry, GeoJsonProperties>,
  layerGroup: "tax_counties" | "management_counties",
): Promise<void> {
  if (!feature.geometry) {
    return;
  }
  const properties = feature.properties ?? {};
  await client.query(
    `
      INSERT INTO county_boundaries (
        layer_group,
        name,
        state_abbr,
        county_state,
        billed_acreage,
        gis_acres,
        raw_properties,
        geom
      )
      VALUES (
        $1, $2, $3, $4, $5, $6, $7::jsonb,
        ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON($8), 4326))
      )
    `,
    [
      layerGroup,
      properties.NAME,
      properties.State_Abbr,
      properties.County_State,
      properties.Billed_Acreage,
      properties.GIS_Acres,
      JSON.stringify(properties),
      JSON.stringify(feature.geometry),
    ],
  );
}

async function seedComments(client: PoolClient): Promise<void> {
  const existingComments = await client.query<{ count: string }>(
    `SELECT COUNT(*)::text AS count FROM comments`,
  );

  if (Number(existingComments.rows[0]?.count ?? 0) > 0) {
    return;
  }

  const { rows: users } = await client.query<{ id: number; email: string }>(
    `SELECT id, email FROM users WHERE email IN ($1, $2) ORDER BY email`,
    ["admin@qaviewer.local", "reviewer@qaviewer.local"],
  );
  const userMap = new Map(users.map((user) => [user.email, user.id]));

  const { rows: questionAreas } = await client.query<{ id: number; code: string; title: string }>(
    `SELECT id, code, title FROM question_areas ORDER BY code ASC LIMIT 3`,
  );

  const cannedComments = [
    "Initial review queued. Please verify the parcel geometry against the management source layer.",
    "Ownership reference looks consistent, but the mapped overlap needs a second pass before sign-off.",
    "Document request opened for legal backup. Attach county parcel support when available.",
  ];

  for (const [index, area] of questionAreas.entries()) {
    const authorId =
      index % 2 === 0
        ? userMap.get("reviewer@qaviewer.local")
        : userMap.get("admin@qaviewer.local");

    if (!authorId) {
      continue;
    }

    await client.query(
      `INSERT INTO comments (question_area_id, author_id, body) VALUES ($1, $2, $3)`,
      [area.id, authorId, cannedComments[index] ?? "Review note added during seed load."],
    );
  }
}
