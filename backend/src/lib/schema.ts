import type { PoolClient } from "pg";

export async function ensureSchema(client: PoolClient): Promise<void> {
  await client.query(`CREATE EXTENSION IF NOT EXISTS postgis`);
  await client.query(`CREATE EXTENSION IF NOT EXISTS pg_trgm`);

  await client.query(`
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT NOT NULL UNIQUE,
      password_hash TEXT NOT NULL,
      role TEXT NOT NULL CHECK (role IN ('admin', 'reviewer', 'client')),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS question_areas (
      id SERIAL PRIMARY KEY,
      code TEXT NOT NULL UNIQUE,
      source_layer TEXT NOT NULL,
      source_group TEXT NOT NULL,
      status TEXT NOT NULL CHECK (status IN ('review', 'active', 'resolved', 'hold')),
      severity TEXT NOT NULL CHECK (severity IN ('high', 'medium', 'low')),
      title TEXT NOT NULL,
      summary TEXT NOT NULL,
      description TEXT,
      county TEXT,
      state TEXT,
      primary_parcel_number TEXT,
      primary_parcel_code TEXT,
      primary_owner_name TEXT,
      property_name TEXT,
      analysis_name TEXT,
      tract_name TEXT,
      assigned_reviewer TEXT,
      search_keywords TEXT,
      source_layers JSONB NOT NULL DEFAULT '[]'::jsonb,
      related_parcels JSONB NOT NULL DEFAULT '[]'::jsonb,
      metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
      geom geometry(MultiPolygon, 4326) NOT NULL,
      centroid geometry(Point, 4326) NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS parcel_features (
      id SERIAL PRIMARY KEY,
      parcel_number TEXT,
      county TEXT,
      state TEXT,
      owner_name TEXT,
      property_name TEXT,
      analysis_name TEXT,
      tract_name TEXT,
      qa_status TEXT,
      ptv_parcel TEXT,
      exists_in_mgt BOOLEAN,
      exists_in_ptv BOOLEAN,
      gis_acres DOUBLE PRECISION,
      raw_properties JSONB NOT NULL DEFAULT '{}'::jsonb,
      geom geometry(MultiPolygon, 4326) NOT NULL
    )
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS parcel_points (
      id SERIAL PRIMARY KEY,
      parcel_id INTEGER,
      parcel_code TEXT,
      owner_name TEXT,
      county TEXT,
      state TEXT,
      description TEXT,
      tract_name TEXT,
      land_use_type TEXT,
      latitude DOUBLE PRECISION,
      longitude DOUBLE PRECISION,
      raw_properties JSONB NOT NULL DEFAULT '{}'::jsonb,
      geom geometry(Point, 4326) NOT NULL
    )
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS management_tracts (
      id SERIAL PRIMARY KEY,
      fund TEXT,
      pu_number DOUBLE PRECISION,
      pu TEXT,
      tract_number TEXT,
      tract_name TEXT,
      ownership TEXT,
      comment TEXT,
      book_area DOUBLE PRECISION,
      raw_properties JSONB NOT NULL DEFAULT '{}'::jsonb,
      geom geometry(MultiPolygon, 4326) NOT NULL
    )
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS county_boundaries (
      id SERIAL PRIMARY KEY,
      layer_group TEXT NOT NULL,
      name TEXT,
      state_abbr TEXT,
      county_state TEXT,
      billed_acreage TEXT,
      gis_acres DOUBLE PRECISION,
      raw_properties JSONB NOT NULL DEFAULT '{}'::jsonb,
      geom geometry(MultiPolygon, 4326) NOT NULL
    )
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS comments (
      id SERIAL PRIMARY KEY,
      question_area_id INTEGER NOT NULL REFERENCES question_areas(id) ON DELETE CASCADE,
      author_id INTEGER NOT NULL REFERENCES users(id),
      body TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS documents (
      id SERIAL PRIMARY KEY,
      question_area_id INTEGER NOT NULL REFERENCES question_areas(id) ON DELETE CASCADE,
      original_name TEXT NOT NULL,
      stored_name TEXT NOT NULL,
      mime_type TEXT,
      size_bytes INTEGER NOT NULL,
      uploaded_by INTEGER NOT NULL REFERENCES users(id),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS question_areas_geom_idx ON question_areas USING GIST (geom);
    CREATE INDEX IF NOT EXISTS question_areas_centroid_idx ON question_areas USING GIST (centroid);
    CREATE INDEX IF NOT EXISTS parcel_features_geom_idx ON parcel_features USING GIST (geom);
    CREATE INDEX IF NOT EXISTS parcel_points_geom_idx ON parcel_points USING GIST (geom);
    CREATE INDEX IF NOT EXISTS management_tracts_geom_idx ON management_tracts USING GIST (geom);
    CREATE INDEX IF NOT EXISTS county_boundaries_geom_idx ON county_boundaries USING GIST (geom);

    CREATE INDEX IF NOT EXISTS question_areas_status_idx ON question_areas (status);
    CREATE INDEX IF NOT EXISTS question_areas_severity_idx ON question_areas (severity);

    CREATE INDEX IF NOT EXISTS comments_question_area_id_idx ON comments (question_area_id);
    CREATE INDEX IF NOT EXISTS documents_question_area_id_idx ON documents (question_area_id);

    CREATE INDEX IF NOT EXISTS question_areas_code_trgm_idx ON question_areas USING GIN (code gin_trgm_ops);
    CREATE INDEX IF NOT EXISTS question_areas_title_trgm_idx ON question_areas USING GIN (title gin_trgm_ops);
    CREATE INDEX IF NOT EXISTS question_areas_search_keywords_trgm_idx ON question_areas USING GIN (search_keywords gin_trgm_ops);
  `);
}
