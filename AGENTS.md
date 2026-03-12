# QAViewer Agent Guide

## Project purpose

QAViewer is a Docker-first GIS review application built from the PRD in `question_area_review_web_app_prd.md` and the source geodatabase in `BTG_PTV_Implementation.gdb`.

The app is centered on question areas derived from mismatch layers in the geodatabase:

- `BTG_Spatial_Fix_Primary_Erase`
- `BTG_Spatial_Fix_Comparison_Erase`

Supporting GIS context comes from:

- `BTG_Spatial_Fix_Primary_Layer`
- `BTG_Points_NoArches_12Feb26`
- `BTG_MGMT_NoArches`
- `TaxParcels_CountySplits_Combined`
- `Management_CountySplits_Combined`

## Repo layout

- `scripts/export_seed_data.py`: reads the `.gdb` and writes normalized seed assets.
- `data/generated/`: generated GeoJSON + manifest used by the backend seed loader.
- `backend/`: Express/TypeScript API, PostGIS schema bootstrap, auth, comments, documents.
- `frontend/`: React/Vite/Leaflet reviewer UI.
- `backend/uploads/`: uploaded documents stored by the API.
- `docker-compose.yml`: local dev stack for PostGIS, API, and frontend.

## Source of truth

- The file geodatabase is the source of truth for GIS data.
- Do not hand-edit files in `data/generated/` unless there is a very specific reason.
- If GIS-derived data changes, regenerate it with:

```bash
.venv/bin/python scripts/export_seed_data.py
```

## Key commands

Start the full stack:

```bash
docker compose up --build
```

Backend dev/build:

```bash
cd backend
npm install
npm run dev
npm run build
```

Frontend dev/build:

```bash
cd frontend
npm install
npm run dev
npm run build
```

## Runtime defaults

- Frontend: `http://localhost:5173`
- API: `http://localhost:3001/api`
- PostGIS: `localhost:5432`

Demo users seeded by the backend:

- `admin@qaviewer.local` / `admin123!`
- `reviewer@qaviewer.local` / `review123!`
- `client@qaviewer.local` / `client123!`

## Important implementation notes

- The backend seeds the database automatically on first start if `question_areas` is empty.
- Geometry import assumes EPSG:4326 in the generated seed files.
- `primary_parcels.geojson` intentionally excludes null/empty geometries during export.
- Document files are stored on disk in `backend/uploads`; metadata is stored in Postgres.
- The frontend is designed around an authenticated single-screen review workspace:
  map, search/filter rail, and question-area details rail.

## When changing the app

- If you change the GIS normalization logic, update `scripts/export_seed_data.py` first.
- If you change persisted backend fields, update both the PostGIS schema and the seed loader.
- If you change question-area payloads, update both backend route responses and frontend types.
- Prefer keeping the browser decoupled from PostGIS; all data access should stay behind the API.

## Verification

Useful checks after changes:

```bash
cd backend && npm run build
cd frontend && npm run build
curl http://localhost:3001/api/health
curl -I http://localhost:5173
```
