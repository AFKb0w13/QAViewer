# QAViewer

QAViewer is a Docker-first GIS review app built from the provided PRD and geodatabase. The app centers on `157` question areas derived from the mismatch layers in `BTG_PTV_Implementation.gdb`, with supporting parcel, point, management, and county layers exported into normalized seed assets.

## Stack

- Frontend: React + Vite + Leaflet
- Backend: Express + TypeScript
- Database: PostgreSQL + PostGIS
- Seed pipeline: Python (`geopandas` / `pyogrio`) export from the supplied `.gdb`

## Project structure

- `scripts/export_seed_data.py`: reads the file geodatabase and writes normalized GeoJSON seed assets into `data/generated`
- `backend/`: API, auth, PostGIS schema, seed loader, comments, and document upload/download endpoints
- `frontend/`: Leaflet reviewer workspace with search, layer toggles, details, comments, and document management
- `data/generated/`: exported GIS seed layers used by the backend on first start

## Run with Docker

1. Copy `.env.example` to `.env` if you want to override the defaults.
2. Start the stack:

```bash
docker compose up --build
```

3. Open:

- Web app: `http://localhost:5173`
- API health: `http://localhost:3001/api/health`

## Demo credentials

- `admin@qaviewer.local` / `admin123!`
- `reviewer@qaviewer.local` / `review123!`
- `client@qaviewer.local` / `client123!`

## Local development without Docker

Backend:

```bash
cd backend
npm install
npm run dev
```

Frontend:

```bash
cd frontend
npm install
npm run dev
```

You still need a PostGIS database available at `DATABASE_URL`.

## Regenerate GIS seed data

The seed assets were generated from the provided geodatabase. To rebuild them:

```bash
.venv/bin/python scripts/export_seed_data.py
```

## Implemented MVP scope

- Basic login/access control
- Leaflet map viewer
- Question area layer and supporting GIS overlays
- Search and selection
- Details/review panel
- Comments
- Document upload/list/download
- Basic status tracking

## Notes

- The backend imports the seed layers automatically on first start if the database is empty.
- Documents are stored in `backend/uploads`.
- The question-area seed is built from `BTG_Spatial_Fix_Primary_Erase` and `BTG_Spatial_Fix_Comparison_Erase`, with parcel context attached from the primary parcel layer.
