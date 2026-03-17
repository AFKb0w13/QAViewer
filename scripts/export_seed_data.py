from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
GDB_PATH = ROOT / "BTG_PTV_Implementation.gdb"
OUTPUT_DIR = ROOT / "data" / "generated"


def read_layer(layer_name: str, columns: list[str] | None = None) -> gpd.GeoDataFrame:
    frame = gpd.read_file(GDB_PATH, layer=layer_name, columns=columns)

    if frame.crs is None:
        raise ValueError(
            f"Layer '{layer_name}' has no CRS defined. "
            "Cannot safely assume EPSG:4326. Fix the source data or set the CRS explicitly."
        )

    source_epsg = frame.crs.to_epsg()
    print(f"  [{layer_name}] source CRS: EPSG:{source_epsg}")

    if source_epsg != 4326:
        frame = frame.to_crs(4326)

    frame = frame.loc[frame.geometry.notna() & ~frame.geometry.is_empty].copy()

    invalid_mask = ~frame.geometry.is_valid
    invalid_count = invalid_mask.sum()
    if invalid_count > 0:
        print(f"  [{layer_name}] repairing {invalid_count} invalid geometries with make_valid")
        frame.loc[invalid_mask, "geometry"] = frame.loc[invalid_mask, "geometry"].make_valid()

    return frame


def clean_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return value
    return value


def with_bounds(frame: gpd.GeoDataFrame) -> dict[str, float]:
    min_x, min_y, max_x, max_y = frame.total_bounds
    return {
        "minLng": round(float(min_x), 6),
        "minLat": round(float(min_y), 6),
        "maxLng": round(float(max_x), 6),
        "maxLat": round(float(max_y), 6),
    }


def write_geojson(path: Path, frame: gpd.GeoDataFrame) -> None:
    feature_collection = json.loads(frame.to_json(drop_id=True))
    path.write_text(json.dumps(feature_collection, separators=(",", ":")), encoding="utf-8")


def build_question_areas() -> tuple[gpd.GeoDataFrame, dict[str, Any]]:
    """Build question areas from BTG_Spatial_Fix_Primary_Layer.

    A feature becomes a question area when its QA_Status field is not null.
    The SpatialOverlayNotes field provides the reason for the QA.
    """
    primary_layer = read_layer(
        "BTG_Spatial_Fix_Primary_Layer",
        [
            "parcelnumb",
            "County",
            "State",
            "RegridOwner",
            "PropertyName",
            "AnalysisName",
            "TractName",
            "PTVParcel",
            "QA_Status",
            "Exists_in_Mgt",
            "Exists_in_PTV",
            "GIS_Acres",
            "SpatialOverlayNotes",
            "geometry",
        ],
    )

    qa_features = primary_layer[primary_layer["QA_Status"].notna()].copy()
    print(f"  [question_areas] {len(qa_features)} parcels with active QA_Status out of {len(primary_layer)} total")

    question_area_rows: list[dict[str, Any]] = []

    for seq, (_index, row) in enumerate(qa_features.iterrows(), start=1):
        centroid = row.geometry.representative_point()
        parcel_code = clean_value(row.get("parcelnumb"))
        ptv_parcel = clean_value(row.get("PTVParcel"))
        owner_name = clean_value(row.get("RegridOwner"))
        overlay_notes = clean_value(row.get("SpatialOverlayNotes"))
        qa_status_raw = str(clean_value(row.get("QA_Status")) or "").strip().lower()
        status = qa_status_raw if qa_status_raw in ("review", "active", "resolved", "hold") else "review"
        title = (
            clean_value(row.get("PropertyName"))
            or str(parcel_code or "")
            or f"QA {seq}"
        )

        related_parcels = []
        if parcel_code:
            related_parcels.append(
                {
                    "parcelNumber": str(parcel_code),
                    "parcelCode": ptv_parcel or parcel_code,
                    "ownerName": owner_name,
                    "county": clean_value(row.get("County")),
                    "state": clean_value(row.get("State")),
                    "propertyName": clean_value(row.get("PropertyName")),
                    "analysisName": clean_value(row.get("AnalysisName")),
                    "tractName": clean_value(row.get("TractName")),
                    "source": "direct",
                }
            )

        question_area_rows.append(
            {
                "question_area_code": f"QA-{seq:04d}",
                "source_layer": "BTG_Spatial_Fix_Primary_Layer",
                "source_group": "primary",
                "status": status,
                "severity": "medium",
                "title": title,
                "summary": overlay_notes or f"QA flagged on parcel {parcel_code or 'unknown'}",
                "description": overlay_notes,
                "county": clean_value(row.get("County")),
                "state": clean_value(row.get("State")),
                "primary_parcel_number": parcel_code,
                "primary_parcel_code": ptv_parcel or parcel_code,
                "primary_owner_name": owner_name,
                "property_name": clean_value(row.get("PropertyName")),
                "analysis_name": clean_value(row.get("AnalysisName")),
                "tract_name": clean_value(row.get("TractName")),
                "assigned_reviewer": None,
                "search_keywords": " ".join(
                    filter(
                        None,
                        [
                            str(parcel_code or ""),
                            str(ptv_parcel or ""),
                            str(owner_name or ""),
                            str(clean_value(row.get("County")) or ""),
                            str(clean_value(row.get("State")) or ""),
                            str(clean_value(row.get("PropertyName")) or ""),
                            str(overlay_notes or ""),
                            str(qa_status_raw or ""),
                        ],
                    )
                ).strip(),
                "source_layers": ["BTG_Spatial_Fix_Primary_Layer"],
                "related_parcels": related_parcels,
                "metrics": {
                    "gisAcres": clean_value(row.get("GIS_Acres")),
                },
                "centroid_lat": round(float(centroid.y), 6),
                "centroid_lng": round(float(centroid.x), 6),
                "geometry": row.geometry,
            }
        )

    question_areas = gpd.GeoDataFrame(question_area_rows, geometry="geometry", crs=4326)
    question_areas = question_areas.sort_values("question_area_code").reset_index(drop=True)

    manifest = {
        "questionAreas": len(question_areas),
        "sourceBreakdown": {"primary": len(question_areas)},
        "bounds": with_bounds(question_areas),
    }
    return question_areas, manifest


def export_support_layers() -> dict[str, Any]:
    """Export the three source layers as GeoJSON for use by the frontend map.

    Layers:
      - primary_parcels: all parcels from BTG_Spatial_Fix_Primary_Layer
      - parcel_points: centroid points from BTG_Points_NoArches_12Feb26
      - management_tracts: authoritative management claims from BTG_MGMT_NoArches
    """
    layer_specs: dict[str, tuple[str, list[str]]] = {
        "primary_parcels": (
            "BTG_Spatial_Fix_Primary_Layer",
            [
                "parcelnumb",
                "County",
                "State",
                "RegridOwner",
                "PropertyName",
                "AnalysisName",
                "TractName",
                "QA_Status",
                "PTVParcel",
                "Exists_in_Mgt",
                "Exists_in_PTV",
                "GIS_Acres",
                "SpatialOverlayNotes",
                "geometry",
            ],
        ),
        "parcel_points": (
            "BTG_Points_NoArches_12Feb26",
            [
                "ParcelID",
                "ParcelCode",
                "OwnerName",
                "County",
                "State",
                "Descriptio",
                "TractName",
                "Latitude",
                "Longitude",
                "LandUseTyp",
                "geometry",
            ],
        ),
        "management_tracts": (
            "BTG_MGMT_NoArches",
            [
                "Fund",
                "PU_Number",
                "PU",
                "Tract_Numb",
                "Tract_Name",
                "Ownership",
                "Comment",
                "Book_Area",
                "geometry",
            ],
        ),
    }

    manifest: dict[str, Any] = {}

    for output_name, (layer_name, columns) in layer_specs.items():
        frame = read_layer(layer_name, columns)
        write_geojson(OUTPUT_DIR / f"{output_name}.geojson", frame)
        manifest[output_name] = {
            "sourceLayer": layer_name,
            "featureCount": len(frame),
            "bounds": with_bounds(frame),
        }

    return manifest


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    question_areas, question_manifest = build_question_areas()
    layer_manifest = export_support_layers()

    write_geojson(OUTPUT_DIR / "question_areas.geojson", question_areas)

    manifest = {
        "sourceDatabase": GDB_PATH.name,
        "questionAreas": question_manifest,
        "layers": layer_manifest,
    }
    (OUTPUT_DIR / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
