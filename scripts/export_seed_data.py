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


def dominant_string(values: list[Any]) -> str | None:
    normalized = [str(value).strip() for value in values if clean_value(value)]
    if not normalized:
        return None
    series = pd.Series(normalized)
    return str(series.value_counts().idxmax())


def build_related_parcels(feature: pd.Series, primary_matches: gpd.GeoDataFrame) -> list[dict[str, Any]]:
    parcels: list[dict[str, Any]] = []

    direct_parcel = clean_value(feature.get("parcelnumb"))
    if direct_parcel:
        parcels.append(
            {
                "parcelNumber": str(direct_parcel),
                "parcelCode": clean_value(feature.get("PTVParcel")) or clean_value(feature.get("parcelnumb")),
                "ownerName": clean_value(feature.get("RegridOwner")),
                "county": clean_value(feature.get("County")),
                "state": clean_value(feature.get("State")),
                "propertyName": clean_value(feature.get("PropertyName")),
                "analysisName": clean_value(feature.get("AnalysisName")),
                "tractName": clean_value(feature.get("TractName")),
                "source": "direct",
            }
        )

    if primary_matches.empty:
        return dedupe_parcels(parcels)

    for _, row in primary_matches.iterrows():
        parcels.append(
            {
                "parcelNumber": clean_value(row.get("parcelnumb")),
                "parcelCode": clean_value(row.get("PTVParcel")) or clean_value(row.get("parcelnumb")),
                "ownerName": clean_value(row.get("RegridOwner")),
                "county": clean_value(row.get("County")),
                "state": clean_value(row.get("State")),
                "propertyName": clean_value(row.get("PropertyName")),
                "analysisName": clean_value(row.get("AnalysisName")),
                "tractName": clean_value(row.get("TractName")),
                "source": "spatial_join",
            }
        )

    return dedupe_parcels(parcels)


def dedupe_parcels(parcels: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[Any, ...]] = set()
    unique: list[dict[str, Any]] = []

    for parcel in parcels:
        key = (
            parcel.get("parcelNumber"),
            parcel.get("parcelCode"),
            parcel.get("ownerName"),
            parcel.get("county"),
            parcel.get("state"),
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(parcel)

    return unique


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
            "geometry",
        ],
    )
    primary_erase = read_layer(
        "BTG_Spatial_Fix_Primary_Erase",
        [
            "parcelnumb",
            "County",
            "State",
            "RegridOwner",
            "Legal_Concat",
            "PropertyName",
            "AnalysisName",
            "TractName",
            "QA_Status",
            "GIS_Acres",
            "SpatialOverlayNotes",
            "PTVParcel",
            "EraseAcres",
            "OriginalAcres",
            "OverlapAcres",
            "OverlapPct",
            "Class",
            "geometry",
        ],
    )
    comparison_erase = read_layer(
        "BTG_Spatial_Fix_Comparison_Erase",
        [
            "Fund",
            "PU_Number",
            "PU",
            "Tract_Numb",
            "Tract_Name",
            "Ownership",
            "Comment",
            "Book_Area",
            "EraseAcres",
            "OriginalAcres",
            "OverlapAcres",
            "OverlapPct",
            "Class",
            "geometry",
        ],
    )

    primary_join = gpd.sjoin(
        primary_erase[["geometry"]],
        primary_layer,
        predicate="intersects",
        how="left",
    )
    comparison_join = gpd.sjoin(
        comparison_erase[["geometry"]],
        primary_layer,
        predicate="intersects",
        how="left",
    )

    question_area_rows: list[dict[str, Any]] = []

    for index, row in primary_erase.iterrows():
        matches = primary_join.loc[primary_join.index == index].dropna(subset=["parcelnumb"], how="all")
        related_parcels = build_related_parcels(row, matches)
        centroid = row.geometry.representative_point()
        title = clean_value(row.get("Legal_Concat")) or clean_value(row.get("PropertyName")) or clean_value(row.get("parcelnumb"))
        owner_name = clean_value(row.get("RegridOwner")) or dominant_string([parcel.get("ownerName") for parcel in related_parcels])

        question_area_rows.append(
            {
                "question_area_code": f"QA-P-{index + 1:04d}",
                "source_layer": "BTG_Spatial_Fix_Primary_Erase",
                "source_group": "primary_gap",
                "status": "review",
                "severity": classify_severity(clean_value(row.get("OverlapPct")), clean_value(row.get("EraseAcres"))),
                "title": title,
                "summary": f"Primary-layer spatial mismatch for parcel {clean_value(row.get('parcelnumb')) or 'unknown parcel'}",
                "description": clean_value(row.get("SpatialOverlayNotes")) or f"Overlap review created from the primary erase layer ({clean_value(row.get('Class'))}).",
                "county": clean_value(row.get("County")),
                "state": clean_value(row.get("State")),
                "primary_parcel_number": clean_value(row.get("parcelnumb")),
                "primary_parcel_code": clean_value(row.get("PTVParcel")) or clean_value(row.get("parcelnumb")),
                "primary_owner_name": owner_name,
                "property_name": clean_value(row.get("PropertyName")),
                "analysis_name": clean_value(row.get("AnalysisName")),
                "tract_name": clean_value(row.get("TractName")),
                "assigned_reviewer": None,
                "search_keywords": " ".join(
                    filter(
                        None,
                        [
                            str(clean_value(row.get("parcelnumb")) or ""),
                            str(clean_value(row.get("PTVParcel")) or ""),
                            str(owner_name or ""),
                            str(clean_value(row.get("County")) or ""),
                            str(clean_value(row.get("State")) or ""),
                            str(clean_value(row.get("Legal_Concat")) or ""),
                        ],
                    )
                ).strip(),
                "source_layers": [
                    "BTG_Spatial_Fix_Primary_Erase",
                    "BTG_Spatial_Fix_Primary_Layer",
                    "BTG_Points_NoArches_12Feb26",
                ],
                "related_parcels": related_parcels,
                "metrics": {
                    "gisAcres": clean_value(row.get("GIS_Acres")),
                    "eraseAcres": clean_value(row.get("EraseAcres")),
                    "originalAcres": clean_value(row.get("OriginalAcres")),
                    "overlapAcres": clean_value(row.get("OverlapAcres")),
                    "overlapPct": clean_value(row.get("OverlapPct")),
                },
                "centroid_lat": round(float(centroid.y), 6),
                "centroid_lng": round(float(centroid.x), 6),
                "geometry": row.geometry,
            }
        )

    for index, row in comparison_erase.iterrows():
        matches = comparison_join.loc[comparison_join.index == index].dropna(subset=["parcelnumb"], how="all")
        related_parcels = build_related_parcels(row, matches)
        centroid = row.geometry.representative_point()
        county = dominant_string(matches["County"].tolist()) if not matches.empty else None
        state = dominant_string(matches["State"].tolist()) if not matches.empty else None
        primary_parcel = related_parcels[0]["parcelNumber"] if related_parcels else None
        parcel_code = related_parcels[0]["parcelCode"] if related_parcels else None
        owner_name = related_parcels[0]["ownerName"] if related_parcels else clean_value(row.get("Ownership"))
        property_name = dominant_string(matches["PropertyName"].tolist()) if not matches.empty else clean_value(row.get("Fund"))
        analysis_name = dominant_string(matches["AnalysisName"].tolist()) if not matches.empty else clean_value(row.get("PU"))

        question_area_rows.append(
            {
                "question_area_code": f"QA-C-{index + 1:04d}",
                "source_layer": "BTG_Spatial_Fix_Comparison_Erase",
                "source_group": "comparison_gap",
                "status": "review",
                "severity": classify_severity(clean_value(row.get("OverlapPct")), clean_value(row.get("EraseAcres"))),
                "title": clean_value(row.get("Tract_Name")) or clean_value(row.get("Fund")) or f"Comparison gap {index + 1}",
                "summary": f"Comparison-layer gap for {clean_value(row.get('Tract_Name')) or clean_value(row.get('Fund'))}",
                "description": clean_value(row.get("Comment")) or f"Comparison erase area flagged as {clean_value(row.get('Class'))}.",
                "county": county,
                "state": state,
                "primary_parcel_number": primary_parcel,
                "primary_parcel_code": parcel_code,
                "primary_owner_name": owner_name,
                "property_name": property_name,
                "analysis_name": analysis_name,
                "tract_name": clean_value(row.get("Tract_Name")),
                "assigned_reviewer": None,
                "search_keywords": " ".join(
                    filter(
                        None,
                        [
                            str(clean_value(row.get("Fund")) or ""),
                            str(clean_value(row.get("PU")) or ""),
                            str(clean_value(row.get("Tract_Name")) or ""),
                            str(primary_parcel or ""),
                            str(parcel_code or ""),
                            str(owner_name or ""),
                            str(county or ""),
                            str(state or ""),
                        ],
                    )
                ).strip(),
                "source_layers": [
                    "BTG_Spatial_Fix_Comparison_Erase",
                    "BTG_MGMT_NoArches",
                    "BTG_Spatial_Fix_Primary_Layer",
                ],
                "related_parcels": related_parcels,
                "metrics": {
                    "bookArea": clean_value(row.get("Book_Area")),
                    "eraseAcres": clean_value(row.get("EraseAcres")),
                    "originalAcres": clean_value(row.get("OriginalAcres")),
                    "overlapAcres": clean_value(row.get("OverlapAcres")),
                    "overlapPct": clean_value(row.get("OverlapPct")),
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
        "sourceBreakdown": question_areas["source_group"].value_counts().to_dict(),
        "bounds": with_bounds(question_areas),
    }
    return question_areas, manifest


def classify_severity(overlap_pct: Any, erase_acres: Any) -> str:
    pct = clean_value(overlap_pct) or 0
    acres = clean_value(erase_acres) or 0
    if pct >= 90 or acres >= 100:
        return "high"
    if pct >= 75 or acres >= 25:
        return "medium"
    return "low"


def export_support_layers() -> dict[str, Any]:
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
        "tax_counties": (
            "TaxParcels_CountySplits_Combined",
            [
                "NAME",
                "State_Abbr",
                "County_State",
                "Billed_Acreage",
                "GIS_Acres",
                "geometry",
            ],
        ),
        "management_counties": (
            "Management_CountySplits_Combined",
            [
                "NAME",
                "State_Abbr",
                "County_State",
                "Billed_Acreage",
                "GIS_Acres",
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
