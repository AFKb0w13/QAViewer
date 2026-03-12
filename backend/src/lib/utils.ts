import fs from "node:fs/promises";
import path from "node:path";

import type { Feature, FeatureCollection, GeoJsonProperties, Geometry } from "geojson";

import { config } from "../config.js";

export async function loadFeatureCollection(
  filename: string,
): Promise<FeatureCollection<Geometry, GeoJsonProperties>> {
  const fullPath = path.join(config.seedDir, filename);
  const raw = await fs.readFile(fullPath, "utf-8");
  return JSON.parse(raw) as FeatureCollection<Geometry, GeoJsonProperties>;
}

export function parseBoolean(value: unknown): boolean | null {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === "boolean") {
    return value;
  }
  const normalized = String(value).trim().toLowerCase();
  if (normalized === "yes" || normalized === "true") {
    return true;
  }
  if (normalized === "no" || normalized === "false") {
    return false;
  }
  return null;
}

export function asGeoJsonString(feature: Feature<Geometry, GeoJsonProperties>): string {
  return JSON.stringify(feature.geometry);
}

export function parseBbox(value?: string): [number, number, number, number] | null {
  if (!value) {
    return null;
  }
  const parts = value.split(",").map((part) => Number(part.trim()));
  if (parts.length !== 4 || parts.some((part) => Number.isNaN(part))) {
    return null;
  }
  const [west, south, east, north] = parts;
  return [west, south, east, north];
}

export function featureCollection(features: Feature[]): FeatureCollection {
  return {
    type: "FeatureCollection",
    features,
  };
}
