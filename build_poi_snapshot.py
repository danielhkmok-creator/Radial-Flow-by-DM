#!/usr/bin/env python3
# ---------------------------------------------------------------------------
# Radial Flow by DM - one-off Hong Kong POI snapshot builder.
# Run on your own PC:   python build_poi_snapshot.py
# Output:               data/hk_pois.csv.gz   (+ data/hk_pois_meta.json)
# Safety rule:          if the finished file is larger than 200 MB it is deleted and NOT produced.
# Concurrency model:    concurrent.futures.ThreadPoolExecutor only (2 polite workers).
# Data: (c) OpenStreetMap contributors, ODbL.
# ---------------------------------------------------------------------------
from __future__ import annotations

import argparse
import csv
import gzip
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

# Folder of this script (falls back to the current folder inside Jupyter, where __file__ does not exist).
SCRIPT_DIR = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

# Load the engine straight from its file path (bypasses any same-named folder or stale import in Jupyter).
import importlib.util

ENGINE_FILE = SCRIPT_DIR / "radial_flow_by_dm.py"
if not ENGINE_FILE.is_file():
    raise ImportError(
        f"radial_flow_by_dm.py was not found in {SCRIPT_DIR}. Save the engine file with exactly that name "
        f"in this folder (it must be a .py file, not only code pasted into a notebook cell). "
        f"Files here: {sorted(path.name for path in SCRIPT_DIR.iterdir())[:30]}") from None
_engine_spec = importlib.util.spec_from_file_location("radial_flow_by_dm", ENGINE_FILE)
_engine = importlib.util.module_from_spec(_engine_spec)
sys.modules["radial_flow_by_dm"] = _engine          # register first: dataclasses need it
_engine_spec.loader.exec_module(_engine)

APP_NAME = _engine.APP_NAME
HK_TZ = _engine.HK_TZ
OVERPASS_ENDPOINTS = _engine.OVERPASS_ENDPOINTS
HttpClient = _engine.HttpClient
classify_destination = _engine.classify_destination
osm_name = _engine.osm_name
safe_float = _engine.safe_float

# Hong Kong bounding box (south, west, north, east) and tile size in degrees (~5 km).
HK_BBOX = (22.13, 113.82, 22.57, 114.45)
TILE_DEGREES = 0.05
MAX_SPLIT_DEPTH = 1              # a failing tile is split into 4 smaller tiles once
TILE_TIME_LIMIT = 480.0          # seconds: a tile (including its splits) is abandoned after this, never stuck for hours
REQUEST_READ_TIMEOUT = 90        # seconds without data from the server
DEFAULT_WORKERS = 2              # Overpass is a shared public server: stay polite
SIZE_LIMIT_BYTES = 200 * 1024 * 1024        # hard stop requested by the owner
GITHUB_BROWSER_LIMIT = 25 * 1024 * 1024     # GitHub web upload limit per file
GITHUB_HARD_LIMIT = 100 * 1024 * 1024       # GitHub rejects bigger files
ROW_ABORT_LIMIT = 6_000_000      # far beyond any realistic size: stop early instead of filling the disk


# Overpass QL for one bounding box: same destination types as the live app (residential only if requested).
def tile_query(south: float, west: float, north: float, east: float, include_residential: bool) -> str:
    box = f"({south:.5f},{west:.5f},{north:.5f},{east:.5f})"
    residential = (f'nwr["building"~"apartments|residential|house|dormitory"]{box};'
                   if include_residential else "")
    return f"""
[out:json][timeout:120];
(
  nwr["public_transport"~"station|stop_area"]{box};
  nwr["railway"~"station|halt|subway_entrance"]{box};
  nwr["amenity"~"school|college|university|kindergarten"]{box};
  nwr["amenity"~"hospital|clinic|doctors|pharmacy|dentist"]{box};
  nwr["amenity"~"restaurant|cafe|fast_food|food_court|bar"]{box};
  nwr["amenity"="marketplace"]{box};
  nwr["shop"~"mall|department_store|supermarket|convenience|greengrocer"]{box};
  nwr["office"]{box};
  nwr["leisure"~"park|garden|playground"]{box};
  nwr["tourism"~"attraction|museum|gallery|theme_park|viewpoint"]{box};
  {residential}
);
out center tags;
"""


# Reduce raw Overpass elements to compact rows (only what the app needs).
def reduce_elements(elements: list[dict[str, Any]], include_residential: bool) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for element in elements:
        center = element.get("center") or {"lat": element.get("lat"), "lon": element.get("lon")}
        latitude, longitude = safe_float(center.get("lat")), safe_float(center.get("lon"))
        tags = element.get("tags") or {}
        category = classify_destination(tags)
        if latitude is None or longitude is None or category is None:
            continue
        if category == "residential" and not include_residential:
            continue
        rows.append({"osm_type": element.get("type", "osm"), "osm_id": element.get("id", ""),
                     "lat": round(latitude, 6), "lon": round(longitude, 6),
                     "category": category,
                     "name": osm_name(tags, "")})
    return rows


# Download one tile, trying every mirror with short pauses. Returns reduced rows, or None when it fails
# or when the tile's deadline has passed.
def download_tile(client: HttpClient, box: tuple[float, float, float, float],
                  include_residential: bool, deadline: float) -> Optional[list[dict[str, Any]]]:
    query = tile_query(*box, include_residential)
    for attempt in range(2):
        for endpoint in OVERPASS_ENDPOINTS:
            if time.monotonic() > deadline:
                return None
            try:
                text = client.post_form(endpoint, data={"data": query}, timeout=(10, REQUEST_READ_TIMEOUT))
                payload = json.loads(text)
                if isinstance(payload, dict) and "elements" in payload:
                    return reduce_elements(payload["elements"], include_residential)
            except Exception:
                continue
        if time.monotonic() + 5 * (attempt + 1) > deadline:
            return None
        time.sleep(5 * (attempt + 1))
    return None


# Split a box into four quadrants (used when a dense tile, such as Mong Kok, is too heavy for one request).
def split_box(box: tuple[float, float, float, float]) -> list[tuple[float, float, float, float]]:
    south, west, north, east = box
    mid_lat, mid_lon = (south + north) / 2, (west + east) / 2
    return [(south, west, mid_lat, mid_lon), (south, mid_lon, mid_lat, east),
            (mid_lat, west, north, mid_lon), (mid_lat, mid_lon, north, east)]


# Stable file name for a tile's saved result (lets an interrupted run resume).
def tile_file(tile_dir: Path, box: tuple[float, float, float, float]) -> Path:
    return tile_dir / ("tile_%.4f_%.4f_%.4f_%.4f.json" % box)


# Worker job: load the tile from disk if done, else download (splitting on failure). Returns (rows, failed boxes).
def process_tile(box: tuple[float, float, float, float], tile_dir: Path, include_residential: bool,
                 depth: int = 0, deadline: Optional[float] = None
                 ) -> tuple[list[dict[str, Any]], list[tuple[float, float, float, float]]]:
    saved = tile_file(tile_dir, box)
    if saved.exists():
        try:
            return json.loads(saved.read_text(encoding="utf-8")), []
        except Exception:
            saved.unlink(missing_ok=True)

    # One shared deadline per top-level tile, so a bad area can never block the run for hours.
    if deadline is None:
        deadline = time.monotonic() + TILE_TIME_LIMIT
    print(f"  downloading tile {box[0]:.2f},{box[1]:.2f} (depth {depth})...", flush=True)
    client = HttpClient(timeout_seconds=REQUEST_READ_TIMEOUT, retries=0, backoff=0.0, status_forcelist=())
    rows = download_tile(client, box, include_residential, deadline)
    if rows is not None:
        saved.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
        return rows, []

    # Too heavy or unstable: split into quadrants and try each one (unless out of time or depth).
    if depth >= MAX_SPLIT_DEPTH or time.monotonic() > deadline:
        print(f"  tile {box[0]:.2f},{box[1]:.2f} FAILED (skipped; re-run later to retry)", flush=True)
        return [], [box]
    all_rows: list[dict[str, Any]] = []
    all_failed: list[tuple[float, float, float, float]] = []
    for sub_box in split_box(box):
        sub_rows, sub_failed = process_tile(sub_box, tile_dir, include_residential, depth + 1, deadline)
        all_rows.extend(sub_rows)
        all_failed.extend(sub_failed)
    return all_rows, all_failed


# Read existing live-app caches (.radial_flow_cache/overpass_*.json) so earlier searches are not wasted.
def rows_from_cache(cache_dir: Path, include_residential: bool) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not cache_dir.exists():
        return rows
    for cache_file in cache_dir.glob("overpass_*.json"):
        try:
            payload = json.loads(cache_file.read_text(encoding="utf-8"))
            rows.extend(reduce_elements(payload.get("elements", []), include_residential))
        except Exception:
            continue
    return rows


# Main flow: tile the territory, download in parallel, merge, de-duplicate, write, then enforce the size limit.
def main() -> int:
    parser = argparse.ArgumentParser(description=f"{APP_NAME}: build the Hong Kong POI snapshot")
    parser.add_argument("--output", default=str(SCRIPT_DIR / "data" / "hk_pois.csv.gz"))
    parser.add_argument("--cache-dir", default=str(Path.cwd() / ".radial_flow_cache"))
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="1-4 (default 2, be polite)")
    parser.add_argument("--include-residential", action="store_true",
                        help="also download residential buildings (much larger file)")
    # parse_known_args ignores the extra arguments that Jupyter passes to the kernel.
    arguments = parser.parse_known_args()[0]

    output_path = Path(arguments.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tile_dir = output_path.parent / ".snapshot_tiles"
    tile_dir.mkdir(exist_ok=True)
    workers = max(1, min(4, arguments.workers))

    # Build the tile grid covering Hong Kong.
    south0, west0, north1, east1 = HK_BBOX
    boxes: list[tuple[float, float, float, float]] = []
    latitude = south0
    while latitude < north1:
        longitude = west0
        while longitude < east1:
            boxes.append((latitude, longitude, min(latitude + TILE_DEGREES, north1),
                          min(longitude + TILE_DEGREES, east1)))
            longitude += TILE_DEGREES
        latitude += TILE_DEGREES
    print(f"{APP_NAME}: {len(boxes)} tiles, {workers} worker(s). Finished tiles are kept, so you can re-run safely.")

    # Download tiles with ThreadPoolExecutor; results are merged in the main thread only.
    all_rows: list[dict[str, Any]] = []
    failed_boxes: list[tuple[float, float, float, float]] = []
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="poi-tile") as pool:
        futures = {pool.submit(process_tile, box, tile_dir, arguments.include_residential): box for box in boxes}
        for done_count, future in enumerate(as_completed(futures), start=1):
            rows, failed = future.result()
            all_rows.extend(rows)
            failed_boxes.extend(failed)
            print(f"[{done_count}/{len(boxes)}] total places so far: {len(all_rows)}"
                  + (f"  (failed areas: {len(failed_boxes)})" if failed_boxes else ""))
            if len(all_rows) > ROW_ABORT_LIMIT:
                print("STOP: far too many rows; file would exceed the 200 MB limit. Nothing produced.")
                pool.shutdown(wait=False, cancel_futures=True)
                return 1

    # Add earlier live-app caches, then de-duplicate by OSM type + id.
    all_rows.extend(rows_from_cache(Path(arguments.cache_dir), arguments.include_residential))
    unique: dict[tuple[str, Any], dict[str, Any]] = {}
    for row in all_rows:
        unique[(row["osm_type"], row["osm_id"])] = row
    final_rows = list(unique.values())
    final_rows.sort(key=lambda r: (r["category"], r["lat"], r["lon"]))

    # Write to a temporary file first, so a too-big result never replaces anything.
    temporary_path = output_path.with_suffix(output_path.suffix + ".tmp")
    with gzip.open(temporary_path, "wt", encoding="utf-8", newline="", compresslevel=9) as file_object:
        writer = csv.DictWriter(file_object, fieldnames=["osm_type", "osm_id", "lat", "lon", "category", "name"])
        writer.writeheader()
        writer.writerows(final_rows)
    size_bytes = temporary_path.stat().st_size

    # Owner rule: more than 200 MB -> do not produce the file.
    if size_bytes > SIZE_LIMIT_BYTES:
        temporary_path.unlink(missing_ok=True)
        print(f"STOP: result would be {size_bytes / 1024 / 1024:.1f} MB (> 200 MB). File NOT produced.")
        return 1

    temporary_path.replace(output_path)
    meta = {
        "built_at_hkt": datetime.now(HK_TZ).isoformat(),
        "rows": len(final_rows),
        "size_mb": round(size_bytes / 1024 / 1024, 2),
        "failed_areas": [list(box) for box in failed_boxes],
        "include_residential": arguments.include_residential,
        "attribution": "(c) OpenStreetMap contributors, ODbL 1.0 - https://www.openstreetmap.org/copyright",
    }
    (output_path.parent / "hk_pois_meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    print(f"\nDone: {len(final_rows)} places, {size_bytes / 1024 / 1024:.1f} MB -> {output_path}")
    if size_bytes > GITHUB_HARD_LIMIT:
        print("WARNING: above GitHub's 100 MB file limit; it cannot be pushed to GitHub as one file.")
    elif size_bytes > GITHUB_BROWSER_LIMIT:
        print("NOTE: above 25 MB; drag-and-drop upload on github.com will fail. Use GitHub Desktop or git push.")
    if failed_boxes:
        print(f"WARNING: {len(failed_boxes)} area(s) failed. Run the script again to retry only those.")
    return 0


if __name__ == "__main__":
    exit_code = main()
    # Inside Jupyter, sys.exit would print a confusing traceback, so just report the code.
    if "ipykernel" in sys.modules:
        print(f"Finished with exit code {exit_code}")
    else:
        sys.exit(exit_code)
