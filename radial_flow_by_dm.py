#!/usr/bin/env python3
# ---------------------------------------------------------------------------
# Radial Flow by DM
# Relative pedestrian-flow potential explorer for Hong Kong (GTFS + OSM + routing).
# Concurrency model: concurrent.futures.ThreadPoolExecutor ONLY (4-8 workers).
# ---------------------------------------------------------------------------
from __future__ import annotations

# Standard-library imports used across the whole program.
import argparse
import csv
import html
import io
import json
import math
import random
import re
import sys
import time
import urllib.parse
import webbrowser
import zipfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Optional

# Third-party HTTP stack: requests with automatic retry/backoff for flaky public APIs.
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Folium is optional at import time; the map step raises a clear error if it is missing.
try:
    import folium
    from branca.element import Element
except ImportError:
    folium = None
    Element = None

# Hong Kong timezone with a fixed UTC+8 fallback when tz database is unavailable.
try:
    from zoneinfo import ZoneInfo

    try:
        HK_TZ = ZoneInfo("Asia/Hong_Kong")
    except Exception:
        HK_TZ = timezone(timedelta(hours=8))
except ImportError:
    HK_TZ = timezone(timedelta(hours=8))


# ---------------------------------------------------------------------------
# Project identity
# ---------------------------------------------------------------------------
# Single source of truth for the project name, so every window/map/log/CSV/User-Agent uses it.
APP_NAME = "Radial Flow by DM"
APP_SLUG = "RadialFlowByDM"
APP_VERSION = "8.0"

# Parallelism limits: 4-8 workers feels much faster, more than that risks throttling/failures.
MIN_WORKERS = 4
MAX_WORKERS = 8
DEFAULT_WORKERS = 4

# Fast-fail tuning: short timeouts, one retry, and a circuit breaker so a dead server costs seconds, not minutes.
ROUTE_TIMEOUT = (5, 20)          # (connect, read) seconds for each routing call
OVERPASS_TIMEOUT = (5, 20)       # (connect, read) seconds per Overpass mirror; no retry, next mirror instead
ROUTE_MAX_ATTEMPTS = 4           # attempts per router per route (429/5xx are retried with back-off)
ROUTE_MAX_WAIT = 20.0            # longest single back-off wait in seconds
ROUTE_ROUNDS = 4                 # passes over still-unresolved routes; NO straight-line estimates are ever used
ROUTE_PACE_SECONDS = (0.8, 1.6, 3.0, 5.0)   # gap between request starts in each round (slower each retry round)
ROUTE_ROUND_COOLDOWN = (0, 8, 15, 25)       # seconds to let the public servers cool down before each round
OVERPASS_CACHE_SECONDS = 24 * 60 * 60


# ---------------------------------------------------------------------------
# External endpoints and static model parameters
# ---------------------------------------------------------------------------
# Official Hong Kong GTFS feed (public transport timetable and headways).
GTFS_URLS = ("https://static.data.gov.hk/td/pt-headway-en/gtfs.zip",)

# Overpass mirrors tried in order for OpenStreetMap destination POIs.
OVERPASS_ENDPOINTS = (
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass-api.de/api/interpreter",
    "https://lz4.overpass-api.de/api/interpreter",
)

# Pedestrian routing engines: Valhalla first, OSRM foot profile as second choice.
VALHALLA_ENDPOINTS = ("https://valhalla1.openstreetmap.de/route",)
OSRM_FOOT_ENDPOINTS = ("https://routing.openstreetmap.de/routed-foot/route/v1/driving",)

# Satellite base map tiles and their required attribution.
ESRI_WORLD_IMAGERY_URL = (
    "https://server.arcgisonline.com/ArcGIS/rest/services/"
    "World_Imagery/MapServer/tile/{z}/{y}/{x}"
)
ESRI_WORLD_IMAGERY_ATTRIBUTION = (
    "Tiles &copy; Esri, Maxar, Earthstar Geographics, and the GIS User Community"
)

# Browser-like User-Agent carrying the new project name.
BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    f"AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36 {APP_SLUG}/{APP_VERSION}"
)

# Descriptive User-Agent for API servers (Overpass returns 406 for browser-impersonating agents).
API_USER_AGENT = f"{APP_SLUG}/{APP_VERSION} (Hong Kong pedestrian-flow research tool; python-requests)"

# Relative supply weight per transport operator (heavier rail = more passenger potential).
AGENCY_WEIGHTS = {
    "MTR": 7.0, "LRT": 4.5, "KMB": 3.0, "LWB": 3.0, "CTB": 3.0, "NWFB": 3.0,
    "NLB": 2.6, "GMB": 2.3, "TRAM": 2.3, "FERRY": 2.0, "XB": 1.5, "UNKNOWN": 2.0,
}

# Relative attraction weight per destination category.
DESTINATION_WEIGHTS = {
    "transport_hub": 4.5, "school": 3.8, "hospital": 4.5, "clinic": 3.2,
    "shopping": 3.8, "supermarket": 3.3, "restaurant": 2.6, "market": 3.7,
    "office": 3.3, "park": 1.6, "residential": 4.0, "attraction": 3.0,
    "target_area": 6.0,
}

# Maximum number of destinations kept per category, to stop one type dominating.
DEFAULT_CATEGORY_CAPS = {
    "transport_hub": 6, "restaurant": 5, "school": 4, "hospital": 3, "clinic": 4,
    "shopping": 4, "supermarket": 4, "market": 3, "office": 4, "park": 3,
    "residential": 5, "attraction": 4,
}


# ---------------------------------------------------------------------------
# Core data types
# ---------------------------------------------------------------------------
# Application-level error shown cleanly to the user (CLI/GUI) instead of a traceback.
class AppError(RuntimeError):
    pass


# Immutable WGS84 coordinate.
@dataclass(frozen=True)
class Point:
    lat: float
    lng: float


# All user-adjustable analysis settings, validated before any network call is made.
@dataclass
class Config:
    subject: Point
    subject_label: str
    start_datetime: datetime
    end_datetime: datetime
    radius_m: float = 900.0
    target_tolerance_m: float = 40.0
    max_sources: int = 12
    max_destinations: int = 25
    include_osm_optional: bool = True
    include_residential_optional: bool = False
    output_dir: Path = field(default_factory=lambda: Path.cwd() / "output")
    cache_dir: Path = field(default_factory=lambda: Path.cwd() / ".radial_flow_cache")
    timeout_seconds: int = 45
    period_minutes: int = 30
    workers: int = DEFAULT_WORKERS

    # Number of ThreadPoolExecutor workers, always clamped into the safe 4-8 range.
    @property
    def effective_workers(self) -> int:
        return max(MIN_WORKERS, min(MAX_WORKERS, int(self.workers)))

    # Reject impossible inputs early with human-readable messages.
    def validate(self) -> None:
        if not -90 <= self.subject.lat <= 90:
            raise AppError("Latitude must be between -90 and 90.")
        if not -180 <= self.subject.lng <= 180:
            raise AppError("Longitude must be between -180 and 180.")
        if self.end_datetime <= self.start_datetime:
            raise AppError("End time must be later than start time.")
        if self.end_datetime - self.start_datetime > timedelta(hours=24):
            raise AppError("The selected period must not exceed 24 hours.")
        if self.radius_m <= 0:
            raise AppError("Radius must be greater than zero.")
        if self.target_tolerance_m <= 0:
            raise AppError("Target tolerance must be greater than zero.")
        if self.max_sources <= 0:
            raise AppError("Maximum source stops must be greater than zero.")
        if self.max_destinations <= 0:
            raise AppError("Maximum destinations must be greater than zero.")
        if self.period_minutes <= 0:
            raise AppError("Period interval must be greater than zero.")
        if self.workers <= 0:
            raise AppError("Workers must be greater than zero.")


# ---------------------------------------------------------------------------
# HTTP client
# ---------------------------------------------------------------------------
# Shared requests session with retry/backoff and a connection pool sized for 8 parallel workers.
class HttpClient:
    def __init__(self, timeout_seconds: int = 45, retries: int = 3, backoff: float = 1.0,
                 status_forcelist: tuple[int, ...] = (429, 500, 502, 503, 504)) -> None:
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()

        # Retry on throttling (429) and transient server errors; "retries" is lowered for fast-fail clients.
        retry = Retry(
            total=retries, connect=retries, read=retries, status=retries, backoff_factor=backoff,
            status_forcelist=status_forcelist,
            allowed_methods=frozenset(("GET", "POST")),
            respect_retry_after_header=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.session.headers.update({
            "User-Agent": API_USER_AGENT,
            "Accept": "*/*",
            "Accept-Language": "zh-Hant,en-US;q=0.9,en;q=0.8",
            "Connection": "keep-alive",
        })

    # Download a binary ZIP (GTFS) and sanity-check that it really is a ZIP.
    def download(self, url: str, timeout: Optional[int] = None) -> bytes:
        response = self.session.get(
            url,
            headers={
                "User-Agent": BROWSER_USER_AGENT,
                "Accept": "application/zip,application/octet-stream,application/x-zip-compressed,*/*",
                "Referer": "https://data.gov.hk/",
                "Origin": "https://data.gov.hk",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            },
            timeout=timeout or max(self.timeout_seconds, 120),
            allow_redirects=True,
        )
        response.raise_for_status()
        content = response.content
        if len(content) < 100:
            raise AppError(f"Downloaded GTFS file is unexpectedly small: {url}")
        if content[:2] != b"PK":
            raise AppError(f"Downloaded file is not a ZIP archive: {url}")
        return content

    # Send one request and return the raw Response (no raise), so callers can handle 429 / Retry-After themselves.
    def request_raw(self, method: str, url: str, params: Optional[dict[str, Any]] = None,
                    json_body: Optional[dict[str, Any]] = None, timeout: Any = None) -> requests.Response:
        return self.session.request(
            method, url, params=params, json=json_body,
            headers={"User-Agent": API_USER_AGENT, "Accept": "*/*"},
            timeout=timeout or self.timeout_seconds,
        )

    # GET a JSON object.
    def get_json(self, url: str, params: Optional[dict[str, Any]] = None,
                 timeout: Any = None) -> dict[str, Any]:
        response = self.session.get(url, params=params, timeout=timeout or self.timeout_seconds)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise AppError(f"Unexpected JSON response from {url}")
        return payload

    # POST a JSON body and read a JSON object back.
    def post_json(self, url: str, body: dict[str, Any],
                  timeout: Any = None) -> dict[str, Any]:
        response = self.session.post(
            url, json=body,
            headers={"User-Agent": API_USER_AGENT, "Accept": "*/*",
                     "Content-Type": "application/json"},
            timeout=timeout or self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise AppError(f"Unexpected JSON response from {url}")
        return payload

    # POST a form (Overpass expects application/x-www-form-urlencoded).
    def post_form(self, url: str, data: dict[str, Any], timeout: Any = None) -> str:
        response = self.session.post(
            url, data=data,
            headers={"User-Agent": API_USER_AGENT, "Accept": "*/*",
                     "Content-Type": "application/x-www-form-urlencoded"},
            timeout=timeout or self.timeout_seconds,
        )
        response.raise_for_status()
        return response.text


# ---------------------------------------------------------------------------
# Small parsing and geometry helpers
# ---------------------------------------------------------------------------
# Tolerant float conversion: returns the default instead of raising.
def safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


# Tolerant int conversion (accepts "3.0"): returns the default instead of raising.
def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


# Parse "YYYY-MM-DD HH:MM" style text into a Hong Kong-timezone datetime.
def parse_datetime_hkt(text: str) -> datetime:
    cleaned = text.strip()
    formats = ("%Y-%m-%d %H:%M", "%Y-%m-%d %H%M", "%Y-%m-%d %H:%M:%S",
               "%Y/%m/%d %H:%M", "%Y-%m-%dT%H:%M")
    for format_string in formats:
        try:
            return datetime.strptime(cleaned, format_string).replace(tzinfo=HK_TZ)
        except ValueError:
            continue
    raise AppError("Time must use YYYY-MM-DD HH:MM, for example 2026-10-01 08:00.")


# GTFS times may exceed 24:00:00 (after-midnight trips); return seconds since service-day start.
def parse_gtfs_time(value: str) -> Optional[int]:
    try:
        pieces = value.strip().split(":")
        if len(pieces) != 3:
            return None
        hours, minutes, seconds = (int(piece) for piece in pieces)
        if hours < 0 or minutes not in range(60) or seconds not in range(60):
            return None
        return hours * 3600 + minutes * 60 + seconds
    except (TypeError, ValueError):
        return None


# Great-circle distance in metres between two WGS84 points.
def haversine_m(a: Point, b: Point) -> float:
    earth_radius = 6371000.0
    lat1, lat2 = math.radians(a.lat), math.radians(b.lat)
    delta_lat = math.radians(b.lat - a.lat)
    delta_lng = math.radians(b.lng - a.lng)
    value = (math.sin(delta_lat / 2) ** 2
             + math.cos(lat1) * math.cos(lat2) * math.sin(delta_lng / 2) ** 2)
    return 2 * earth_radius * math.asin(min(1.0, math.sqrt(value)))


# Shortest distance in metres from a point to a line segment (local flat-earth projection).
def point_to_segment_m(point: Point, start: Point, end: Point) -> float:
    mean_latitude = math.radians((point.lat + start.lat + end.lat) / 3)
    x_scale = 111320.0 * math.cos(mean_latitude)
    y_scale = 110540.0
    ax, ay = start.lng * x_scale, start.lat * y_scale
    bx, by = end.lng * x_scale, end.lat * y_scale
    px, py = point.lng * x_scale, point.lat * y_scale
    dx, dy = bx - ax, by - ay
    if dx == 0 and dy == 0:
        return math.hypot(px - ax, py - ay)
    projection = ((px - ax) * dx + (py - ay) * dy) / (dx * dx + dy * dy)
    projection = max(0.0, min(1.0, projection))
    return math.hypot(px - (ax + projection * dx), py - (ay + projection * dy))


# Decode a Google/Valhalla encoded polyline (Valhalla uses precision 6) into [lat, lng] pairs.
def decode_polyline(encoded: str, precision: int = 6) -> list[list[float]]:
    coordinates: list[list[float]] = []
    index = latitude = longitude = 0
    factor = 10 ** precision
    while index < len(encoded):
        for longitude_component in (False, True):
            shift = result = 0
            while True:
                if index >= len(encoded):
                    raise AppError("Invalid encoded route geometry.")
                byte = ord(encoded[index]) - 63
                index += 1
                result |= (byte & 0x1F) << shift
                shift += 5
                if byte < 0x20:
                    break
            delta = ~(result >> 1) if result & 1 else result >> 1
            if longitude_component:
                longitude += delta
            else:
                latitude += delta
        coordinates.append([latitude / factor, longitude / factor])
    return coordinates


# Turn raw text (lat,lng / Google Maps URL / short link / Plus Code) into a Point.
def parse_location(text: str, client: HttpClient) -> Point:
    cleaned = text.strip()
    if not cleaned:
        raise AppError("Please enter coordinates or a Google Maps link.")

    # Unwrap markdown-style links like [label](https://...).
    markdown_match = re.search(r"\((https?://[^)]+)\)", cleaned)
    if markdown_match:
        cleaned = markdown_match.group(1)
    decoded = urllib.parse.unquote(cleaned)

    # Patterns for @lat,lng, !3d..!4d.., ?q=lat,lng, and a bare "lat, lng".
    patterns = (
        r"@(-?\d{1,2}(?:\.\d+)?),(-?\d{1,3}(?:\.\d+)?)",
        r"!3d(-?\d{1,2}(?:\.\d+)?)!4d(-?\d{1,3}(?:\.\d+)?)",
        r"[?&](?:q|query|ll)=(-?\d{1,2}(?:\.\d+)?),(-?\d{1,3}(?:\.\d+)?)",
        r"^\s*(-?\d{1,2}(?:\.\d+)?)\s*,\s*(-?\d{1,3}(?:\.\d+)?)\s*$",
    )

    # First try to read coordinates straight from the text.
    for pattern in patterns:
        match = re.search(pattern, decoded, re.IGNORECASE)
        if match:
            latitude, longitude = float(match.group(1)), float(match.group(2))
            if -90 <= latitude <= 90 and -180 <= longitude <= 180:
                return Point(latitude, longitude)

    # Short links (maps.app.goo.gl etc.): follow redirects, then re-run the patterns.
    if cleaned.lower().startswith(("http://", "https://")):
        try:
            response = client.session.get(
                cleaned, timeout=client.timeout_seconds, allow_redirects=True,
                headers={"User-Agent": BROWSER_USER_AGENT},
            )
            redirected = urllib.parse.unquote(response.url)
            for pattern in patterns:
                match = re.search(pattern, redirected, re.IGNORECASE)
                if match:
                    latitude, longitude = float(match.group(1)), float(match.group(2))
                    if -90 <= latitude <= 90 and -180 <= longitude <= 180:
                        return Point(latitude, longitude)
        except Exception:
            pass

    # Last resort: Open Location Code (Plus Code), needs the optional package.
    plus_code_match = re.search(
        r"([23456789CFGHJMPQRVWX]{8,}\+[23456789CFGHJMPQRVWX]{2,})", decoded.upper()
    )
    if plus_code_match:
        try:
            from openlocationcode import openlocationcode

            area = openlocationcode.decode(plus_code_match.group(1))
            return Point(area.latitudeCenter, area.longitudeCenter)
        except ImportError as exc:
            raise AppError("Install openlocationcode to use Plus Codes: "
                           "pip install openlocationcode") from exc
        except Exception as exc:
            raise AppError(f"Could not decode the Plus Code: {exc}") from exc

    raise AppError("No coordinates were found. Enter latitude,longitude "
                   "or paste a Google Maps link containing coordinates.")


# Map OpenStreetMap tags onto one of the model's destination categories (or None to ignore).
def classify_destination(tags: dict[str, Any]) -> Optional[str]:
    amenity = str(tags.get("amenity", "")).lower()
    shop = str(tags.get("shop", "")).lower()
    leisure = str(tags.get("leisure", "")).lower()
    building = str(tags.get("building", "")).lower()
    office = str(tags.get("office", "")).lower()
    public_transport = str(tags.get("public_transport", "")).lower()
    railway = str(tags.get("railway", "")).lower()
    tourism = str(tags.get("tourism", "")).lower()

    if public_transport in {"station", "stop_area"} or railway in {"station", "halt", "subway_entrance"}:
        return "transport_hub"
    if amenity in {"school", "college", "university", "kindergarten"}:
        return "school"
    if amenity == "hospital":
        return "hospital"
    if amenity in {"clinic", "doctors", "pharmacy", "dentist"}:
        return "clinic"
    if amenity in {"restaurant", "cafe", "fast_food", "food_court", "bar"}:
        return "restaurant"
    if amenity == "marketplace":
        return "market"
    if shop in {"mall", "department_store"}:
        return "shopping"
    if shop in {"supermarket", "convenience", "greengrocer"}:
        return "supermarket"
    if leisure in {"park", "garden", "playground"}:
        return "park"
    if office:
        return "office"
    if tourism in {"attraction", "museum", "gallery", "theme_park", "viewpoint"}:
        return "attraction"
    if building in {"apartments", "residential", "house", "dormitory"}:
        return "residential"
    return None


# Pick the best display name, preferring Traditional Chinese.
def osm_name(tags: dict[str, Any], fallback: str) -> str:
    return str(tags.get("name:zh-Hant") or tags.get("name:zh")
               or tags.get("official_name") or tags.get("name") or fallback)


# Write a list of dict rows to a UTF-8-BOM CSV (opens correctly in Excel with Chinese text).
def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8-sig")
        return
    fieldnames: list[str] = []
    known_fields: set[str] = set()
    for row in rows:
        for field_name in row:
            if field_name not in known_fields:
                known_fields.add(field_name)
                fieldnames.append(field_name)
    with path.open("w", newline="", encoding="utf-8-sig") as file_object:
        writer = csv.DictWriter(file_object, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# Main analysis engine
# ---------------------------------------------------------------------------
class FootfallAnalyzer:
    # Validate config, create the HTTP client, prepare route cache/counters and output folders.
    def __init__(self, config: Config, logger: Callable[[str], None] = print) -> None:
        self.config = config
        self.config.validate()
        self.log = logger
        self.client = HttpClient(config.timeout_seconds)

        # Routing client: no automatic retries, so 429 / Retry-After are handled explicitly in _call_router.
        self.route_client = HttpClient(config.timeout_seconds, retries=0, backoff=0.0, status_forcelist=())

        # Overpass client: zero retries, so a slow mirror is abandoned after one timeout and the next is tried.
        self.overpass_client = HttpClient(config.timeout_seconds, retries=0, backoff=0.0)

        # Routes that could not be resolved by any router (excluded from scoring, reported in CSV/summary).
        self.failed_routes: list[dict[str, Any]] = []

        # Route cache and counters are ONLY touched from the main thread (workers stay pure),
        # so no locks are needed while ThreadPoolExecutor runs the network calls.
        self.route_cache: dict[tuple[float, float, float, float], dict[str, Any]] = {}
        self.routing_attempts = 0
        self.routing_successes = 0

        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        self.config.cache_dir.mkdir(parents=True, exist_ok=True)

    # Stream rows of one GTFS text file lazily (keeps memory low on the big stop_times.txt).
    @staticmethod
    def gtfs_rows(archive: zipfile.ZipFile, member_name: str):
        if member_name not in archive.namelist():
            return
        with archive.open(member_name) as binary_file:
            with io.TextIOWrapper(binary_file, encoding="utf-8-sig", newline="") as text_file:
                yield from csv.DictReader(text_file)

    # Return the GTFS zip: fresh cache (<24h) -> download -> stale cache as last fallback.
    def load_gtfs_zip(self) -> zipfile.ZipFile:
        cache_path = self.config.cache_dir / "hong_kong_official_gtfs.zip"
        cache_fresh = (cache_path.exists()
                       and time.time() - cache_path.stat().st_mtime < 24 * 60 * 60)

        if cache_fresh:
            self.log(f"Using cached GTFS: {cache_path}")
            try:
                return zipfile.ZipFile(cache_path)
            except zipfile.BadZipFile:
                cache_path.unlink(missing_ok=True)

        self.log("Downloading official Hong Kong GTFS feed...")
        errors: list[str] = []
        for url in GTFS_URLS:
            try:
                content = self.client.download(url, timeout=180)
                cache_path.write_bytes(content)
                return zipfile.ZipFile(cache_path)
            except Exception as exc:
                errors.append(f"{url}: {exc}")

        if cache_path.exists():
            try:
                self.log("GTFS download failed; using existing cached copy.")
                return zipfile.ZipFile(cache_path)
            except Exception:
                pass
        raise AppError("Could not download the official GTFS feed. " + " | ".join(errors))

    # Which GTFS service_ids run on a date (calendar.txt weekly rules + calendar_dates.txt exceptions).
    def active_service_ids(self, archive: zipfile.ZipFile, service_date: date) -> set[str]:
        date_key = service_date.strftime("%Y%m%d")
        weekday_field = service_date.strftime("%A").lower()
        active: set[str] = set()

        if "calendar.txt" in archive.namelist():
            for row in self.gtfs_rows(archive, "calendar.txt"):
                if (row.get("start_date", "") <= date_key <= row.get("end_date", "")
                        and row.get(weekday_field) == "1"):
                    active.add(row.get("service_id", ""))

        if "calendar_dates.txt" in archive.namelist():
            for row in self.gtfs_rows(archive, "calendar_dates.txt"):
                if row.get("date") != date_key:
                    continue
                service_id = row.get("service_id", "")
                if row.get("exception_type") == "1":
                    active.add(service_id)
                elif row.get("exception_type") == "2":
                    active.discard(service_id)
        return active

    # GTFS stops inside the analysis radius, with straight-line distance to the subject.
    def nearby_gtfs_stops(self, archive: zipfile.ZipFile) -> dict[str, dict[str, Any]]:
        nearby: dict[str, dict[str, Any]] = {}
        for row in self.gtfs_rows(archive, "stops.txt"):
            latitude = safe_float(row.get("stop_lat"))
            longitude = safe_float(row.get("stop_lon"))
            if latitude is None or longitude is None:
                continue
            point = Point(latitude, longitude)
            distance = haversine_m(self.config.subject, point)
            if distance > self.config.radius_m:
                continue
            stop_id = row.get("stop_id", "")
            if not stop_id:
                continue
            nearby[stop_id] = {
                "stop_id": stop_id,
                "name": row.get("stop_name") or stop_id,
                "point": point,
                "straight_m": round(distance, 3),
            }
        self.log(f"GTFS stops inside radius: {len(nearby)}")
        return nearby

    # Service days to inspect: one day before start (overnight trips) through the end date.
    def service_dates_for_period(self) -> list[date]:
        current_date = self.config.start_datetime.date() - timedelta(days=1)
        last_date = self.config.end_datetime.date()
        dates: list[date] = []
        while current_date <= last_date:
            dates.append(current_date)
            current_date += timedelta(days=1)
        return dates

    # Build ranked "source" stops with scheduled departures inside the chosen time window.
    def load_sources(self) -> tuple[list[dict[str, Any]], list[datetime]]:
        archive = self.load_gtfs_zip()
        try:
            nearby_stops = self.nearby_gtfs_stops(archive)
            if not nearby_stops:
                return [], []

            # Route id -> operator and readable route name.
            route_info: dict[str, dict[str, str]] = {}
            for row in self.gtfs_rows(archive, "routes.txt"):
                route_id = row.get("route_id", "")
                route_info[route_id] = {
                    "operator": (row.get("agency_id") or "UNKNOWN").upper(),
                    "route_name": (row.get("route_short_name")
                                   or row.get("route_long_name") or route_id),
                }

            # Active services per calendar day in the window.
            services_by_date: dict[date, set[str]] = {}
            for service_date in self.service_dates_for_period():
                services_by_date[service_date] = self.active_service_ids(archive, service_date)

            # Trips that run on at least one relevant day.
            active_dates_by_trip: dict[str, list[date]] = defaultdict(list)
            trip_route: dict[str, str] = {}
            for row in self.gtfs_rows(archive, "trips.txt"):
                trip_id = row.get("trip_id", "")
                service_id = row.get("service_id", "")
                route_id = row.get("route_id", "")
                for service_date, active_services in services_by_date.items():
                    if service_id in active_services:
                        active_dates_by_trip[trip_id].append(service_date)
                        trip_route[trip_id] = route_id
            if not active_dates_by_trip:
                raise AppError("No active GTFS services were found for the selected date.")

            # Frequency-based trips (headway_secs) expand into repeated departures later.
            frequencies: dict[str, list[tuple[int, int, int]]] = defaultdict(list)
            if "frequencies.txt" in archive.namelist():
                for row in self.gtfs_rows(archive, "frequencies.txt"):
                    trip_id = row.get("trip_id", "")
                    if trip_id not in active_dates_by_trip:
                        continue
                    start_seconds = parse_gtfs_time(row.get("start_time", ""))
                    end_seconds = parse_gtfs_time(row.get("end_time", ""))
                    headway_seconds = safe_int(row.get("headway_secs"), 0)
                    if start_seconds is not None and end_seconds is not None and headway_seconds > 0:
                        frequencies[trip_id].append((start_seconds, end_seconds, headway_seconds))

            # Single pass over stop_times: first departure per trip + rows at nearby stops.
            first_trip_time: dict[str, int] = {}
            nearby_stop_times: list[tuple[str, str, int]] = []
            for row in self.gtfs_rows(archive, "stop_times.txt"):
                trip_id = row.get("trip_id", "")
                if trip_id not in active_dates_by_trip:
                    continue
                departure_seconds = parse_gtfs_time(
                    row.get("departure_time", "") or row.get("arrival_time", ""))
                if departure_seconds is None:
                    continue
                if trip_id not in first_trip_time or departure_seconds < first_trip_time[trip_id]:
                    first_trip_time[trip_id] = departure_seconds
                stop_id = row.get("stop_id", "")
                if stop_id in nearby_stops:
                    nearby_stop_times.append((trip_id, stop_id, departure_seconds))

            # Per-stop accumulators for departures, routes and operators.
            metrics: dict[str, dict[str, Any]] = {
                stop_id: {"departures": [], "routes": set(), "operators": set()}
                for stop_id in nearby_stops
            }

            # Expand each stop-time into concrete datetimes and keep those inside the window.
            for trip_id, stop_id, stop_departure_seconds in nearby_stop_times:
                route_id = trip_route.get(trip_id, "")
                info = route_info.get(route_id, {"operator": "UNKNOWN",
                                                 "route_name": route_id or trip_id})
                departure_datetimes: list[datetime] = []
                for service_date in active_dates_by_trip[trip_id]:
                    service_midnight = datetime.combine(service_date, datetime.min.time(), tzinfo=HK_TZ)
                    if trip_id in frequencies:
                        first_seconds = first_trip_time.get(trip_id, stop_departure_seconds)
                        offset = stop_departure_seconds - first_seconds
                        for frequency_start, frequency_end, headway in frequencies[trip_id]:
                            current_seconds = frequency_start + offset
                            limit_seconds = frequency_end + offset
                            while current_seconds < limit_seconds:
                                departure_datetimes.append(
                                    service_midnight + timedelta(seconds=current_seconds))
                                current_seconds += headway
                    else:
                        departure_datetimes.append(
                            service_midnight + timedelta(seconds=stop_departure_seconds))

                for departure_datetime in departure_datetimes:
                    if self.config.start_datetime <= departure_datetime < self.config.end_datetime:
                        metrics[stop_id]["departures"].append(departure_datetime)
                        metrics[stop_id]["routes"].add(info["route_name"])
                        metrics[stop_id]["operators"].add(info["operator"])

            # Turn accumulators into scored source rows (departures x operator weight x distance decay).
            sources: list[dict[str, Any]] = []
            for stop_id, stop in nearby_stops.items():
                stop_metrics = metrics[stop_id]
                departure_count = len(stop_metrics["departures"])
                if departure_count <= 0:
                    continue
                operators = sorted(stop_metrics["operators"])
                primary_operator = operators[0] if operators else "UNKNOWN"
                operator_weight = max(
                    (AGENCY_WEIGHTS.get(op, AGENCY_WEIGHTS["UNKNOWN"]) for op in operators),
                    default=AGENCY_WEIGHTS["UNKNOWN"],
                )
                supply_score = (departure_count * operator_weight
                                * math.exp(-stop["straight_m"] / max(self.config.radius_m, 1)))
                sources.append({
                    "stop_id": stop_id,
                    "name": stop["name"],
                    "point": stop["point"],
                    "straight_m": stop["straight_m"],
                    "scheduled_departures": departure_count,
                    "active_routes": len(stop_metrics["routes"]),
                    "route_names": sorted(stop_metrics["routes"]),
                    "operators": operators,
                    "operator": primary_operator,
                    "supply_score": round(supply_score, 6),
                    "departure_datetimes": sorted(stop_metrics["departures"]),
                })

            # Keep only the strongest stops (max_sources) and merge their departures for the profile.
            sources.sort(key=lambda row: (row["supply_score"], row["scheduled_departures"]), reverse=True)
            selected_sources = sources[: self.config.max_sources]
            selected_departures: list[datetime] = []
            for source in selected_sources:
                selected_departures.extend(source["departure_datetimes"])
            selected_departures.sort()
            self.log(f"Selected active source stops: {len(selected_sources)}")
            return selected_sources, selected_departures
        finally:
            archive.close()

    # Build the destination list: the target itself + capped, ranked OSM points of interest.
    def load_destinations(self) -> tuple[list[dict[str, Any]], bool]:
        destinations: list[dict[str, Any]] = [{
            "id": "target",
            "name": f"{self.config.subject_label} (target)",
            "point": self.config.subject,
            "category": "target_area",
            "straight_m": 0.0,
            "attraction_weight": DESTINATION_WEIGHTS["target_area"],
            "data_source": "User-selected target",
        }]
        if not self.config.include_osm_optional:
            return destinations, False

        latitude, longitude = self.config.subject.lat, self.config.subject.lng
        radius = int(self.config.radius_m)

        # Optional residential buildings clause for the Overpass query.
        residential_query = ""
        if self.config.include_residential_optional:
            residential_query = (f'nwr["building"~"apartments|residential|house|dormitory"]'
                                 f"(around:{radius},{latitude},{longitude});")

        # One Overpass QL query covering every supported destination type.
        overpass_query = f"""
[out:json][timeout:25];
(
  nwr["public_transport"~"station|stop_area"](around:{radius},{latitude},{longitude});
  nwr["railway"~"station|halt|subway_entrance"](around:{radius},{latitude},{longitude});
  nwr["amenity"~"school|college|university|kindergarten"](around:{radius},{latitude},{longitude});
  nwr["amenity"~"hospital|clinic|doctors|pharmacy|dentist"](around:{radius},{latitude},{longitude});
  nwr["amenity"~"restaurant|cafe|fast_food|food_court|bar"](around:{radius},{latitude},{longitude});
  nwr["amenity"="marketplace"](around:{radius},{latitude},{longitude});
  nwr["shop"~"mall|department_store|supermarket|convenience|greengrocer"](around:{radius},{latitude},{longitude});
  nwr["office"](around:{radius},{latitude},{longitude});
  nwr["leisure"~"park|garden|playground"](around:{radius},{latitude},{longitude});
  nwr["tourism"~"attraction|museum|gallery|theme_park|viewpoint"](around:{radius},{latitude},{longitude});
  {residential_query}
);
out center tags;
"""

        # Disk cache keyed by location/radius: repeat runs skip the slow Overpass call entirely.
        cache_name = (f"overpass_{latitude:.4f}_{longitude:.4f}_{radius}"
                      f"_{int(self.config.include_residential_optional)}.json")
        overpass_cache = self.config.cache_dir / cache_name
        payload: Optional[dict[str, Any]] = None
        errors: list[str] = []
        if overpass_cache.exists() and time.time() - overpass_cache.stat().st_mtime < OVERPASS_CACHE_SECONDS:
            try:
                cached = json.loads(overpass_cache.read_text(encoding="utf-8"))
                if isinstance(cached, dict):
                    payload = cached
                    self.log(f"Using cached Overpass data: {overpass_cache.name}")
            except Exception:
                payload = None

        # Try each Overpass mirror one by one (sequential on purpose: polite to public servers).
        for endpoint in OVERPASS_ENDPOINTS:
            if payload is not None:
                break
            self.log(f"Querying Overpass: {endpoint}")
            try:
                response_text = self.overpass_client.post_form(
                    endpoint, data={"data": overpass_query}, timeout=OVERPASS_TIMEOUT)
                parsed = json.loads(response_text)
                if isinstance(parsed, dict):
                    payload = parsed
                    overpass_cache.write_text(response_text, encoding="utf-8")
                    break
            except Exception as exc:
                errors.append(f"{endpoint}: {exc}")

        # If every mirror failed, continue with the target only (the run still completes).
        if payload is None:
            self.log("OSM destinations unavailable; continuing with target only.")
            if errors:
                self.log(" | ".join(errors))
            return destinations, False

        # Classify, de-duplicate and score every returned OSM element.
        category_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
        seen_positions: set[tuple[float, float, str]] = set()
        for element in payload.get("elements", []):
            center = element.get("center") or {"lat": element.get("lat"), "lon": element.get("lon")}
            element_latitude = safe_float(center.get("lat"))
            element_longitude = safe_float(center.get("lon"))
            if element_latitude is None or element_longitude is None:
                continue
            point = Point(element_latitude, element_longitude)
            distance = haversine_m(self.config.subject, point)
            if distance > self.config.radius_m:
                continue
            tags = element.get("tags") or {}
            category = classify_destination(tags)
            if category is None:
                continue
            if category == "residential" and not self.config.include_residential_optional:
                continue
            deduplication_key = (round(point.lat, 5), round(point.lng, 5), category)
            if deduplication_key in seen_positions:
                continue
            seen_positions.add(deduplication_key)

            base_weight = DESTINATION_WEIGHTS.get(category, 2.0)
            ranking_score = base_weight * math.exp(-distance / max(self.config.radius_m, 1))
            category_rows[category].append({
                "id": f"{element.get('type', 'osm')}:{element.get('id', '')}",
                "name": osm_name(tags, category.replace("_", " ").title()),
                "point": point,
                "category": category,
                "straight_m": round(distance, 3),
                "attraction_weight": round(base_weight, 6),
                "ranking_score": round(ranking_score, 6),
                "data_source": "OpenStreetMap/Overpass",
            })

        # Apply per-category caps, rank globally, and keep max_destinations - 1 (target uses one slot).
        candidate_destinations: list[dict[str, Any]] = []
        for category, rows in category_rows.items():
            rows.sort(key=lambda row: row["ranking_score"], reverse=True)
            candidate_destinations.extend(rows[: DEFAULT_CATEGORY_CAPS.get(category, 3)])
        candidate_destinations.sort(key=lambda row: row["ranking_score"], reverse=True)
        destinations.extend(candidate_destinations[: max(0, self.config.max_destinations - 1)])
        self.log(f"Selected destinations: {len(destinations)}")
        return destinations, True

    # ------------------------------------------------------------------
    # Pedestrian routing (pure network calls: safe to run inside worker threads)
    # Real walking routes only: if every router fails, the route is reported as failed, never estimated.
    # ------------------------------------------------------------------
    # Seconds to wait after a 429/5xx: honour Retry-After when numeric, else exponential back-off + jitter.
    @staticmethod
    def backoff_seconds(response: Optional[requests.Response], attempt: int) -> float:
        if response is not None:
            header = response.headers.get("Retry-After", "")
            if header.strip().isdigit():
                return min(ROUTE_MAX_WAIT, float(header))
        return min(ROUTE_MAX_WAIT, 2.0 ** (attempt + 1) + random.uniform(0.0, 1.0))

    # Turn a Valhalla JSON payload into the common route dict (raises AppError on a bad payload).
    @staticmethod
    def parse_valhalla(payload: dict[str, Any]) -> dict[str, Any]:
        trip = payload.get("trip") or {}
        status = trip.get("status")
        if status not in (None, 0):
            raise AppError(str(trip.get("status_message") or f"Valhalla status {status}"))
        legs = trip.get("legs") or []
        if not legs:
            raise AppError("Valhalla returned no route legs")

        coordinates: list[list[float]] = []
        instructions: list[str] = []
        for leg in legs:
            encoded_shape = leg.get("shape")
            if encoded_shape:
                decoded_shape = decode_polyline(encoded_shape, precision=6)
                if coordinates and decoded_shape and coordinates[-1] == decoded_shape[0]:
                    decoded_shape = decoded_shape[1:]
                coordinates.extend(decoded_shape)
            for maneuver in leg.get("maneuvers", []):
                if maneuver.get("instruction"):
                    instructions.append(str(maneuver["instruction"]))
        if len(coordinates) < 2:
            raise AppError("Valhalla returned empty geometry")

        summary = trip.get("summary") or {}
        length_km = safe_float(summary.get("length"), 0.0) or 0.0
        duration_seconds = safe_float(summary.get("time"), 0.0) or 0.0
        if length_km <= 0:
            length_m = sum(haversine_m(Point(*coordinates[i]), Point(*coordinates[i + 1]))
                           for i in range(len(coordinates) - 1))
        else:
            length_m = length_km * 1000
        return {
            "route_method": "valhalla_pedestrian", "routing_success": True,
            "length_m": round(length_m, 3), "time_min": round(duration_seconds / 60, 3),
            "route_coordinates": coordinates, "directions": " ".join(instructions)[:4000],
            "routing_error": None,
        }

    # Turn an OSRM JSON payload into the common route dict (raises AppError on a bad payload).
    @staticmethod
    def parse_osrm(payload: dict[str, Any]) -> dict[str, Any]:
        if payload.get("code") != "Ok" or not payload.get("routes"):
            raise AppError(str(payload.get("message") or payload.get("code") or "OSRM routing failed"))
        route = payload["routes"][0]
        geometry = (route.get("geometry") or {}).get("coordinates") or []
        coordinates = [[float(i[1]), float(i[0])] for i in geometry if isinstance(i, list) and len(i) >= 2]
        if len(coordinates) < 2:
            raise AppError("OSRM returned empty geometry")
        return {
            "route_method": "osrm_foot", "routing_success": True,
            "length_m": round(float(route.get("distance") or 0.0), 3),
            "time_min": round(float(route.get("duration") or 0.0) / 60, 3),
            "route_coordinates": coordinates, "directions": "", "routing_error": None,
        }

    # Call one router with 429/5xx-aware retries. Raises AppError after ROUTE_MAX_ATTEMPTS failures.
    def call_router(self, name: str, start: Point, end: Point) -> dict[str, Any]:
        if name == "valhalla":
            endpoints, parser = VALHALLA_ENDPOINTS, self.parse_valhalla
        else:
            endpoints, parser = OSRM_FOOT_ENDPOINTS, self.parse_osrm
        valhalla_body = {
            "locations": [{"lat": start.lat, "lon": start.lng, "type": "break"},
                          {"lat": end.lat, "lon": end.lng, "type": "break"}],
            "costing": "pedestrian", "units": "kilometers",
            "directions_options": {"units": "kilometers", "language": "en-US"},
        }
        coordinate_string = f"{start.lng:.7f},{start.lat:.7f};{end.lng:.7f},{end.lat:.7f}"
        errors: list[str] = []

        for endpoint in endpoints:
            for attempt in range(ROUTE_MAX_ATTEMPTS):
                response: Optional[requests.Response] = None
                try:
                    if name == "valhalla":
                        response = self.route_client.request_raw(
                            "POST", endpoint, json_body=valhalla_body, timeout=ROUTE_TIMEOUT)
                    else:
                        response = self.route_client.request_raw(
                            "GET", f"{endpoint}/{coordinate_string}",
                            params={"overview": "full", "geometries": "geojson",
                                    "steps": "false", "alternatives": "false"},
                            timeout=ROUTE_TIMEOUT)
                except requests.RequestException as exc:
                    errors.append(f"{name} attempt {attempt + 1}: {type(exc).__name__}")
                    time.sleep(self.backoff_seconds(None, attempt))
                    continue

                # Throttled or temporarily broken server: wait, then try the same route again.
                if response.status_code == 429 or response.status_code >= 500:
                    errors.append(f"{name} attempt {attempt + 1}: HTTP {response.status_code}")
                    time.sleep(self.backoff_seconds(response, attempt))
                    continue
                if response.status_code >= 400:
                    # A real client error (for example 400 no route found) will not improve by retrying.
                    errors.append(f"{name}: HTTP {response.status_code} {response.text[:120]}")
                    break
                try:
                    return parser(response.json())
                except Exception as exc:
                    errors.append(f"{name}: bad payload ({exc})")
                    break
        raise AppError(" | ".join(errors))

    # Route one pair: same-location shortcut, otherwise the preferred router first and the other as backup.
    # Raises AppError when BOTH routers fail; there is no straight-line estimate.
    def pedestrian_route(self, start: Point, end: Point, preferred: str = "valhalla") -> dict[str, Any]:
        if haversine_m(start, end) < 8:
            return {
                "route_method": "same_location", "routing_success": True,
                "length_m": 0.0, "time_min": 0.0,
                "route_coordinates": [[start.lat, start.lng], [end.lat, end.lng]],
                "directions": "", "routing_error": None,
            }
        order = ("valhalla", "osrm") if preferred == "valhalla" else ("osrm", "valhalla")
        errors: list[str] = []
        for name in order:
            try:
                return self.call_router(name, start, end)
            except Exception as exc:
                errors.append(str(exc))
        raise AppError(" || ".join(errors))

    # Worker wrapper: wait until this job's scheduled start time (paces requests across all workers), then route.
    def paced_route(self, scheduled_at: float, start: Point, end: Point, preferred: str) -> dict[str, Any]:
        time.sleep(max(0.0, scheduled_at - time.monotonic()))
        return self.pedestrian_route(start, end, preferred)

    # Stable cache key for a start/end coordinate pair.
    @staticmethod
    def route_cache_key(start: Point, end: Point) -> tuple[float, float, float, float]:
        return (round(start.lat, 6), round(start.lng, 6), round(end.lat, 6), round(end.lng, 6))

    # Does a route pass within the tolerance of the target? Returns (flag, minimum distance in m).
    def route_passes_target(self, coordinates: list[list[float]]) -> tuple[bool, float]:
        if len(coordinates) < 2:
            return False, float("inf")
        minimum_distance = float("inf")
        for index in range(len(coordinates) - 1):
            start = Point(coordinates[index][0], coordinates[index][1])
            end = Point(coordinates[index + 1][0], coordinates[index + 1][1])
            minimum_distance = min(minimum_distance,
                                   point_to_segment_m(self.config.subject, start, end))
        return minimum_distance <= self.config.target_tolerance_m, minimum_distance

    # ------------------------------------------------------------------
    # Pair building: routes run in a ThreadPoolExecutor, paced, and unresolved ones are retried in slower rounds
    # ------------------------------------------------------------------
    def build_pairs(self, sources: list[dict[str, Any]],
                    destinations: list[dict[str, Any]]) -> list[dict[str, Any]]:
        jobs: list[tuple[dict[str, Any], dict[str, Any]]] = [
            (source, destination) for source in sources for destination in destinations
        ]
        results: dict[tuple[float, float, float, float], dict[str, Any]] = {}
        unresolved: dict[tuple[float, float, float, float], tuple[Point, Point]] = {}

        # Reuse cached routes; queue each unique missing route once.
        for source, destination in jobs:
            key = self.route_cache_key(source["point"], destination["point"])
            if key in self.route_cache:
                results[key] = dict(self.route_cache[key])
            elif key not in unresolved:
                unresolved[key] = (source["point"], destination["point"])

        total_unique = len(unresolved)
        workers = self.config.effective_workers
        last_errors: dict[tuple[float, float, float, float], str] = {}
        self.log(f"Routing {total_unique} unique routes with ThreadPoolExecutor "
                 f"({workers} workers, up to {ROUTE_ROUNDS} paced rounds; no straight-line estimates)...")

        # Each round re-submits only what is still unresolved, slower than the previous round.
        for round_index in range(ROUTE_ROUNDS):
            if not unresolved:
                break
            if round_index > 0:
                cooldown = ROUTE_ROUND_COOLDOWN[min(round_index, len(ROUTE_ROUND_COOLDOWN) - 1)]
                self.log(f"Round {round_index + 1}: {len(unresolved)} route(s) unresolved; "
                         f"cooling down {cooldown}s before retrying more slowly...")
                time.sleep(cooldown)

            pace = ROUTE_PACE_SECONDS[min(round_index, len(ROUTE_PACE_SECONDS) - 1)]
            round_start = time.monotonic()
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="radial-flow") as pool:
                future_to_key = {}
                for position, (key, (start, end)) in enumerate(list(unresolved.items())):
                    # Alternate the preferred router so the load is split across both public servers.
                    preferred = "valhalla" if (position + round_index) % 2 == 0 else "osrm"
                    future = pool.submit(self.paced_route, round_start + position * pace, start, end, preferred)
                    future_to_key[future] = key

                # Results are handled only in the main thread, so cache and counters need no locks.
                for future in as_completed(future_to_key):
                    key = future_to_key[future]
                    try:
                        result = future.result()
                    except Exception as exc:
                        last_errors[key] = str(exc)[:600]
                        continue
                    results[key] = result
                    self.route_cache[key] = dict(result)
                    unresolved.pop(key, None)
                    last_errors.pop(key, None)
                    self.log(f"Routed {total_unique - len(unresolved)}/{total_unique} ({result['route_method']})")

        # Anything still unresolved is reported and excluded; it is NOT replaced by a straight line.
        self.routing_attempts += total_unique
        self.routing_successes += total_unique - len(unresolved)
        self.failed_routes = []
        failed_keys = set(unresolved)
        for source, destination in jobs:
            key = self.route_cache_key(source["point"], destination["point"])
            if key in failed_keys:
                self.failed_routes.append({
                    "source": source["name"], "destination": destination["name"],
                    "destination_type": destination["category"],
                    "routing_error": last_errors.get(key, "unknown error"),
                })
        if failed_keys:
            self.log(f"WARNING: {len(failed_keys)} route(s) could not be routed and are excluded from scoring.")

        pairs: list[dict[str, Any]] = []
        for source, destination in jobs:
            key = self.route_cache_key(source["point"], destination["point"])
            if key not in results:
                continue
            route = results[key]
            walking_distance = route["length_m"]
            distance_decay = math.exp(-walking_distance / max(self.config.radius_m, 300.0))
            pair_score = source["supply_score"] * destination["attraction_weight"] * distance_decay
            passes_target, minimum_target_distance = self.route_passes_target(route["route_coordinates"])
            pairs.append({
                "source_id": source["stop_id"], "source": source["name"], "operator": source["operator"],
                "destination_id": destination["id"], "destination": destination["name"],
                "destination_type": destination["category"],
                "walk_m": round(walking_distance, 3), "walk_time_min": route["time_min"],
                "route_method": route["route_method"], "routing_success": route["routing_success"],
                "routing_error": route["routing_error"],
                "minimum_target_distance_m": (round(minimum_target_distance, 3)
                                              if math.isfinite(minimum_target_distance) else None),
                "passes_target": passes_target,
                "pair_flow_score": round(pair_score, 6),
                "route_coordinates": route["route_coordinates"],
            })
        if not pairs:
            raise AppError("No walking route could be obtained from any routing server. "
                           "Wait a few minutes and run again (public servers are rate-limiting).")
        pairs.sort(key=lambda row: row["pair_flow_score"], reverse=True)
        return pairs

    # Count scheduled departures per time slot (default 30 minutes) across the window.
    def build_time_profile(self, selected_departures: list[datetime]) -> list[dict[str, Any]]:
        profile: list[dict[str, Any]] = []
        slot_start = self.config.start_datetime
        slot_size = timedelta(minutes=self.config.period_minutes)
        while slot_start < self.config.end_datetime:
            slot_end = min(slot_start + slot_size, self.config.end_datetime)
            departure_count = sum(slot_start <= d < slot_end for d in selected_departures)
            profile.append({
                "slot_start_hkt": slot_start.isoformat(),
                "slot_end_hkt": slot_end.isoformat(),
                "slot_label": f"{slot_start:%H:%M}-{slot_end:%H:%M}",
                "scheduled_departures": departure_count,
            })
            slot_start = slot_end
        return profile

    # Aggregate everything into one JSON-friendly summary with quality flags and contributor mixes.
    def build_summary(self, sources: list[dict[str, Any]], destinations: list[dict[str, Any]],
                      pairs: list[dict[str, Any]], profile: list[dict[str, Any]],
                      osm_success: bool) -> dict[str, Any]:
        total_departures = sum(source["scheduled_departures"] for source in sources)
        total_score = sum(pair["pair_flow_score"] for pair in pairs)
        pass_through_score = sum(p["pair_flow_score"] for p in pairs if p["passes_target"])

        # Score mixes by stop, operator and destination type.
        source_scores: dict[str, float] = defaultdict(float)
        operator_scores: dict[str, float] = defaultdict(float)
        destination_scores: dict[str, float] = defaultdict(float)
        for pair in pairs:
            source_scores[pair["source"]] += pair["pair_flow_score"]
            operator_scores[pair["operator"]] += pair["pair_flow_score"]
            destination_scores[pair["destination_type"]] += pair["pair_flow_score"]

        peak_slot = max(profile, key=lambda row: row["scheduled_departures"], default=None)
        quietest_slot = min(profile, key=lambda row: row["scheduled_departures"], default=None)

        # Data-quality grade depends on routing success rate and OSM availability.
        routing_success_rate = (self.routing_successes / self.routing_attempts
                                if self.routing_attempts else 0.0)
        failed_count = len(self.failed_routes)
        if routing_success_rate >= 0.8 and osm_success and sources:
            data_quality = "High"
        elif routing_success_rate >= 0.4 and sources:
            data_quality = "Medium"
        else:
            data_quality = "Low"

        # Completeness: 100% routing success is misleading when OSM destinations were not loaded.
        if not osm_success:
            completeness = "INCOMPLETE: OSM destinations unavailable (target-only result)"
        elif failed_count:
            completeness = (f"INCOMPLETE: {failed_count} route(s) could not be routed and were excluded "
                            "(no straight-line estimates used)")
        else:
            completeness = "Complete"

        def ranked(mapping: dict[str, float], key_name: str, limit: Optional[int] = None):
            items = sorted(mapping.items(), key=lambda item: item[1], reverse=True)
            if limit:
                items = items[:limit]
            return [{key_name: name, "score": round(score, 6)} for name, score in items]

        return {
            "project": APP_NAME,
            "version": APP_VERSION,
            "generated_at_hkt": datetime.now(HK_TZ).isoformat(),
            "period_start_hkt": self.config.start_datetime.isoformat(),
            "period_end_hkt": self.config.end_datetime.isoformat(),
            "period_minutes": self.config.period_minutes,
            "subject_label": self.config.subject_label,
            "subject_lat": self.config.subject.lat,
            "subject_lng": self.config.subject.lng,
            "radius_m": self.config.radius_m,
            "target_tolerance_m": self.config.target_tolerance_m,
            "map_base_layer": "Esri World Imagery",
            "parallel_workers": self.config.effective_workers,
            "source_count": len(sources),
            "destination_count": len(destinations),
            "pair_count": len(pairs),
            "pass_through_pair_count": sum(bool(p["passes_target"]) for p in pairs),
            "optional_osm_success": osm_success,
            "total_scheduled_departures": total_departures,
            "average_scheduled_departures_per_30m": round(total_departures / max(1, len(profile)), 3),
            "peak_30m_slot": peak_slot["slot_label"] if peak_slot else None,
            "peak_30m_departures": peak_slot["scheduled_departures"] if peak_slot else None,
            "quietest_30m_slot": quietest_slot["slot_label"] if quietest_slot else None,
            "quietest_30m_departures": quietest_slot["scheduled_departures"] if quietest_slot else None,
            "time_profile": profile,
            "total_flow_potential_score": round(total_score, 6),
            "target_pass_through_score": round(pass_through_score, 6),
            "target_pass_through_share": round(pass_through_score / total_score, 6) if total_score else None,
            "routing_attempt_count": self.routing_attempts,
            "routing_success_count": self.routing_successes,
            "routing_api_success_rate": round(routing_success_rate, 6),
            "failed_route_count": failed_count,
            "data_quality": data_quality,
            "completeness": completeness,
            "warning": "Scores are relative model indicators, not observed pedestrian counts.",
            "top_source_contributors": ranked(source_scores, "name", 10),
            "source_operator_mix": ranked(operator_scores, "operator"),
            "destination_type_mix": ranked(destination_scores, "type"),
        }

    # ------------------------------------------------------------------
    # Map + report output
    # ------------------------------------------------------------------
    # Floating "Location profile" HTML card shown on top of the map (branded with the new name).
    def statistics_panel(self, summary: dict[str, Any]) -> str:
        pass_through_share = summary["target_pass_through_share"] or 0.0
        rows = (
            ("Location", summary["subject_label"]),
            ("Period", f"{self.config.start_datetime:%Y-%m-%d %H:%M} to "
                       f"{self.config.end_datetime:%Y-%m-%d %H:%M}"),
            ("Radius", f"{summary['radius_m']:.0f} m"),
            ("Base map", "Esri World Imagery"),
            ("Source stops", summary["source_count"]),
            ("Destinations", summary["destination_count"]),
            ("Scheduled departures", summary["total_scheduled_departures"]),
            ("Peak slot", summary["peak_30m_slot"] or "-"),
            ("Pass-through routes", summary["pass_through_pair_count"]),
            ("Pass-through share", f"{pass_through_share * 100:.1f}%"),
            ("Routing success", f"{summary['routing_api_success_rate'] * 100:.1f}%"),
            ("Routes failed (excluded)", summary["failed_route_count"]),
            ("Parallel workers", summary["parallel_workers"]),
            ("Data quality", summary["data_quality"]),
            ("Completeness", summary["completeness"]),
        )
        table_rows = "".join(
            "<tr><td style='padding:3px 8px 3px 0;color:#cbd5e1;vertical-align:top'>"
            f"{html.escape(str(label))}</td>"
            "<td style='padding:3px 0;font-weight:600;vertical-align:top;color:#ffffff'>"
            f"{html.escape(str(value))}</td></tr>"
            for label, value in rows
        )

        # Red warning banner whenever the result is not complete.
        banner_html = ""
        if summary["completeness"] != "Complete":
            banner_html = ("<div style='margin:6px 0;padding:6px 8px;border-radius:6px;"
                           "background:#7f1d1d;color:#fecaca;font-size:11px;font-weight:600'>"
                           f"{html.escape(summary['completeness'])}</div>")

        return (
            "<div style='position:fixed;top:14px;right:14px;z-index:9999;width:310px;"
            "max-height:calc(100vh - 28px);overflow:auto;background:rgba(15,23,42,0.92);"
            "border:1px solid rgba(255,255,255,0.28);border-radius:10px;padding:13px 15px;"
            "box-shadow:0 8px 28px rgba(0,0,0,0.35);font-family:Arial,sans-serif;"
            "font-size:12px;color:#ffffff;backdrop-filter:blur(5px)'>"
            f"<div style='font-size:15px;font-weight:700;margin-bottom:2px;color:#ffffff'>"
            f"{html.escape(APP_NAME)}</div>"
            "<div style='font-size:11px;margin-bottom:6px;color:#94a3b8'>Location profile</div>"
            f"{banner_html}"
            f"<table style='width:100%;border-collapse:collapse'>{table_rows}</table>"
            "<div style='margin-top:8px;padding-top:8px;border-top:1px solid rgba(255,255,255,0.2);"
            "font-size:10px;line-height:1.4;color:#fbbf24'>"
            "Relative model indicators only; not observed pedestrian counts.</div></div>"
        )

    # Build the interactive Folium map (satellite base, radius, target, stops, POIs, routes).
    def build_map(self, sources: list[dict[str, Any]], destinations: list[dict[str, Any]],
                  pairs: list[dict[str, Any]], summary: dict[str, Any]) -> Path:
        if folium is None or Element is None:
            raise AppError("Map output requires Folium: pip install folium")

        subject = [self.config.subject.lat, self.config.subject.lng]
        map_object = folium.Map(location=subject, zoom_start=16, tiles=None,
                                control_scale=True, prefer_canvas=True)

        # Satellite base layer.
        folium.TileLayer(tiles=ESRI_WORLD_IMAGERY_URL, attr=ESRI_WORLD_IMAGERY_ATTRIBUTION,
                         name="Esri World Imagery", overlay=False, control=False,
                         show=True, max_zoom=20).add_to(map_object)

        # Analysis radius circle.
        folium.Circle(location=subject, radius=self.config.radius_m, color="#38bdf8", weight=3,
                      fill=True, fill_color="#38bdf8", fill_opacity=0.10,
                      tooltip=f"Analysis radius: {self.config.radius_m:.0f} m").add_to(map_object)

        # Pass-through tolerance circle around the target.
        folium.Circle(location=subject, radius=self.config.target_tolerance_m, color="#facc15",
                      weight=2, dash_array="5,5", fill=True, fill_color="#facc15", fill_opacity=0.12,
                      tooltip=f"Pass-through tolerance: {self.config.target_tolerance_m:.0f} m"
                      ).add_to(map_object)

        # Target marker.
        folium.Marker(
            location=subject,
            popup=folium.Popup(
                f"<b>{html.escape(self.config.subject_label)}</b><br>"
                f"{self.config.subject.lat:.6f}, {self.config.subject.lng:.6f}", max_width=340),
            tooltip=self.config.subject_label,
            icon=folium.Icon(color="red", icon="star", prefix="fa"),
        ).add_to(map_object)

        # Public-transport source stops layer.
        source_layer = folium.FeatureGroup(name="Public transport sources", show=True)
        for source in sources:
            popup_html = (
                f"<b>{html.escape(source['name'])}</b><br>"
                f"Operator: {html.escape(', '.join(source['operators']))}<br>"
                f"Scheduled departures: {source['scheduled_departures']}<br>"
                f"Active routes: {source['active_routes']}<br>"
                f"Distance: {source['straight_m']:.0f} m<br>"
                f"Supply score: {source['supply_score']:.3f}"
            )
            folium.CircleMarker(
                location=[source["point"].lat, source["point"].lng], radius=8, color="#ffffff",
                weight=2, fill=True, fill_color="#2563eb", fill_opacity=0.95,
                popup=folium.Popup(popup_html, max_width=360), tooltip=source["name"],
            ).add_to(source_layer)
        source_layer.add_to(map_object)

        # Destination POI layer (the target itself is skipped, it already has a marker).
        destination_layer = folium.FeatureGroup(name="Destinations", show=True)
        for destination in destinations:
            if destination["category"] == "target_area":
                continue
            popup_html = (
                f"<b>{html.escape(destination['name'])}</b><br>"
                f"Type: {html.escape(destination['category'])}<br>"
                f"Distance: {destination['straight_m']:.0f} m<br>"
                f"Attraction weight: {destination['attraction_weight']:.2f}<br>"
                f"Source: {html.escape(destination['data_source'])}"
            )
            folium.CircleMarker(
                location=[destination["point"].lat, destination["point"].lng], radius=7,
                color="#ffffff", weight=2, fill=True, fill_color="#10b981", fill_opacity=0.95,
                popup=folium.Popup(popup_html, max_width=360), tooltip=destination["name"],
            ).add_to(destination_layer)
        destination_layer.add_to(map_object)

        # Two route layers: pass-through (red) and other (blue). Only real walking routes are drawn.
        pass_through_layer = folium.FeatureGroup(name="Pass-through routes", show=True)
        other_routes_layer = folium.FeatureGroup(name="Other routes", show=False)

        maximum_pair_score = max((p["pair_flow_score"] for p in pairs), default=1.0)
        for pair in pairs:
            coordinates = pair["route_coordinates"]
            if len(coordinates) < 2:
                continue
            normalized_score = pair["pair_flow_score"] / maximum_pair_score if maximum_pair_score else 0.0
            line_weight = 2.5 + 4.5 * math.sqrt(max(0.0, normalized_score))
            popup_html = (
                f"<b>{html.escape(pair['source'])}</b> &rarr; <b>{html.escape(pair['destination'])}</b><br>"
                f"Walking distance: {pair['walk_m']:.0f} m<br>"
                f"Walking time: {pair['walk_time_min']:.1f} min<br>"
                f"Route method: {html.escape(pair['route_method'])}<br>"
                f"Flow-potential score: {pair['pair_flow_score']:.3f}<br>"
                f"Passes target: {'Yes' if pair['passes_target'] else 'No'}"
            )
            popup = folium.Popup(popup_html, max_width=420)
            if pair["passes_target"]:
                folium.PolyLine(coordinates, color="#ff2d55", weight=line_weight, opacity=0.90,
                                popup=popup).add_to(pass_through_layer)
            else:
                folium.PolyLine(coordinates, color="#38bdf8", weight=max(2.0, line_weight - 1),
                                opacity=0.70, popup=popup).add_to(other_routes_layer)
        pass_through_layer.add_to(map_object)
        other_routes_layer.add_to(map_object)

        # Overlay the statistics card, add a layer switcher, and save the HTML file.
        map_object.get_root().html.add_child(Element(self.statistics_panel(summary)))
        folium.LayerControl(collapsed=False, position="bottomleft").add_to(map_object)
        map_object.get_root().header.add_child(Element(f"<title>{html.escape(APP_NAME)}</title>"))

        map_path = self.config.output_dir / "latest_radial_flow_map.html"
        map_object.save(str(map_path))
        return map_path

    # Export sources, destinations, pairs (without geometry), time profile and summary.
    def write_outputs(self, sources: list[dict[str, Any]], destinations: list[dict[str, Any]],
                      pairs: list[dict[str, Any]], profile: list[dict[str, Any]],
                      summary: dict[str, Any]) -> None:
        source_rows = [{
            "stop_id": s["stop_id"], "name": s["name"],
            "latitude": s["point"].lat, "longitude": s["point"].lng,
            "straight_m": s["straight_m"],
            "scheduled_departures": s["scheduled_departures"],
            "active_routes": s["active_routes"],
            "route_names": " | ".join(s["route_names"]),
            "operators": " | ".join(s["operators"]),
            "supply_score": s["supply_score"],
        } for s in sources]

        destination_rows = [{
            "id": d["id"], "name": d["name"],
            "latitude": d["point"].lat, "longitude": d["point"].lng,
            "category": d["category"], "straight_m": d["straight_m"],
            "attraction_weight": d["attraction_weight"], "data_source": d["data_source"],
        } for d in destinations]

        pair_rows = [{k: v for k, v in p.items() if k != "route_coordinates"} for p in pairs]

        out = self.config.output_dir
        write_csv(out / "latest_sources.csv", source_rows)
        write_csv(out / "latest_destinations.csv", destination_rows)
        write_csv(out / "latest_flow_pairs.csv", pair_rows)
        write_csv(out / "latest_time_profile.csv", profile)
        write_csv(out / "latest_failed_routes.csv", list(self.failed_routes))
        (out / "latest_summary.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    # Orchestrate the full pipeline: sources -> destinations -> parallel routing -> summary -> files -> map.
    def run(self) -> dict[str, Any]:
        self.log(f"Starting {APP_NAME} analysis...")
        self.log(f"Period: {self.config.start_datetime:%Y-%m-%d %H:%M} to "
                 f"{self.config.end_datetime:%Y-%m-%d %H:%M}")

        sources, selected_departures = self.load_sources()
        if not sources:
            raise AppError("No active public transport source stops "
                           "were found for this location and time period.")

        destinations, osm_success = self.load_destinations()
        pairs = self.build_pairs(sources, destinations)
        profile = self.build_time_profile(selected_departures)
        summary = self.build_summary(sources, destinations, pairs, profile, osm_success)
        self.write_outputs(sources, destinations, pairs, profile, summary)
        map_path = self.build_map(sources, destinations, pairs, summary)

        self.log(f"Routing success: {summary['routing_api_success_rate'] * 100:.1f}%")
        self.log(f"Map saved: {map_path}")
        self.log("Analysis completed.")
        return {"summary": summary, "map_path": map_path}


# ---------------------------------------------------------------------------
# Runtime helpers and command-line interface
# ---------------------------------------------------------------------------
# Detect Jupyter/Colab so the program opens the GUI instead of reading sys.argv.
def running_in_notebook() -> bool:
    return "ipykernel" in sys.modules or "google.colab" in sys.modules


# Parse CLI arguments, build a Config, run the analysis and print a short JSON result.
def run_cli(arguments: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=f"{APP_NAME}: Hong Kong source-to-destination footfall-potential explorer")
    parser.add_argument("--location", required=True, help="Latitude,longitude or a Google Maps URL")
    parser.add_argument("--label", default="Subject location")
    parser.add_argument("--start-time", required=True, help="YYYY-MM-DD HH:MM")
    parser.add_argument("--end-time", required=True, help="YYYY-MM-DD HH:MM")
    parser.add_argument("--radius", type=float, default=600.0)
    parser.add_argument("--tolerance", type=float, default=60.0)
    parser.add_argument("--max-sources", type=int, default=6)
    parser.add_argument("--max-destinations", type=int, default=8)
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                        help=f"ThreadPoolExecutor workers ({MIN_WORKERS}-{MAX_WORKERS}, default {DEFAULT_WORKERS})")
    parser.add_argument("--no-osm", action="store_true")
    parser.add_argument("--include-residential", action="store_true")
    parser.add_argument("--output-dir", default=str(Path.cwd() / "output"))

    parsed = parser.parse_args(arguments)
    client = HttpClient()
    subject = parse_location(parsed.location, client)

    config = Config(
        subject=subject,
        subject_label=parsed.label,
        start_datetime=parse_datetime_hkt(parsed.start_time),
        end_datetime=parse_datetime_hkt(parsed.end_time),
        radius_m=parsed.radius,
        target_tolerance_m=parsed.tolerance,
        max_sources=parsed.max_sources,
        max_destinations=parsed.max_destinations,
        include_osm_optional=not parsed.no_osm,
        include_residential_optional=parsed.include_residential,
        output_dir=Path(parsed.output_dir),
        cache_dir=Path(parsed.output_dir).parent / ".radial_flow_cache",
        workers=parsed.workers,
    )

    result = FootfallAnalyzer(config).run()
    print(json.dumps({
        "project": APP_NAME,
        "map_path": str(result["map_path"]),
        "routing_success": result["summary"]["routing_api_success_rate"],
        "data_quality": result["summary"]["data_quality"],
        "parallel_workers": result["summary"]["parallel_workers"],
    }, ensure_ascii=False, indent=2))
    return 0


# ---------------------------------------------------------------------------
# Desktop GUI (Tkinter). The analysis runs in a ThreadPoolExecutor(max_workers=1)
# so the window stays responsive; no threading.Thread is used anywhere.
# ---------------------------------------------------------------------------
def launch_gui() -> None:
    try:
        import tkinter as tk
        from tkinter import filedialog, messagebox, ttk
    except ImportError as exc:
        raise AppError("Tkinter is required for GUI mode.") from exc

    # Main window with the new project name.
    root = tk.Tk()
    root.title(APP_NAME)
    root.geometry("900x900")
    root.minsize(760, 740)

    style = ttk.Style(root)
    try:
        style.theme_use("vista")
    except tk.TclError:
        pass

    main_frame = ttk.Frame(root, padding=18)
    main_frame.pack(fill="both", expand=True)

    # Title and subtitle.
    ttk.Label(main_frame, text=APP_NAME, font=("Segoe UI", 16, "bold")).pack(anchor="w", pady=(0, 4))
    ttk.Label(main_frame, text="Estimate relative pedestrian-flow potential "
                               "for a selected location and time period."
              ).pack(anchor="w", pady=(0, 14))

    form_frame = ttk.Frame(main_frame)
    form_frame.pack(fill="x", anchor="n")
    form_frame.columnconfigure(1, weight=1)

    variables: dict[str, tk.StringVar] = {}

    # Helper that adds one "label | entry | hint" row to the form grid.
    def add_field(row_number: int, label_text: str, key: str,
                  default_value: str = "", help_text: str = "") -> None:
        ttk.Label(form_frame, text=label_text).grid(
            row=row_number, column=0, sticky="w", padx=(0, 12), pady=5)
        variable = tk.StringVar(value=default_value)
        variables[key] = variable
        ttk.Entry(form_frame, textvariable=variable, width=68).grid(
            row=row_number, column=1, sticky="ew", pady=5)
        ttk.Label(form_frame, text=help_text, foreground="#666666").grid(
            row=row_number, column=2, sticky="w", padx=(12, 0), pady=5)

    # Default window: today 08:00 to 10:00 HKT.
    today = datetime.now(HK_TZ).date()
    default_start = datetime.combine(today, datetime.strptime("08:00", "%H:%M").time(), tzinfo=HK_TZ)
    default_end = default_start + timedelta(hours=2)

    add_field(3, "Location", "location", "22.3193,114.1694", "Coordinates or Google Maps link")
    add_field(4, "Location label", "label", "Subject location", "")
    add_field(5, "Start time", "start_time", default_start.strftime("%Y-%m-%d %H:%M"), "YYYY-MM-DD HH:MM")
    add_field(6, "End time", "end_time", default_end.strftime("%Y-%m-%d %H:%M"), "YYYY-MM-DD HH:MM")
    add_field(7, "Analysis radius (m)", "radius", "600", "Recommended 300 to 1000")
    add_field(8, "Route tolerance (m)", "tolerance", "60", "Pass-through distance")
    add_field(9, "Maximum source stops", "max_sources", "6", "Recommended 4 to 10")
    add_field(10, "Maximum destinations", "max_destinations", "8", "Recommended 5 to 15")
    add_field(11, "Parallel workers", "workers", str(DEFAULT_WORKERS),
              f"ThreadPoolExecutor {MIN_WORKERS}-{MAX_WORKERS}")

    # Optional data toggles.
    osm_variable = tk.BooleanVar(value=True)
    residential_variable = tk.BooleanVar(value=False)
    ttk.Checkbutton(form_frame, text="Load optional OpenStreetMap destination data",
                    variable=osm_variable).grid(row=12, column=1, sticky="w", pady=(8, 4))
    ttk.Checkbutton(form_frame, text="Include optional residential destinations",
                    variable=residential_variable).grid(row=13, column=1, sticky="w", pady=4)

    # Output folder picker.
    output_variable = tk.StringVar(value=str(Path.cwd() / "output"))
    ttk.Label(form_frame, text="Output folder").grid(
        row=14, column=0, sticky="w", padx=(0, 12), pady=(12, 5))
    ttk.Entry(form_frame, textvariable=output_variable, width=58).grid(
        row=14, column=1, sticky="ew", pady=(12, 5))

    def browse_output() -> None:
        selected = filedialog.askdirectory(initialdir=output_variable.get() or str(Path.cwd()))
        if selected:
            output_variable.set(selected)

    ttk.Button(form_frame, text="Browse", command=browse_output).grid(
        row=14, column=2, sticky="w", padx=(12, 0), pady=(12, 5))

    # Button row, status line and scrolling log area.
    button_frame = ttk.Frame(main_frame)
    button_frame.pack(fill="x", pady=(14, 8))

    progress_variable = tk.StringVar(value="Ready.")
    ttk.Label(main_frame, textvariable=progress_variable, foreground="#334155").pack(anchor="w", pady=(0, 4))

    log_frame = ttk.Frame(main_frame)
    log_frame.pack(fill="both", expand=True)
    log_text = tk.Text(log_frame, height=15, wrap="word", font=("Consolas", 9))
    log_scrollbar = ttk.Scrollbar(log_frame, orient="vertical", command=log_text.yview)
    log_text.configure(yscrollcommand=log_scrollbar.set)
    log_text.pack(side="left", fill="both", expand=True)
    log_scrollbar.pack(side="right", fill="y")

    run_button: Optional[ttk.Button] = None
    last_map_path: dict[str, Optional[Path]] = {"value": None}

    # Single-worker pool that runs the whole analysis off the GUI thread.
    gui_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="radial-flow-gui")

    # Thread-safe log: schedule the Tk update on the main loop with root.after.
    def append_log(message: str) -> None:
        def update() -> None:
            timestamp = datetime.now(HK_TZ).strftime("%H:%M:%S")
            log_text.insert("end", f"[{timestamp}] {message}\n")
            log_text.see("end")
            progress_variable.set(message)
        root.after(0, update)

    # Enable/disable the Run button while an analysis is in progress.
    def set_running(running: bool) -> None:
        def update() -> None:
            if run_button is not None:
                run_button.configure(state="disabled" if running else "normal")
        root.after(0, update)

    # Open the most recent map in the default browser.
    def open_result_map() -> None:
        map_path = last_map_path["value"]
        if map_path is None or not map_path.exists():
            messagebox.showinfo("No result", "Run an analysis first.")
            return
        webbrowser.open(map_path.resolve().as_uri())

    # Open the output folder in the OS file manager.
    def open_output_folder() -> None:
        output_path = Path(output_variable.get()).expanduser()
        output_path.mkdir(parents=True, exist_ok=True)
        try:
            if sys.platform.startswith("win"):
                import os
                os.startfile(str(output_path.resolve()))
            elif sys.platform == "darwin":
                import subprocess
                subprocess.Popen(["open", str(output_path.resolve())])
            else:
                import subprocess
                subprocess.Popen(["xdg-open", str(output_path.resolve())])
        except Exception as exc:
            messagebox.showerror("Could not open folder", str(exc))

    # Background job: read the form, build Config, run, then report back through root.after.
    def worker() -> None:
        try:
            output_path = Path(output_variable.get()).expanduser()
            client = HttpClient()
            subject = parse_location(variables["location"].get(), client)
            config = Config(
                subject=subject,
                subject_label=variables["label"].get().strip() or "Subject location",
                start_datetime=parse_datetime_hkt(variables["start_time"].get()),
                end_datetime=parse_datetime_hkt(variables["end_time"].get()),
                radius_m=float(variables["radius"].get()),
                target_tolerance_m=float(variables["tolerance"].get()),
                max_sources=int(variables["max_sources"].get()),
                max_destinations=int(variables["max_destinations"].get()),
                include_osm_optional=osm_variable.get(),
                include_residential_optional=residential_variable.get(),
                output_dir=output_path,
                cache_dir=output_path.parent / ".radial_flow_cache",
                workers=int(variables["workers"].get() or DEFAULT_WORKERS),
            )
            result = FootfallAnalyzer(config, logger=append_log).run()
            last_map_path["value"] = Path(result["map_path"])
            summary = result["summary"]

            root.after(0, lambda: webbrowser.open(Path(result["map_path"]).resolve().as_uri()))
            root.after(0, lambda: messagebox.showinfo(
                "Analysis completed",
                "Analysis completed successfully.\n\n"
                "Base map: Esri World Imagery\n"
                f"Routing success: {summary['routing_api_success_rate'] * 100:.1f}%\n"
                f"Data quality: {summary['data_quality']}\n\n"
                f"Map:\n{result['map_path']}"))
        except Exception as exc:
            append_log(f"ERROR: {exc}")
            root.after(0, lambda error=str(exc): messagebox.showerror("Analysis failed", error))
        finally:
            set_running(False)

    # Button handler: clear the log and submit the worker to the executor.
    def run_analysis() -> None:
        set_running(True)
        log_text.delete("1.0", "end")
        append_log(f"Starting {APP_NAME} analysis...")
        gui_executor.submit(worker)

    # Action buttons.
    run_button = ttk.Button(button_frame, text="Run analysis", command=run_analysis)
    run_button.pack(side="left", fill="x", expand=True, padx=(0, 6))
    ttk.Button(button_frame, text="Open result map", command=open_result_map).pack(side="left", padx=6)
    ttk.Button(button_frame, text="Open output folder", command=open_output_folder).pack(side="left", padx=(6, 0))

    # Shut the executor down cleanly when the window is closed.
    def on_close() -> None:
        gui_executor.shutdown(wait=False, cancel_futures=True)
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_close)
    root.mainloop()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
# Notebook or no arguments -> GUI; otherwise -> command-line mode.
def main() -> int:
    if running_in_notebook():
        launch_gui()
        return 0
    if len(sys.argv) == 1:
        launch_gui()
        return 0
    return run_cli()


# Script guard: friendly error output and non-zero exit code on AppError.
if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AppError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(1)
