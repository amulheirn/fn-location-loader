#!/usr/bin/env python3
import os
import sys
import csv
import json
import time
import logging
import argparse
from typing import List, Dict, Optional, Tuple

import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# ==== CONFIGURATION (from .env) ==============================================

FORWARD_API_BASE_URL = os.getenv("FORWARD_API_BASE_URL")
FORWARD_URL = os.getenv("FORWARD_URL")
NETWORK_ID = os.getenv("NETWORK_ID")

API_KEY_ID = os.getenv("API_KEY_ID")
API_SECRET = os.getenv("API_SECRET")

ENV_DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
ENV_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

if not all([FORWARD_API_BASE_URL, FORWARD_URL, NETWORK_ID, API_KEY_ID, API_SECRET]):
    print("ERROR: One or more required .env variables are missing.", file=sys.stderr)
    sys.exit(1)

FORWARD_POST_URL = f"{FORWARD_API_BASE_URL}/networks/{NETWORK_ID}/locations"

# Nominatim endpoint
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

# Geocoding behaviour / retry settings
GEOCODE_DELAY_SECONDS = 1.0          # normal delay between requests
GEOCODE_MAX_RETRIES = 3
GEOCODE_RETRY_BACKOFF = 2.0          # multiplier for exponential backoff

# POST retry settings
POST_MAX_RETRIES = 3
POST_RETRY_BACKOFF = 2.0

# Default JSON output filename in dry run
DRY_RUN_OUTPUT_FILE = "locations_payload.json"

logger = logging.getLogger(__name__)


# ==== FUNCTIONS =============================================================

def setup_logging(level_name: str) -> None:
    """Configure root logger."""
    numeric_level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger.info("Logging initialized at level %s", level_name)


def geocode_address(address: str) -> Tuple[float, float]:
    """
    Geocode an address string using OpenStreetMap Nominatim with retries.
    Returns (lat, lng) as floats.
    Raises ValueError if no result after retries.
    """
    params = {
        "q": address,
        "format": "json",
        "addressdetails": 1,
        "limit": 1,
    }
    headers = {
        "User-Agent": "forward-networks-location-loader/1.0",
    }

    attempt = 1
    delay = GEOCODE_DELAY_SECONDS

    while attempt <= GEOCODE_MAX_RETRIES:
        try:
            logger.debug("Geocoding attempt %d for address: %s", attempt, address)
            resp = requests.get(NOMINATIM_URL, params=params, headers=headers, timeout=10)
            resp.raise_for_status()

            data = resp.json()
            if not data:
                raise ValueError(f"No geocoding result found for address: {address}")

            first = data[0]
            lat = float(first["lat"])
            lng = float(first["lon"])
            logger.debug("Geocoding successful for '%s': lat=%s, lng=%s", address, lat, lng)
            return lat, lng

        except Exception as e:
            logger.warning(
                "Geocoding failed (attempt %d/%d) for '%s': %s",
                attempt,
                GEOCODE_MAX_RETRIES,
                address,
                e,
            )
            if attempt == GEOCODE_MAX_RETRIES:
                # Final failure
                raise

            # Exponential backoff
            time.sleep(delay)
            delay *= GEOCODE_RETRY_BACKOFF
            attempt += 1

    # Should never reach here
    raise ValueError(f"Geocoding failed after {GEOCODE_MAX_RETRIES} attempts for address: {address}")


def load_locations_from_csv(csv_path: str) -> List[Dict]:
    """
    Load locations from a CSV file.

    Required columns:
      - id
      - name
      - address

    Optional columns:
      - lat
      - lng

    Returns list of dicts:
      {
        "id": str,
        "name": str,
        "address": str,
        "lat": Optional[float],
        "lng": Optional[float],
      }
    """
    locations: List[Dict] = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required_cols = {"id", "name", "address"}
        missing = required_cols - set(reader.fieldnames or [])
        if missing:
            raise ValueError(
                f"CSV missing required columns: {', '.join(sorted(missing))} "
                f"(found: {reader.fieldnames})"
            )

        has_lat = "lat" in reader.fieldnames
        has_lng = "lng" in reader.fieldnames

        for row in reader:
            loc_id = (row.get("id") or "").strip()
            name = (row.get("name") or "").strip()
            address = (row.get("address") or "").strip()

            if not loc_id or not name or not address:
                logger.warning("Skipping incomplete row (missing id/name/address): %s", row)
                continue

            lat: Optional[float] = None
            lng: Optional[float] = None

            if has_lat:
                lat_val = (row.get("lat") or "").strip()
                if lat_val:
                    try:
                        lat = float(lat_val)
                    except ValueError:
                        logger.warning("Invalid lat '%s' for id=%s, ignoring", lat_val, loc_id)

            if has_lng:
                lng_val = (row.get("lng") or "").strip()
                if lng_val:
                    try:
                        lng = float(lng_val)
                    except ValueError:
                        logger.warning("Invalid lng '%s' for id=%s, ignoring", lng_val, loc_id)

            locations.append(
                {
                    "id": loc_id,
                    "name": name,
                    "address": address,
                    "lat": lat,
                    "lng": lng,
                }
            )

    logger.info("Loaded %d location(s) from CSV", len(locations))
    return locations


def geocode_locations(locations: List[Dict]) -> List[Dict]:
    """
    For each location:
      - If lat & lng already present, use them and skip geocoding.
      - Otherwise, geocode the address.

    Returns list of:
      {id, name, lat, lng}
    """
    results: List[Dict] = []

    for i, loc in enumerate(locations, start=1):
        loc_id = loc["id"]
        name = loc["name"]
        address = loc["address"]
        lat = loc.get("lat")
        lng = loc.get("lng")

        if lat is not None and lng is not None:
            logger.info(
                "[%d/%d] Using existing lat/lng for '%s' (%s): lat=%s, lng=%s",
                i, len(locations), name, loc_id, lat, lng
            )
            results.append(
                {
                    "id": loc_id,
                    "name": name,
                    "lat": lat,
                    "lng": lng,
                }
            )
            # No delay needed for non-geocoded locations
            continue

        logger.info(
            "[%d/%d] Geocoding address for '%s' (%s): %s",
            i, len(locations), name, loc_id, address
        )

        try:
            lat, lng = geocode_address(address)
            logger.info(" -> lat=%s, lng=%s", lat, lng)

            results.append(
                {
                    "id": loc_id,
                    "name": name,
                    "lat": lat,
                    "lng": lng,
                }
            )

        except Exception as e:
            logger.error("FAILED geocoding '%s' (%s): %s", name, address, e)

        # Be nice to Nominatim (base delay between geocoded addresses)
        time.sleep(GEOCODE_DELAY_SECONDS)

    logger.info(
        "Successfully prepared coordinates for %d of %d location(s)",
        len(results),
        len(locations),
    )
    return results


def post_single_location_to_forward(location: Dict) -> None:
    """
    POST a single location object to Forward Networks using Basic Auth,
    with retries. Expected JSON shape:
      { "id": "...", "name": "...", "lat": 12.34, "lng": 56.78 }
    """
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    auth = HTTPBasicAuth(API_KEY_ID, API_SECRET)

    attempt = 1
    delay = 1.0

    while attempt <= POST_MAX_RETRIES:
        try:
            logger.info(
                "POST attempt %d/%d to %s for location id=%s",
                attempt,
                POST_MAX_RETRIES,
                FORWARD_POST_URL,
                location.get("id"),
            )

            resp = requests.post(
                FORWARD_POST_URL,
                headers=headers,
                json=location,
                auth=auth,
                timeout=30,
            )

            # Client errors — log & abort further retry for this location
            if 400 <= resp.status_code < 500:
                logger.error(
                    "Client error %d for location id=%s\nResponse: %s",
                    resp.status_code,
                    location.get("id"),
                    resp.text,
                )
                resp.raise_for_status()

            # Retry on server errors or rate limiting
            if resp.status_code >= 500 or resp.status_code == 429:
                logger.warning(
                    "Server error %d for id=%s (attempt %d/%d): %s",
                    resp.status_code,
                    location.get("id"),
                    attempt,
                    POST_MAX_RETRIES,
                    resp.text,
                )
                if attempt == POST_MAX_RETRIES:
                    resp.raise_for_status()
                time.sleep(delay)
                delay *= POST_RETRY_BACKOFF
                attempt += 1
                continue

            resp.raise_for_status()

            logger.info(
                "POST succeeded for id=%s (status=%d)",
                location.get("id"),
                resp.status_code,
            )
            return

        except requests.HTTPError as e:
            logger.exception("HTTP error for location id=%s: %s", location.get("id"), e)
            raise
        except requests.RequestException as e:
            logger.warning(
                "POST network error for id=%s (attempt %d/%d): %s",
                location.get("id"),
                attempt,
                POST_MAX_RETRIES,
                e,
            )
            if attempt == POST_MAX_RETRIES:
                raise
            time.sleep(delay)
            delay *= POST_RETRY_BACKOFF
            attempt += 1


def write_payload_to_file(payload: List[Dict], filename: str) -> None:
    """
    Write the payload to a JSON file.
    """
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    logger.info("Dry run: payload written to %s", filename)


# ==== MAIN ===================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load locations from CSV, geocode (if needed), and send to Forward Networks."
    )
    parser.add_argument("csv_path", help="Path to locations CSV file")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Prepare payload (using existing lat/lng or geocoding) and write to JSON, but do NOT POST",
    )
    parser.add_argument(
        "--log-level",
        default=ENV_LOG_LEVEL,
        help=f"Logging level (DEBUG, INFO, WARNING, ERROR). Default from LOG_LEVEL env ({ENV_LOG_LEVEL})",
    )
    parser.add_argument(
        "--dry-run-output",
        default=DRY_RUN_OUTPUT_FILE,
        help=f"Output JSON filename in dry run (default: {DRY_RUN_OUTPUT_FILE})",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    setup_logging(args.log_level)

    dry_run = ENV_DRY_RUN or args.dry_run
    if dry_run:
        logger.info("Dry run mode ENABLED (no POST will be performed)")

    csv_path = args.csv_path

    try:
        logger.info("Loading CSV: %s", csv_path)
        locs_raw = load_locations_from_csv(csv_path)

        if not locs_raw:
            logger.error("No valid locations found in CSV. Exiting.")
            sys.exit(1)

        locs_with_coords = geocode_locations(locs_raw)

        if not locs_with_coords:
            logger.error("No locations successfully prepared with coordinates. Exiting.")
            sys.exit(1)

        payload_json = json.dumps(locs_with_coords, indent=2)
        logger.info("Final JSON payload:\n%s", payload_json)

        if dry_run:
            write_payload_to_file(locs_with_coords, args.dry_run_output)
            logger.info("Dry run complete. Skipping POST to Forward Networks.")
            return

        logger.info(
            "Posting %d location(s) individually to %s",
            len(locs_with_coords),
            FORWARD_POST_URL,
        )

        for loc in locs_with_coords:
            try:
                post_single_location_to_forward(loc)
            except Exception as e:
                logger.error("Failed to POST location id=%s: %s", loc.get("id"), e)
                # Continue to next location

        logger.info("All POST attempts completed.")

    except Exception as e:
        logger.exception("Fatal error: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
