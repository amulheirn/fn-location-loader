#!/usr/bin/env python3
import os
import sys
import csv
import time
import logging
import argparse
import re
from typing import Dict, List, Set

import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# ==== CONFIGURATION (from .env) ==============================================

FORWARD_API_BASE_URL = os.getenv("FORWARD_API_BASE_URL")
NETWORK_ID = os.getenv("NETWORK_ID")
API_KEY_ID = os.getenv("API_KEY_ID")
API_SECRET = os.getenv("API_SECRET")

ENV_DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
ENV_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

if not all([FORWARD_API_BASE_URL, NETWORK_ID, API_KEY_ID, API_SECRET]):
    print("ERROR: One or more required .env variables are missing.", file=sys.stderr)
    sys.exit(1)

LOCATIONS_URL = f"{FORWARD_API_BASE_URL}/networks/{NETWORK_ID}/locations"
ATLAS_URL = f"{FORWARD_API_BASE_URL}/networks/{NETWORK_ID}/atlas"
DEVICE_TAGS_URL = f"{FORWARD_API_BASE_URL}/networks/{NETWORK_ID}/device-tags?action=addBatchTo"
DEVICE_TAGS_LIST_URL = f"{FORWARD_API_BASE_URL}/networks/{NETWORK_ID}/device-tags"

# Retry settings
PATCH_MAX_RETRIES = 3
PATCH_RETRY_BACKOFF = 2.0

# Tag validation: letters, numbers, underscores, hyphens only
TAG_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")

logger = logging.getLogger(__name__)

ORANGE = "\033[33m"
GREEN = "\033[32m"
RESET = "\033[0m"


class ColorFormatter(logging.Formatter):
    """Add color to error-level log lines."""

    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        if record.levelno >= logging.ERROR:
            return f"{ORANGE}{base}{RESET}"
        return base


# ==== HELPERS ================================================================

def setup_logging(level_name: str) -> None:
    """Configure root logger."""
    numeric_level = getattr(logging, level_name.upper(), logging.INFO)
    formatter = ColorFormatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root = logging.getLogger()
    root.handlers.clear()
    handler = logging.StreamHandler()
    handler.setLevel(numeric_level)
    handler.setFormatter(formatter)
    root.setLevel(numeric_level)
    root.addHandler(handler)

    logger.info("Logging initialized at level %s", level_name)


def load_devices_from_csv(csv_path: str) -> List[Dict[str, str]]:
    """
    Load device -> location mappings from CSV.

    Required columns:
      - device
      - location

    Optional:
      - tag (single tag, letters/numbers/_/- only)
    """
    devices: List[Dict[str, str]] = []
    errors: List[str] = []

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required_cols = {"device", "location"}
        missing = required_cols - set(reader.fieldnames or [])
        if missing:
            raise ValueError(
                f"CSV missing required columns: {', '.join(sorted(missing))} "
                f"(found: {reader.fieldnames})"
            )

        has_tag_col = "tag" in (reader.fieldnames or [])

        for idx, row in enumerate(reader, start=2):  # account for header row
            device = (row.get("device") or "").strip()
            location_name = (row.get("location") or "").strip()
            tag = (row.get("tag") or "").strip() if has_tag_col else ""

            if not device or not location_name:
                errors.append(f"Row {idx}: missing device/location -> {row}")
                continue

            if tag and not TAG_PATTERN.fullmatch(tag):
                errors.append(f"Row {idx}: invalid tag '{tag}' (must be letters/numbers/_/-)")
                continue

            devices.append(
                {
                    "device": device,
                    "location": location_name,
                    "tag": tag,
                }
            )

    if errors:
        raise ValueError("CSV validation errors:\n" + "\n".join(errors))

    logger.info("Loaded %d device mapping(s) from CSV", len(devices))
    return devices


def fetch_location_lookup(auth: HTTPBasicAuth) -> Dict[str, str]:
    """Fetch locations and build name->id lookup (lowercase name key)."""
    headers = {"Accept": "application/json"}
    logger.info("Fetching locations from %s", LOCATIONS_URL)
    resp = requests.get(LOCATIONS_URL, headers=headers, auth=auth, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    if not isinstance(data, list):
        raise ValueError(f"Unexpected response shape from locations endpoint: {data}")

    name_to_id: Dict[str, str] = {}

    for loc in data:
        loc_id = str(loc.get("id", "")).strip()
        name = str(loc.get("name", "")).strip()
        if not loc_id or not name:
            continue

        key = name.lower()
        if key in name_to_id and name_to_id[key] != loc_id:
            logger.warning(
                "Duplicate location name detected: '%s' -> ids [%s, %s]",
                name,
                name_to_id[key],
                loc_id,
            )
        name_to_id[key] = loc_id

    if not name_to_id:
        raise ValueError("No locations returned from API; cannot continue.")

    logger.info("Discovered %d location(s) from API", len(name_to_id))
    return name_to_id


def fetch_existing_tags(auth: HTTPBasicAuth) -> Set[str]:
    """Fetch existing device tags; return a lowercase set of tag names."""
    headers = {"Accept": "application/json"}
    logger.info("Fetching existing device tags from %s", DEVICE_TAGS_LIST_URL)
    resp = requests.get(DEVICE_TAGS_LIST_URL, headers=headers, auth=auth, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    tags = data.get("tags") if isinstance(data, dict) else None
    if not isinstance(tags, list):
        raise ValueError(f"Unexpected response shape from device-tags endpoint: {data}")

    tag_set = {str(item.get("name", "")).strip().lower() for item in tags if item.get("name")}
    logger.info("Discovered %d tag(s) from API", len(tag_set))
    return tag_set


def patch_device_location(device: str, location_id: str, auth: HTTPBasicAuth, dry_run: bool) -> None:
    """PATCH device location on the atlas endpoint with retries."""
    payload = {device: location_id}
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    if dry_run:
        logger.info("[dry-run] Would PATCH %s with payload: %s", ATLAS_URL, payload)
        return

    attempt = 1
    delay = 1.0

    while attempt <= PATCH_MAX_RETRIES:
        try:
            logger.info(
                "PATCH attempt %d/%d for device=%s to location_id=%s",
                attempt,
                PATCH_MAX_RETRIES,
                device,
                location_id,
            )
            resp = requests.patch(
                ATLAS_URL,
                headers=headers,
                json=payload,
                auth=auth,
                timeout=30,
            )

            if 400 <= resp.status_code < 500:
                logger.error(
                    "Client error %d for device=%s\nResponse: %s",
                    resp.status_code,
                    device,
                    resp.text,
                )
                resp.raise_for_status()

            if resp.status_code >= 500 or resp.status_code == 429:
                logger.warning(
                    "Server error %d for device=%s (attempt %d/%d): %s",
                    resp.status_code,
                    device,
                    attempt,
                    PATCH_MAX_RETRIES,
                    resp.text,
                )
                if attempt == PATCH_MAX_RETRIES:
                    resp.raise_for_status()
                time.sleep(delay)
                delay *= PATCH_RETRY_BACKOFF
                attempt += 1
                continue

            resp.raise_for_status()
            logger.info(
                "%sLocation PATCH succeeded for device=%s (status=%d)%s",
                GREEN,
                device,
                resp.status_code,
                RESET,
            )
            return

        except requests.HTTPError as e:
            logger.exception("HTTP error while patching device=%s: %s", device, e)
            raise
        except requests.RequestException as e:
            logger.warning(
                "Network error for device=%s (attempt %d/%d): %s",
                device,
                attempt,
                PATCH_MAX_RETRIES,
                e,
            )
            if attempt == PATCH_MAX_RETRIES:
                raise
            time.sleep(delay)
            delay *= PATCH_RETRY_BACKOFF
            attempt += 1


def add_tag_to_device(device: str, tag: str, auth: HTTPBasicAuth, dry_run: bool) -> None:
    """Add a single tag to a device using the device-tags endpoint."""
    payload = {
        "devices": [device],
        "tags": [tag],
    }
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    if dry_run:
        logger.info("[dry-run] Would POST %s with payload: %s", DEVICE_TAGS_URL, payload)
        return

    attempt = 1
    delay = 1.0

    while attempt <= PATCH_MAX_RETRIES:
        try:
            logger.info(
                "Tag POST attempt %d/%d for device=%s tag=%s",
                attempt,
                PATCH_MAX_RETRIES,
                device,
                tag,
            )
            resp = requests.post(
                DEVICE_TAGS_URL,
                headers=headers,
                json=payload,
                auth=auth,
                timeout=30,
            )

            if 400 <= resp.status_code < 500:
                logger.error(
                    "Client error %d adding tag for device=%s\nResponse: %s",
                    resp.status_code,
                    device,
                    resp.text,
                )
                resp.raise_for_status()

            if resp.status_code >= 500 or resp.status_code == 429:
                logger.warning(
                    "Server error %d for device=%s tag=%s (attempt %d/%d): %s",
                    resp.status_code,
                    device,
                    tag,
                    attempt,
                    PATCH_MAX_RETRIES,
                    resp.text,
                )
                if attempt == PATCH_MAX_RETRIES:
                    resp.raise_for_status()
                time.sleep(delay)
                delay *= PATCH_RETRY_BACKOFF
                attempt += 1
                continue

            resp.raise_for_status()
            logger.info(
                "Tag POST succeeded for device=%s tag=%s (status=%d)",
                device,
                tag,
                resp.status_code,
            )
            return

        except requests.HTTPError as e:
            logger.exception("HTTP error while tagging device=%s: %s", device, e)
            raise
        except requests.RequestException as e:
            logger.warning(
                "Network error tagging device=%s (attempt %d/%d): %s",
                device,
                attempt,
                PATCH_MAX_RETRIES,
                e,
            )
            if attempt == PATCH_MAX_RETRIES:
                raise
            time.sleep(delay)
            delay *= PATCH_RETRY_BACKOFF
            attempt += 1


# ==== MAIN ===================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Load devices from CSV, resolve location IDs, and update device "
            "locations and tags via Forward Networks APIs."
        )
    )
    parser.add_argument("csv_path", help="Path to CSV file with device, location[, tag] columns")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse CSV and resolve locations but do NOT perform PATCH/POST requests",
    )
    parser.add_argument(
        "--log-level",
        default=ENV_LOG_LEVEL,
        help=f"Logging level (DEBUG, INFO, WARNING, ERROR). Default from LOG_LEVEL env ({ENV_LOG_LEVEL})",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    setup_logging(args.log_level)

    dry_run = ENV_DRY_RUN or args.dry_run
    if dry_run:
        logger.info("Dry run mode ENABLED (no PATCH/POST will be performed)")

    try:
        logger.info("Loading CSV: %s", args.csv_path)
        devices = load_devices_from_csv(args.csv_path)
    except Exception as e:
        logger.exception("Failed to load CSV: %s", e)
        sys.exit(1)

    auth = HTTPBasicAuth(API_KEY_ID, API_SECRET)

    try:
        name_to_id = fetch_location_lookup(auth)
    except Exception as e:
        logger.exception("Failed to fetch locations from API: %s", e)
        sys.exit(1)

    tags_in_csv: Set[str] = {entry["tag"] for entry in devices if entry.get("tag")}
    existing_tags: Set[str] = set()
    if tags_in_csv:
        try:
            existing_tags = fetch_existing_tags(auth)
        except Exception as e:
            logger.exception("Failed to fetch existing device tags: %s", e)
            sys.exit(1)

    failures = 0

    for entry in devices:
        device = entry["device"]
        location_name = entry["location"]
        tag = entry["tag"]

        if tag:
            if tag.lower() not in existing_tags:
                logger.error("Tag '%s' not found in Forward (device '%s')", tag, device)
                failures += 1
                continue

        location_id = name_to_id.get(location_name.lower())
        if not location_id:
            logger.error("No location found matching name '%s' for device '%s'", location_name, device)
            failures += 1
            continue

        logger.info(
            "Processing device '%s' -> location '%s' (id=%s) with tag '%s'",
            device,
            location_name,
            location_id,
            tag,
        )

        try:
            patch_device_location(device, location_id, auth, dry_run)
            if tag:
                add_tag_to_device(device, tag, auth, dry_run)
        except Exception as e:
            logger.error("Failed processing device '%s': %s", device, e)
            failures += 1

    if failures:
        logger.error("Completed with %d failure(s).", failures)
        sys.exit(1)

    logger.info("%sAll device updates completed successfully.%s", GREEN, RESET)


if __name__ == "__main__":
    main()
