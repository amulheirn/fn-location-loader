# fn-location-loader and fn-device-loader

## Overview
Two scripts are provided:
- `fn-location-loader.py`: geocodes addresses (via OpenStreetMap) and creates locations in Forward Enterprise.
- `fn-device-loader.py`: maps devices to the locations, and optionally applies device tags.

Both scripts require `.env` values: `FORWARD_API_BASE_URL`, `NETWORK_ID`, `API_KEY_ID`, `API_SECRET`. Optional: `DRY_RUN`, `LOG_LEVEL`.

## Setup
1. Clone this repository.
2. Copy `.env_example` to `.env`.
3. Fill in your network ID, API key, and secret. 
4. If you are using Forward Enterprise on-prem, change the base URL from fwd.app to your URL.

## Using the location loader
1. Prepare `addresses.csv` (see `addresses-example.csv` for columns: `id,name,address,lat,lng`).  Lat and long are optional values but the field must be present even if left empty.
2. Run: `python fn-location-loader.py addresses.csv`

The script will look up the address in OpenStreetmap and geocode it to a lat/long.  Then it will create the location with those coordinates in Forward's API.

You can do a dry run if you want to check this. It writes a file called `locations_payload.json`).  Add the flag `--dry-run` when running the command.

Increase verbosity using the log-level flag: `--log-level DEBUG`

Notes: The script will not create a location if one of the same name already exists.

## Using the device loader
1. Prepare `devices.csv` (see `devices-example.csv`) with columns:
   - Required: `device`, `location`
   - Optional: `tag` (single tag value which must be letters, numbers, `_`, `-`)
2. Run: `python fn-device-loader.py devices.csv`

Dry-run and debug log levels can be enabled as in the fn-location-loader 

Behaviour:
- Looks up all locations via `/api/networks/{NETWORK_ID}/locations` and matches by name (case-insensitive).
- If any tag values are present, fetches existing tags via `/api/networks/{NETWORK_ID}/device-tags` and errors when a tag is missing.
- Updates device locations via PATCH `/api/networks/{NETWORK_ID}/atlas`.
- Adds device tags via POST `/api/networks/{NETWORK_ID}/device-tags?action=addBatchTo`.

Color logging:
- Errors are printed in orange.
- Successful location PATCH lines and the final success line are printed in green.


## Note
This script is provided under the MIT license.  See `LICENSE` for details.

