# FN Loation Loader

## Overview
This python script allows you to create a lot of new locations in Forward Enterprise using the API, saving quite a lot of time and effort.

It geo-codes the lat/long using the OpenStreetmap API, then creates the location in Forward Enterprise for you.

## How to use

Clone this repository

Copy .env_example to a new file called .env

Enter your network ID, API Key and API Secret in the file.

Create your addresses in an addresses.csv file (see the example for the fields needed)

Run the script using `fn-location-locater.py addresses.csv`

## Debugging

If things are not working as you expect you can turn on a higher logging level using:

`fn-location-locater.py --log-level DEBUG addresses.csv`

Alternatively, do a dry-run and look at the locations_payload.json file that is created.

`fn-location-locater.py --dry-run addresses.csv`

## Notes
This software is supplied as-is and with no warranty or support.

The software will not create a location if one of the same name still exists.
