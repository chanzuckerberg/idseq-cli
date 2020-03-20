"""Module for handling location metadata and geosearching."""

import random
import requests
import threading
import time

# For Python2 compatibility
from builtins import input

from . import constants

MAX_GEOSEARCH_ATTEMPTS = 3
MAX_GEOSEARCH_THREADS = 5
COLLECTION_LOCATION_ALIASES = [
    "collection location",
    "Collection Location",
    "Collection location",
    "collection_location",
]

def geosearch_and_set_csv_locations(base_url, headers, csv_data, project_id, accept_all):
    """Automatically geosearch CSV collection locations for matches."""
    raw_names = get_raw_locations(csv_data)

    matched_locations = fetch_location_matches(raw_names, base_url, headers)
    if len(matched_locations) > 0 and not accept_all:
        confirm_location_matches(matched_locations)

    set_location_matches(csv_data, matched_locations)
    print_location_matches(csv_data, base_url, project_id)
    return csv_data


def get_raw_locations(csv_data):
    raw_names = set()
    for metadata in csv_data.values():
        for field_name, value in metadata.items():
            if field_name.lower() in COLLECTION_LOCATION_ALIASES:
                raw_names.add(value)
    return raw_names


def fetch_location_matches(raw_names, base_url, headers):
    matched_locations = {}
    semaphore = threading.Semaphore(MAX_GEOSEARCH_THREADS)
    threads = []
    for query in raw_names:
        with semaphore:
            t = threading.Thread(
                target=get_geo_search_suggestion,
                args=[base_url, headers, query, matched_locations],
            )
            t.start()
            threads.append(t)
    for t in threads:
        t.join()
    return matched_locations


def confirm_location_matches(matched_locations):
    print("\nConfirm Your Collection Locations")
    print(
        "We automatically searched for location matches. Please double check and correct any "
        "errors. If you reject a match, it will be unresolved plain text and not shown on "
        "IDseq maps."
    )
    for raw_name in list(matched_locations.keys()):
        result = matched_locations[raw_name]["name"]
        if raw_name != result:
            print('\nWe matched "{}" to "{}"'.format(raw_name, result))
            resp = input("Is this correct (y/N)? y for yes or N to reject the match: ")
            if resp.lower() not in ["y", "yes"]:
                del matched_locations[raw_name]


def set_location_matches(csv_data, matched_locations):
    for sample_name, metadata in csv_data.items():
        for field_name, value in metadata.items():
            if field_name.lower() in COLLECTION_LOCATION_ALIASES:
                if value in matched_locations:
                    result = matched_locations[value]
                    is_human = any(
                        [metadata.get(n) == "Human" for n in constants.HOST_GENOME_ALIASES]
                    )
                    metadata[field_name] = process_location_selection(result, is_human)


def print_location_matches(csv_data, base_url, project_id):
    print("\n{:30} | Collection Location".format("Sample Name"))
    print("-" * 60)
    plain_text_found = False
    restricted_found = False
    for sample_name, metadata in csv_data.items():
        for field_name, result in metadata.items():
            if field_name.lower() in COLLECTION_LOCATION_ALIASES:
                value = result
                if type(value) is dict:
                    value = value["name"]
                    if result.get("restricted"):
                        value += " (!)"
                        restricted_found = True
                        result.pop("restricted")
                else:
                    value += " *"
                    plain_text_found = True
                print("{:30} | {}".format(sample_name, value))
    if plain_text_found:
        print("\n* Unresolved plain text location, not shown on maps.")
    if restricted_found:
        print("\n(!) Changed to county/district level for personal privacy.")
    print(
        "\nTo make additional changes after uploading, go to the project page: "
        "{}/my_data?projectId={} (and click Upload -> Upload Metadata)".format(
            base_url, project_id
        )
    )


def get_geo_search_suggestion(base_url, headers, query, matched_locations, attempt=0):
    """Get a geosearch location suggestion from the server."""
    url = "{}/locations/external_search?query={}&limit=1".format(base_url, query)
    resp = requests.get(url, headers=headers)

    if resp.status_code == 200:
        resp = resp.json()
        if len(resp) > 0:
            matched_locations[query] = resp[0]
    elif attempt < MAX_GEOSEARCH_ATTEMPTS:
        # Wait 1-2 seconds
        time.sleep(1 + random.random())
        get_geo_search_suggestion(
            base_url, headers, query, matched_locations, attempt + 1
        )
    else:
        print(
            "\nError finding location match for: '{}'. Location will be saved as plain text "
            "and not appear on IDseq maps.\n".format(query)
        )


def process_location_selection(result, is_human):
    if is_human and result.get("geo_level") == "city":
        # NOTE: The backend will redo the geosearch for confirmation and re-apply this restriction:
        # For human samples, drop the city part of the name and show a warning.
        # TODO(jsheu): Consider consolidating warnings to the backend.
        new_name = ", ".join(
            [
                result[n]
                for n in ["subdivision_name", "state_name", "country_name"]
                if n in result
            ]
        )
        result["name"] = new_name
        result["restricted"] = True
    return result
