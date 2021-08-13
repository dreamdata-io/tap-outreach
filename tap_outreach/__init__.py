#!/usr/bin/env python3

from datetime import datetime
import sys
import json
import argparse
import pytz

import singer
from singer import metadata

from tap_outreach.client import OutreachClient
from tap_outreach.discover import discover
from tap_outreach.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    "start_date",
    "client_id",
    "client_secret",
    "redirect_uri",
    "refresh_token",
]


def do_discover(client):
    LOGGER.info("Testing authentication")
    try:
        client.get("stages", endpoint="stages")
    except KeyboardInterrupt:
        raise
    except:
        raise Exception("Error testing Outreach authentication")

    LOGGER.info("Starting discover")
    catalog = discover()
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info("Finished discover")


@singer.utils.handle_top_exception(LOGGER)
def main():
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    with OutreachClient(parsed_args.config) as client:
        if parsed_args.discover:
            do_discover(client)
        else:
            # This is a compatability conversion to fix an issue with how we
            # generate config files for taps.
            # Basically, we store a "UTC timestamp" called `tapped_at` in
            # datastore, which is in the (assumed) default Python format
            # instead of the much more common ISO 8601 format.
            #
            # This issue only occurs when the tap is not run with a state file,
            # since state file bookmarks are actually in the correct ISO 8601
            # format due to the tap actually writing results from the API as
            # state. I'm guessing this is why no one has experienced this bug.
            #
            # This might very well break, but in that case at least it's going
            # to blow up so we can actually detect it.
            start_date = (
                datetime.strptime(
                    parsed_args.config["start_date"],
                    "%Y-%m-%dT%H:%M:%S.%f",
                )
                .astimezone(pytz.UTC)
                .strftime("%Y-%m-%dT%H:%M:%SZ")
            )

            sync(
                client,
                parsed_args.catalog,
                parsed_args.state,
                start_date,
            )
