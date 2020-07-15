#!/usr/bin/env python3
import json
import singer
from singer import utils
from tap_intercom.stream import Stream
from typing import Optional, Dict

REQUIRED_CONFIG_KEYS = [
    "start_date",
    "access_token",
]
LOGGER = singer.get_logger()
STREAMS = ["companies", "contacts", "conversations", "segments", "tags"]


def sync(config: Dict, state: Optional[Dict] = None):
    stream = Stream(config)

    for tap_stream_id in STREAMS:

        LOGGER.info(f"syncing {stream}")
        stream.do_sync(tap_stream_id=tap_stream_id, state=state)


@utils.handle_top_exception(LOGGER)
def main():

    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    sync(args.config, args.state)


if __name__ == "__main__":
    main()
