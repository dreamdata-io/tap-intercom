#!/usr/bin/env python3
import singer
from singer import utils
from tap_intercom.stream import Stream
from typing import Optional, Dict
from tap_intercom.stream import InvalidCredentialsError
import sys

REQUIRED_CONFIG_KEYS = [
    "start_date",
    "access_token",
]
LOGGER = singer.get_logger()
STREAMS = ["companies", "contacts", "conversations"]


def sync(config: Dict, state: Optional[Dict] = None):
    stream = Stream(config)

    for tap_stream_id in STREAMS:
        LOGGER.info(f"syncing {tap_stream_id}")
        stream.do_sync(tap_stream_id=tap_stream_id, state=state)


@utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    try:
        sync(args.config, args.state)
    except InvalidCredentialsError as e:
        LOGGER.error(f"Invalid Credentials Error. error: {str(e)}")
        sys.exit(5)


if __name__ == "__main__":
    main()
