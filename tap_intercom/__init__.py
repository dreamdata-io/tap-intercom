#!/usr/bin/env python3
import json
import singer
from singer import utils, metadata, Catalog, CatalogEntry, Schema
from tap_intercom.stream import Stream
from pathlib import Path

KEY_PROPERTIES = "id"
VALID_REPLICATION_KEYS = ["updated_at"]
FULL_TABLE_STREAM = ["tags"]
REQUIRED_CONFIG_KEYS = [
    "start_date",
    "access_token",
]
LOGGER = singer.get_logger()


def load_schemas():
    schemas = {}
    schemas_path = Path(__file__).parent.absolute() / "schemas"
    for schema_path in schemas_path.iterdir():
        stream_name = schema_path.stem
        schemas[stream_name] = json.loads(schema_path.read_text())

    return schemas


def discover() -> Catalog:
    schemas = load_schemas()
    streams = []

    for tap_stream_id, schema in schemas.items():
        schema = schemas[tap_stream_id]
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=KEY_PROPERTIES,
            valid_replication_keys=None
            if tap_stream_id in FULL_TABLE_STREAM
            else VALID_REPLICATION_KEYS,
        )
        streams.append(
            CatalogEntry(
                stream=tap_stream_id,
                tap_stream_id=tap_stream_id,
                key_properties=KEY_PROPERTIES,
                schema=Schema.from_dict(schema),
                metadata=mdata,
                replication_method="FULL_TABLE"
                if tap_stream_id in FULL_TABLE_STREAM
                else "INCREMENTAL",
            )
        )
    return Catalog(streams)


def sync(catalog, config, state=None):
    for catalog_entry in catalog.streams:
        if not catalog_entry.is_selected():
            continue
        LOGGER.info(f"syncing {catalog_entry.tap_stream_id}")
        stream = Stream(catalog_entry, config)
        stream.do_sync(state)


@utils.handle_top_exception(LOGGER)
def main():

    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        catalog = discover()
        catalog.dump()
        return
    catalog = args.catalog or discover()
    sync(catalog, args.config, args.state)


if __name__ == "__main__":
    main()
