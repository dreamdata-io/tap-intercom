import requests
from ratelimit import limits
import ratelimit
import singer
import backoff
import datetime
from typing import Dict

LOGGER = singer.get_logger()
ONE_MINITE = 60


class Intercom:
    BASE_URL = "https://api.intercom.io"
    DATA_FIELD = {
        "companies": "data",
        "contacts": "data",
        "tags": "data",
        "conversations": "conversations",
        "segments": "segments",
    }

    def __init__(self, access_token, tap_stream_id):
        self.SESSION = requests.Session()
        self.access_token = access_token
        self.tap_stream_id = tap_stream_id

    def get_records(self):
        per_page = 150 if self.tap_stream_id == "contacts" else 60
        replication_path = ["updated_at"]
        params = {"per_page": per_page}

        url = f"{self.BASE_URL}/{self.tap_stream_id}"
        for record in self.paginate(url, params=params):

            replication_value = self.unixseconds_to_datetime(
                self.get_value(record, replication_path)
            )

            yield record, replication_value

    def get_value(self, obj: dict, path_to_replication_key=None, default=None):
        if not path_to_replication_key:
            return default
        for path_element in path_to_replication_key:
            obj = obj.get(path_element)
            if not obj:
                return default
        return obj

    def unixseconds_to_datetime(self, ms: int) -> datetime.datetime:
        return (
            datetime.datetime.fromtimestamp(ms, datetime.timezone.utc) if ms else None
        )

    def paginate(self, url: str, params: Dict = None):
        while True:
            data = self.call_api(url, params=params)
            if data:
                yield from data.get(self.DATA_FIELD.get(self.tap_stream_id))
            if self.tap_stream_id == "contacts":
                starting_after = self.get_value(
                    data, ["pages", "next", "starting_after"]
                )
                if not starting_after:
                    break
                params["starting_after"] = starting_after

            else:
                url = self.get_value(data, ["pages", "next"])
            if not url or url == "null":
                break

    @backoff.on_exception(
        backoff.expo,
        (
            requests.exceptions.RequestException,
            requests.exceptions.HTTPError,
            ratelimit.exception.RateLimitException,
        ),
    )
    @limits(calls=1000, period=ONE_MINITE)
    def call_api(self, url, params={}):
        response = self.SESSION.get(
            url,
            headers={
                "Authorization": f"Bearer {self.access_token}",
                "Accept": "application/json",
            },
            params=params,
        )
        LOGGER.info(response.url)
        response.raise_for_status()
        return response.json()
