import requests
from ratelimit import limits
import ratelimit
import singer
import backoff
import datetime
from typing import Dict

LOGGER = singer.get_logger()
ONE_MINUTE = 60


class Intercom:
    BASE_URL = "https://api.intercom.io"

    def __init__(self, access_token):
        self.SESSION = requests.Session()
        self.access_token = access_token

    def scroll_companies(self):
        url = f"{self.BASE_URL}/companies/scroll"
        scroll_param: str = None

        while True:
            # This is a little iffy, because the endpoint specifically mentions erroring out
            # due to timeouts, which results in a 500 Internal Server Error. When this happens
            # you need to retry the entire scrolling process.
            # I've been unable to confirm if you can immediately retry the scroll process, or
            # if you actually need to wait for 1 minute until the scroll is reset for the app.
            data = self.call_api(url, {"scroll_param": scroll_param})

            companies = data.get("data")
            scroll_param = data.get("scroll_param")

            if not companies:
                break

            for company in companies:
                yield company, self.unixseconds_to_datetime(company["updated_at"])

    def get_records(self, tap_stream_id):
        data_field = "data"
        endpoint = tap_stream_id
        replication_path = ["updated_at"]
        page_size = 60
        pagination_path = ["pages", "next"]

        if tap_stream_id in ["conversations", "segments"]:
            data_field = tap_stream_id
        if tap_stream_id == "contacts":
            pagination_path.append("starting_after")
            page_size = 150

        yield from self.__get(
            page_size=page_size,
            endpoint=endpoint,
            replication_path=replication_path,
            tap_stream_id=tap_stream_id,
            pagination_path=pagination_path,
            data_field=data_field,
        )

    def __get(
        self,
        page_size,
        endpoint,
        replication_path,
        tap_stream_id,
        pagination_path,
        data_field,
    ):
        params = {"per_page": page_size}

        url = f"{self.BASE_URL}/{endpoint}"
        for record in self.paginate(
            url,
            pagination_path=pagination_path,
            tap_stream_id=tap_stream_id,
            data_field=data_field,
            params=params,
        ):

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

    def paginate(
        self,
        url: str,
        pagination_path: list,
        tap_stream_id: str,
        data_field: str,
        params: Dict = None,
    ):
        while True:
            data = self.call_api(url, params=params)
            if data:
                yield from data.get(data_field)
            next_page = self.get_value(data, pagination_path)
            if not next_page or next_page == "null":
                return
            if tap_stream_id == "contacts":
                params["starting_after"] = next_page
            else:
                url = next_page
                params = {}

    @backoff.on_exception(
        backoff.expo,
        (
            requests.exceptions.RequestException,
            requests.exceptions.HTTPError,
            ratelimit.exception.RateLimitException,
        ),
        max_tries=5,
    )
    @limits(calls=1000, period=ONE_MINUTE)
    def call_api(self, url, params={}):
        response = self.SESSION.get(
            url,
            headers={
                "Authorization": f"Bearer {self.access_token}",
                "Accept": "application/json",
            },
            params=params,
        )
        LOGGER.debug(response.url)
        response.raise_for_status()
        return response.json()
