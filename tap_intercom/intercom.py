import requests
from ratelimit import limits
import ratelimit
from requests.models import HTTPError
import singer
import backoff
import datetime
from typing import Dict

LOGGER = singer.get_logger()
ONE_MINUTE = 60


def _is_internal_server_error(e: requests.HTTPError) -> bool:
    return e.response.status_code == 500


class Intercom:
    BASE_URL = "https://api.intercom.io"

    def __init__(self, access_token):
        self.SESSION = requests.Session()
        self.access_token = access_token

    def scroll_companies(self):
        # The scroll endpoint specifically documents that a 500 Internal Server Error may occur
        # while iterating the scroll results. If this occurs, the scroll cannot be continued,
        # meaning we need to restart the entire process.
        # This is consistently testable either, so if this blows up, blame bad API design.
        url = f"{self.BASE_URL}/companies/scroll"

        for _ in range(0, 5):
            try:
                scroll_param: str = None

                while True:
                    data = self.call_scroll_api(url, {"scroll_param": scroll_param})

                    companies = data.get("data")
                    scroll_param = data.get("scroll_param")

                    if not companies:
                        return

                    for company in companies:
                        yield company, self.unixseconds_to_datetime(
                            company["updated_at"]
                        )
            except HTTPError as err:
                # This is likely to occur if the tap is run more than once in a short interval.
                # Basically, only one scroll can be done per application at a time, and if a
                # a second one is attempted, it will result in a 400 Bad Request.
                # Yeah, it's weird.
                if err.response.status_code != 500:
                    raise

        raise RuntimeError("attempted to restart companies scroll more than 5 times")

    @backoff.on_exception(
        backoff.expo,
        (
            requests.exceptions.RequestException,
            requests.exceptions.HTTPError,
            ratelimit.exception.RateLimitException,
        ),
        max_tries=5,
        base=5,
        factor=1,
        giveup=_is_internal_server_error,
    )
    @limits(calls=1000, period=ONE_MINUTE)
    def call_scroll_api(self, url, params={}):
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
        base=5,
        factor=1,
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
