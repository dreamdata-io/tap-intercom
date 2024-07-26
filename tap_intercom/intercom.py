import requests
from ratelimit import limits
import ratelimit
from requests.models import HTTPError
import singer
import backoff
import datetime
from typing import Dict
from simplejson.scanner import JSONDecodeError

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
                if err.response.status_code not in [500, 400]:
                    raise
                    
        raise RuntimeError("attempted to restart companies scroll more than 5 times")

    @backoff.on_exception(
        backoff.expo,
        (
            requests.exceptions.RequestException,
            requests.exceptions.HTTPError,
            ratelimit.exception.RateLimitException,
            JSONDecodeError,
        ),
        max_tries=20,
        factor=5,
        max_time=60 * 10,
        giveup=_is_internal_server_error,
    )
    @limits(calls=1000, period=ONE_MINUTE)
    def call_scroll_api(self, url, params={}):
        response = self.SESSION.get(
            url,
            headers=self.__make_header(),
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

        if tap_stream_id == "conversations":
            data_field = tap_stream_id

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
            JSONDecodeError,
        ),
        max_tries=20,
        factor=5,
        max_time=60 * 10,
        giveup=_is_internal_server_error,
    )
    @limits(calls=1000, period=ONE_MINUTE)
    def call_api(self, url, params={}):
        response = self.SESSION.get(
            url,
            headers=self.__make_header(),
            params=params,
        )
        LOGGER.debug(response.url)
        response.raise_for_status()
        return response.json()

    @backoff.on_exception(
        backoff.expo,
        (
            requests.exceptions.RequestException,
            requests.exceptions.HTTPError,
            ratelimit.exception.RateLimitException,
            JSONDecodeError,
        ),
        max_tries=20,
        factor=5,
        max_time=60 * 10,
        giveup=_is_internal_server_error,
    )
    @limits(calls=1000, period=ONE_MINUTE)
    def call_search_api(self, url, params={}, json=None):
        response = self.SESSION.post(
            url,
            headers=self.__make_header(),
            params=params,
            json=json,
        )
        LOGGER.debug(response.url)
        response.raise_for_status()
        return response.json()

    def search(self, tap_stream_id, start_date, end_date):
        url = f"{self.BASE_URL}/{tap_stream_id}/search"

        data_field = tap_stream_id
        if tap_stream_id == "contacts":
            data_field = "data"

        # For more options:
        # https://developers.intercom.com/docs/references/rest-api/api.intercom.io/Contacts/SearchContacts/
        per_page = 150
        pagination = {"page": 0, "per_page": per_page}
        json = {
            "query": {
                "operator": "OR",
                "value": [
                    {
                        "operator": "AND",
                        "value": [
                            {
                                "field": "updated_at",
                                "operator": ">",
                                "value": start_date.timestamp(),
                            },
                            {
                                "field": "updated_at",
                                "operator": "<",
                                "value": end_date.timestamp(),
                            },
                        ],
                    },
                    {
                        "field": "updated_at",
                        "operator": "=",
                        "value": start_date.timestamp(),
                    },
                ],
            },
            "sort": {"field": "updated_at", "order": "ascending"},
            "pagination": pagination,
        }
        pagination_path = ["pages", "next"]
        replication_path = ["updated_at"]
        while True:
            response_data = self.call_search_api(url, json=json)
            if response_data:
                records = self.get_value(response_data, [data_field])
                if records:
                    for record in records:
                        replication_value = self.get_value(record, replication_path)
                        yield record, self.unixseconds_to_datetime(replication_value)
            pagination = self.get_value(response_data, pagination_path)
            if not pagination or pagination == "null":
                return
            pagination["per_page"] = per_page
            json["pagination"] = pagination

    def __make_header(self):
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
        }
