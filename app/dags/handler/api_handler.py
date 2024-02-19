import backoff
import requests
from requests.exceptions import RequestException, HTTPError
import logging
from urllib.parse import urljoin


def backoff_hdlr(details):
    logging.warning(
        "Backing off {wait:0.1f} seconds afters {tries} tries "
        "calling function {target} with args {args} and kwargs "
        "{kwargs}".format(**details)
    )


def giveup_hdlr(details):
    logging.error(
        "Giving up calling function {target} after {tries} tries.".format(**details)
    )


class ApiClient:
    def __init__(self, base_url, timeout=120):
        self.base_url = base_url
        self.timeout = timeout
        self.session = requests.Session()
        self.logger = logging.getLogger(__name__)

    def on_backoff(self, details):
        logging.warning(
            "Backing off {wait:0.1f} seconds afters {tries} tries "
            "calling function {target} with args {args} and kwargs "
            "{kwargs}".format(**details)
        )

    def on_giveup(self, details):
        logging.error(
            "Giving up calling function {target} after {tries} tries.".format(**details)
        )

    @backoff.on_exception(
        backoff.constant,
        RequestException,
        max_tries=10,
        interval=60,
        on_backoff=backoff_hdlr,
        on_giveup=giveup_hdlr,
    )
    def make_request(self, method, endpoint, params=None, data=None, headers=None):
        url = urljoin(self.base_url, endpoint)
        response = self.session.request(
            method, url, params=params, data=data, headers=headers, timeout=self.timeout
        )
        response.raise_for_status()

        return response.json()

    def execute_request(self, method, endpoint, params=None, data=None, headers=None):
        return self.make_request(method, endpoint, params, data, headers)
