import logging
from requests import sessions
import time

# The sleep time required in order to abide by various rate limits
# Calculated as 25% greater than the minimum time required to abide by that limit
RATE_LIMITS = {
    "/replays/": {
        "regular": {"per_second": 0.625, "per_hour": 4.500},
        "gold": {"per_second": 0.625, "per_hour": 2.250},
        "diamond": {"per_second": 0.313, "per_hour": 0.900},
        "champion": 0.156,
        "gc": 0.078,
    },
    "/replays": {
        "regular": {"per_second": 0.625, "per_hour": 9.000},
        "gold": {"per_second": 0.625, "per_hour": 4.500},
        "diamond": {"per_second": 0.313, "per_hour": 2.250},
        "champion": 0.156,
        "gc": 0.078,
    },
}


class API:
    """
    A class to represent a connection to the ballchasing.com API.

    Attributes:
        api_key (str): API key used for the connection.
        _session (Session): Internal session from which requests are made.
        _consecutive_failed_requests (int): Internal counter of consecutive 429s or 500s.
        patron_type (str): Patreon tier corresponding to API key.

    Methods:
        ping(): Ping the API to assign a patron type.
        call(url: str, sleep_time: float): Make a call to the API and sleep an amount of seconds.
        compute_sleep_time(url: str, num_requests: int): Find the recommended sleep time for a
        number of requests to a certain endpoint to abide by the relevant rate limit.

    """

    def __init__(self, api_key):
        """Initialise a requests session and assign a patron type."""

        self.api_key = api_key

        self._session = sessions.Session()

        self._consecutive_failed_requets = 0

        self.patron_type = None

        # Ping the API to assign patron type
        self.ping()

        logging.info("established session with ballchasing.com API")

    def ping(self) -> dict:
        """Call the ping endpoint to assign a patron type."""

        r = self._session.get(
            "https://ballchasing.com/api/", headers={"Authorization": self.api_key}
        )

        logging.debug(f"call to https://ballchasing.com/api/ returned {r.status_code}")

        if r.status_code == 200:
            r_json = r.json()
            self.patron_type = r_json["type"]
            return r_json
        else:
            raise APIError(f"status code {r.status_code}")

    def call(self, url: str, sleep_time: float) -> dict:
        """
        Call either the /replays or /replays/{id} endpoint, and sleeps a specified amount of time.
        If the request returns status code 429 or 500, the request is retried a further 9 times
        with pauses applying exponential backoff.

        Args:
            url (str): The url to make the request to.
            sleep_time (float): The amount of seconds to sleep after completing the request.

        Returns:
            dict: A dictionary containing the response data.

        Raises:
            APIError: If the response returns a 429 or 500 ten consecutive times, or another non
            200 status code once.

        """

        r = self._session.get(url, headers={"Authorization": self.api_key})

        if r.status_code == 200:
            # If a request is successful reset the failed requests counter
            self._consecutive_failed_requets = 0

            logging.debug(f"call to {url} returned {r.status_code}")

            time.sleep(sleep_time)

            return r.json()

        elif r.status_code in [429, 500]:
            # Increment the number of failed requests
            self._consecutive_failed_requets += 1

            # Raise an error if the same request has failed 10 times
            if self._consecutive_failed_requets >= 10:
                raise APIError(f"failed after 10 retries with status code {r.status_code}")
            else:
                logging.warning(
                    f"call to {url} returned {r.status_code}, retrying ({self._consecutive_failed_requets} consecutive failed requests)"
                )

                time.sleep(sleep_time)

                backoff_time = 0.5 * 2 ** (self._consecutive_failed_requets - 1)
                time.sleep(backoff_time)
                # Retry the request after applying exponential backoff
                self.call(url, sleep_time)
        else:
            logging.critical(f"call to {url} returned {r.status_code}, failing")
            raise APIError(f"status code {r.status_code}")

    def compute_sleep_time(self, url: str, num_requests: int) -> float:
        """
        Calculates the sleep time required for a request, using the per second or per hour rate
        limit depending on the number of requests.

        Args:
            url (str): The URL of the request to determine the endpoint used.
            num_requests (int): The number of requests expected to be made.

        Returns:
            float: The number of seconds to sleep to abide by the rate limit.

        Raises:
            URLError: If the URL provided does not use a supported endpoint."""

        # Check if the url uses one of the supported endpoints
        # NOTE: This relies on the order of the RATE_LIMITS dictionary
        valid_endpoint = False
        for endpoint in list(RATE_LIMITS.keys()):
            if endpoint in url:
                valid_endpoint = True
                break

        # Raise a URLError if the url does not use a supported endpoint
        if not valid_endpoint:
            raise URLError("url is not valid (only /replays and /replays/{id} are supported)")

        # Get either a float or dictionary corresponding to the endpoint and patron tier
        rate_limit = RATE_LIMITS[endpoint][self.patron_type]

        # If rate_limit is just a float, the patron type doesn't have an hourly rate limit
        if isinstance(rate_limit, float):
            return rate_limit
        else:
            # If the number of requests is less than the per second rate then use that,
            # otherwise use the hourly rate
            if num_requests < 3600 / rate_limit["per_hour"]:
                return rate_limit["per_second"]
            else:
                return rate_limit["per_hour"]


class APIError(Exception):
    """Raised when an API request has failed."""

    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return f"APIError: {self.msg}"


class URLError(Exception):
    """Raised when the url uses an unsupposed API endpoint."""

    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return f"URLError: {self.msg}"
