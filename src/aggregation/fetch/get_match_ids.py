import logging

from typing import Optional
import csv
from datetime import datetime, timedelta

import aggregation.fetch.ballchasing_api as ballchasing_api


def _append_ids_to_csv(outfile: str, ids):
    """Append a list of match ids to a .csv file at a specified relative path."""
    with open(outfile, "a", newline="") as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerows([[id] for id in ids])


def get_ids(
    api_key: str,
    base_url: str,
    start: datetime,
    end: datetime,
    time_resolution: Optional[timedelta] = timedelta(days=1),
    outfile: Optional[str] = None,
):
    """
    Get the ballchasing ids for matches with specified parameters and timeframe, and optionally
    store them in a .csv file.

    Args:
        api_key (str): The ballchasing.com API key of the user making the requests.
        base_url (str): The url of the query to the /replays endpoint including any non-time
        parameters.
        start (datetime): Datetime object signifying the start of the date range.
        end (datetime): Datetime object signifying the end of the date range.
        time_resolution (Optional[timedelta]): The time range for each query. Defaults to 1 day.
        outfile (Optional[str]): The relative path to a .csv file where the match ids should be
        stored. Defaults to None.

    Returns:
        list: A list of match ids fulfilling the arguments.

    Raises:
        TypeError: When an argument is invalid.
        ResponseOverflowError: When the query returns too many results due to an insufficient time
        resolution.
    """
    # Type checking
    if not isinstance(api_key, str):
        raise TypeError("API key must be of type string")

    if not isinstance(base_url, str):
        raise TypeError(
            "query base URL must be a string beginning 'https://ballchasing.com/api/replays?'"
        )

    if not isinstance(start, datetime):
        raise TypeError("start time must be of type datetime")

    if not isinstance(end, datetime):
        raise TypeError("end time must be of type datetime")

    # Raise an error if time_resolution is not None and is not of type timedelta
    if time_resolution != None and not isinstance(time_resolution, timedelta):
        raise TypeError("time_resolution must be of type timedelta")

    # Raise an error if outfile is not None and is not a string ending in .csv
    if outfile != None and (not isinstance(outfile, str) or outfile[-4:] != ".csv"):
        raise TypeError("output file path must be a string ending '.csv'")

    # Round the input time resolution to the nearest minute
    time_resolution = timedelta(
        days=time_resolution.days,
        minutes=round((time_resolution.seconds + (time_resolution.microseconds / 1_000_000)) / 60),
    )

    # If the rounded resolution is 0 days and 0 minutes, set it to 1 minute (max resolution)
    if time_resolution == timedelta():
        logging.warning("time resolution too high, clamped to 1 minute (max resolution)")
        time_resolution = timedelta(minutes=1)

    # Get the start times for all the required API calls
    start_times = []
    while start < end:
        start_times.append(start)
        start += time_resolution

    if len(start_times) <= 100:
        logging.info(f"{len(start_times)} API calls required")
    else:
        logging.warning(
            f"{len(start_times)} API calls required - consider using a lower resolution"
        )

    # Create an instance of ballchasing_api with the API key, and calculate the required sleep time
    # between calls
    api = ballchasing_api.API(api_key)
    sleep_time = api.compute_sleep_time(base_url, len(start_times))

    all_match_ids = []
    for start_time in start_times:

        time_format = "%Y-%m-%dT%H:%M:00Z"
        # The end time for each call is the start time plus the time resolution
        # The exception is the last call, where the end time will be 'end'
        if start_time + time_resolution < end:
            end_time_str = (start_time + time_resolution).strftime(time_format)
        else:
            end_time_str = end.strftime(time_format)

        start_time_str = start_time.strftime(time_format)

        time_interval_str = f"created-after={start_time_str}&created-before={end_time_str}"

        # Add an "&" if needed to add a new url parameter
        url = base_url if base_url[-1] in ["?", "&"] else f"{base_url}&"
        url += time_interval_str

        data = api.call(url, sleep_time)

        match_ids = []
        # Check if any matches have been returned
        if data != {"list": []}:
            # If the count is more than 9999, or the count doesn't match the length of the data
            # list, then not all data has been captured, so raise an error
            if data["count"] > 9999 or len(data["list"]) != data["count"]:
                raise ResponseOverflowError(
                    "data not present for all matches in range, a higher resolution is required"
                )
            else:

                for match in data["list"]:
                    match_ids.append(match["id"])

        all_match_ids += match_ids

        logging.info(
            f"{len(data['list'])} match ids stored in range {start_time_str} to {end_time_str}"
        )

        # If the path to an outfile has been passed in, append the match ids from this call to the
        # file
        if outfile:
            _append_ids_to_csv(outfile, match_ids)

    return all_match_ids


class ResponseOverflowError(Exception):
    """Raised when a request returns too many results."""

    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return f"ResponseOverflowError: {self.msg}"
