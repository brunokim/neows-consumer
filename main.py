from attrs import frozen, define
from datetime import date, datetime
import logging
import os
import requests
from typing import Any


logger = logging.getLogger(__name__)

API_KEY = os.getenv("API_KEY") or "DEMO_KEY"


@frozen
class CloseApproach:
    close_approach_time: datetime
    relative_velocity_mph: float
    miss_distance_miles: float
    orbiting_body: str

    @staticmethod
    def from_api(d: dict[str, Any]) -> "CloseApproach":
        close_approach_time = date.fromtimestamp(d["epoch_date_close_approach"] / 1000)
        relative_velocity_mph = float(d["relative_velocity"]["miles_per_hour"])
        miss_distance_miles = float(d["miss_distance"]["miles"])
        orbiting_body = d["orbiting_body"]
        return CloseApproach(
            close_approach_time=close_approach_time,
            relative_velocity_mph=relative_velocity_mph,
            miss_distance_miles=miss_distance_miles,
            orbiting_body=orbiting_body,
        )


@frozen
class NearEarthObject:
    neo_reference_id: str
    name: str
    absolute_magnitude_h: float
    estimated_diameter_min_ft: float
    estimated_diameter_max_ft: float
    is_potentially_hazardous_asteroid: bool
    close_approach_data: tuple[CloseApproach, ...]
    is_sentry_object: bool

    @staticmethod
    def from_api(d: dict[str, Any]) -> "NearEarthObject":
        neo_reference_id = d["neo_reference_id"]
        name = d["name"]
        absolute_magnitude_h = float(d["absolute_magnitude_h"])
        estimated_diameter_min_ft = float(
            d["estimated_diameter"]["feet"]["estimated_diameter_min"]
        )
        estimated_diameter_max_ft = float(
            d["estimated_diameter"]["feet"]["estimated_diameter_max"]
        )
        is_potentially_hazardous_asteroid = d["is_potentially_hazardous_asteroid"]
        close_approach_data = tuple(
            CloseApproach.from_api(x) for x in d["close_approach_data"]
        )
        is_sentry_object = d["is_sentry_object"]

        return NearEarthObject(
            neo_reference_id=neo_reference_id,
            name=name,
            absolute_magnitude_h=absolute_magnitude_h,
            estimated_diameter_min_ft=estimated_diameter_min_ft,
            estimated_diameter_max_ft=estimated_diameter_max_ft,
            is_potentially_hazardous_asteroid=is_potentially_hazardous_asteroid,
            close_approach_data=close_approach_data,
            is_sentry_object=is_sentry_object,
        )


def download_page(start_date: date) -> list[NearEarthObject]:
    resp = requests.get(
        f"https://api.nasa.gov/neo/rest/v1/feed",
        params={
            "start_date": start_date.isoformat(),
            "api_key": API_KEY,
            "detailed": False,
        },
    )
    assert resp.ok, resp.text
    logger.debug(
        "download_page: Remaining calls: %s", resp.headers["X-RateLimit-Remaining"]
    )

    content = resp.json()

    neos: list[NearEarthObject] = []
    for date_str, date_neos in content["near_earth_objects"].items():
        for neo in date_neos:
            neos.append(NearEarthObject.from_api(neo))
    return neos


def main():
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(levelname)s:%(asctime)s:%(name)s:%(funcName)s:%(message)s",
    )

    from pprint import pprint

    for neo in download_page(date.fromisoformat("1982-12-10")):
        pprint(neo)


if __name__ == "__main__":
    main()
