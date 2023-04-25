from attrs import frozen, define
from datetime import date, datetime
import logging
import os
import psycopg2
import requests
from typing import Any


logger = logging.getLogger(__name__)

API_KEY = os.getenv("API_KEY") or "DEMO_KEY"
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME") or DB_USER


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

    def as_db_tuple(self) -> tuple[datetime, float, float, str]:
        return (
            self.close_approach_time,
            self.relative_velocity_mph,
            self.miss_distance_miles,
            self.orbiting_body,
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

    def anonymize_reference_id(self) -> str:
        refid = self.neo_reference_id
        mask = "-" * (len(refid) - 4)
        last = refid[:4]
        return mask + last

    def as_db_tuple(self) -> tuple[str, str, float, float, float, bool, bool]:
        return (
            self.anonymize_reference_id(),
            self.name,
            self.absolute_magnitude_h,
            self.estimated_diameter_min_ft,
            self.estimated_diameter_max_ft,
            self.is_potentially_hazardous_asteroid,
            self.is_sentry_object,
        )


def download_neos(start_date: date) -> list[NearEarthObject]:
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


def persist_neos(conn, neos: list[NearEarthObject]):
    num_approaches = 0
    with conn:
        with conn.cursor() as cur:
            for neo in neos:
                cur.execute(
                    """
                    INSERT INTO neo(
                        neo_reference_id,
                        name,
                        absolute_magnitude_h,
                        estimated_diameter_min_ft,
                        estimated_diameter_max_ft,
                        is_potentially_hazardous_asteroid,
                        is_sentry_object
                    ) VALUES(%s, %s, %s, %s, %s, %s, %s)
                    RETURNING id""",
                    neo.as_db_tuple(),
                )
                neo_id = cur.fetchone()[0]
                cur.executemany(
                    """
                    INSERT INTO close_approach(
                        neo_id,
                        close_approach_time,
                        relative_velocity_mph,
                        miss_distance_miles,
                        orbiting_body
                    ) VALUES(%s, %s, %s, %s, %s)""",
                    [
                        (neo_id, *approach.as_db_tuple())
                        for approach in neo.close_approach_data
                    ],
                )
                num_approaches += cur.rowcount
    logger.debug(
        "Wrote %d NEO rows and %d close approach rows", len(neos), num_approaches
    )


def init_db(conn):
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS neo (
                    id SERIAL PRIMARY KEY,
                    neo_reference_id VARCHAR,
                    name TEXT,
                    absolute_magnitude_h DOUBLE PRECISION,
                    estimated_diameter_min_ft DOUBLE PRECISION,
                    estimated_diameter_max_ft DOUBLE PRECISION,
                    is_potentially_hazardous_asteroid BOOL,
                    is_sentry_object BOOL
                );"""
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS close_approach (
                    id SERIAL PRIMARY KEY,
                    neo_id INTEGER REFERENCES neo(id),
                    close_approach_time TIMESTAMP,
                    relative_velocity_mph DOUBLE PRECISION,
                    miss_distance_miles DOUBLE PRECISION,
                    orbiting_body TEXT
                );"""
            )


def main():
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(levelname)s:%(asctime)s:%(name)s:%(funcName)s:%(message)s",
    )

    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )
    try:
        init_db(conn)

        neos = download_neos(date.fromisoformat("1982-12-10"))
        persist_neos(conn, neos)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
