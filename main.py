from attrs import define, evolve, field, frozen
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from dotenv import load_dotenv
import logging
import os
import psycopg2
from pyrate_limiter import Duration, RequestRate, Limiter
import queue
import requests
import time
import threading
import tqdm
from typing import Any

#### Initialization

logger = logging.getLogger(__name__)

load_dotenv()

API_KEY = os.getenv("API_KEY") or "DEMO_KEY"
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME") or DB_USER

#### Data model


def traverse(obj, *keys):
    """Traverse a sequence of keys, returning None if there's a key/index error.

    >>> traverse({"foo": [[1, 2], [3, 4]]}, ["foo", 0, 1])
    2
    """
    for key in keys:
        if obj is None:
            return None
        match obj:
            case list() | tuple():
                key = int(key)
                if 0 <= key < len(list):
                    obj = obj[key]
                return None
            case dict():
                obj = obj.get(key)
            case _:
                assert isinstance(key, str), key
                if not hasattr(obj, key):
                    return None
                obj = getattr(obj, key)
    return obj


def extract(obj, f, *keys):
    """Traverse an object and convert it to the given type if it's not None."""
    obj = traverse(obj, *keys)
    if obj is None:
        return None
    return f(obj)


@frozen
class CloseApproach:
    close_approach_time: datetime
    relative_velocity_mph: float
    miss_distance_miles: float
    orbiting_body: str

    @staticmethod
    def from_api(d: dict[str, Any]) -> "CloseApproach":
        close_approach_time = extract(
            d, lambda x: datetime.fromtimestamp(x / 1000), "epoch_date_close_approach"
        )
        relative_velocity_mph = extract(d, float, "relative_velocity", "miles_per_hour")
        miss_distance_miles = extract(d, float, "miss_distance", "miles")
        orbiting_body = extract(d, str, "orbiting_body")
        return CloseApproach(
            close_approach_time=close_approach_time,
            relative_velocity_mph=relative_velocity_mph,
            miss_distance_miles=miss_distance_miles,
            orbiting_body=orbiting_body,
        )

    def as_db_tuple(
        self, neo_id: str, ingest_time: datetime
    ) -> tuple[datetime, str, datetime, float, float, str]:
        return (
            ingest_time,
            neo_id,
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
        neo_reference_id = extract(d, str, "neo_reference_id")
        name = extract(d, str, "name")
        absolute_magnitude_h = extract(d, float, "absolute_magnitude_h")
        estimated_diameter_min_ft = extract(
            d, float, "estimated_diameter", "feet", "estimated_diameter_min"
        )
        estimated_diameter_max_ft = extract(
            d, float, "estimated_diameter", "feet", "estimated_diameter_max"
        )
        is_potentially_hazardous_asteroid = extract(
            d, bool, "is_potentially_hazardous_asteroid"
        )
        close_approach_data = tuple(
            CloseApproach.from_api(x)
            for x in (extract(d, list, "close_approach_data") or [])
        )
        is_sentry_object = extract(d, bool, "is_sentry_object")

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
        """Mask neo_reference_id as PII.

        Keep only last 4 characters, and change others to '-'.
        """
        refid = self.neo_reference_id
        mask = "-" * (len(refid) - 4)
        last = refid[:4]
        return mask + last

    def as_db_tuple(
        self, ingest_time: datetime
    ) -> tuple[datetime, str, str, float, float, float, bool, bool]:
        return (
            ingest_time,
            self.anonymize_reference_id(),
            self.name,
            self.absolute_magnitude_h,
            self.estimated_diameter_min_ft,
            self.estimated_diameter_max_ft,
            self.is_potentially_hazardous_asteroid,
            self.is_sentry_object,
        )


#### Download from source.


class TooManyRequestsException(Exception):
    pass


def download_neos(start_date: date, end_date: date) -> list[NearEarthObject]:
    """Download near Earth objects from NASA API.

    Returns all objects with closest approach to Earth in the [start_date, end_date) interval.
    The API returns dates at most 8 days apart.
    """

    # The API actually includes end_date in the response, so we subtract 1 day to keep it a
    # closed-open interval.
    resp = requests.get(
        "https://api.nasa.gov/neo/rest/v1/feed",
        params={
            "start_date": start_date.isoformat(),
            "end_date": (end_date - timedelta(days=1)).isoformat(),
            "api_key": API_KEY,
            "detailed": False,
        },
    )
    if resp.status_code == 429:
        raise TooManyRequestsException()
    assert resp.ok, resp.text

    remaining = resp.headers["X-RateLimit-Remaining"]
    total = resp.headers["X-RateLimit-Limit"]
    logger.debug("download_page: %s/%s remaining calls", remaining, total)

    content = resp.json()

    return [
        NearEarthObject.from_api(neo)
        for date_str, date_neos in content["near_earth_objects"].items()
        for neo in date_neos
    ]


#### Database persistence


def init_db(conn):
    """Create DB tables."""
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS neo (
                    id SERIAL PRIMARY KEY,
                    ingest_time TIMESTAMP WITH TIME ZONE,
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
                    ingest_time TIMESTAMP WITH TIME ZONE,
                    neo_id INTEGER REFERENCES neo(id),
                    close_approach_time TIMESTAMP,
                    relative_velocity_mph DOUBLE PRECISION,
                    miss_distance_miles DOUBLE PRECISION,
                    orbiting_body TEXT
                );"""
            )


def persist_neos(conn, ingest_time: datetime, neos: list[NearEarthObject]):
    """Persist NEOs into the database."""
    num_approaches = 0
    with conn:
        with conn.cursor() as cur:
            for neo in neos:
                cur.execute(
                    """
                    INSERT INTO neo(
                        ingest_time,
                        neo_reference_id,
                        name,
                        absolute_magnitude_h,
                        estimated_diameter_min_ft,
                        estimated_diameter_max_ft,
                        is_potentially_hazardous_asteroid,
                        is_sentry_object
                    ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id""",
                    neo.as_db_tuple(ingest_time),
                )
                neo_id = cur.fetchone()[0]
                cur.executemany(
                    """
                    INSERT INTO close_approach(
                        ingest_time,
                        neo_id,
                        close_approach_time,
                        relative_velocity_mph,
                        miss_distance_miles,
                        orbiting_body
                    ) VALUES(%s, %s, %s, %s, %s, %s)""",
                    [
                        approach.as_db_tuple(neo_id, ingest_time)
                        for approach in neo.close_approach_data
                    ],
                )
                num_approaches += cur.rowcount
    logger.debug(
        "Wrote %d NEO rows and %d close approach rows", len(neos), num_approaches
    )


#### Ingestion control


@define
class Turnstile:
    """Only one thread can pass at a time. The turnstile may be stopped for a period of time."""

    sem: threading.Semaphore = field(init=False)

    def __attrs_post_init__(self):
        self.sem = threading.Semaphore(1)

    def wait(self):
        """Wait for your turn through the turnstile."""
        self.sem.acquire()
        self.sem.release()

    def stop(self, wait_secs: float = 60.0):
        """Stop all other threads for wait_secs."""
        if self.sem.acquire(block=False):
            threading.Timer(wait_secs, self.restart)

    def restart(self):
        """Restart the turnstile."""
        self.sem.release()


@frozen
class Task:
    start: date
    end: date
    retry_count: int = field(default=0)

    def try_again(self) -> "Task":
        return evolve(self, retry_count=self.retry_count + 1)


# Slightly higher than the observed rate limit of 2000/hr.
# Any overflow will be caught by waiting on the turnstile.
limiter = Limiter(RequestRate(2500, Duration.HOUR))


@define
class Ingestion:
    conn = field()
    start_date: date = field()
    end_date: date = field(factory=date.today)
    window_size: timedelta = field(default=timedelta(days=8))
    num_workers: int = field(default=15)
    max_retries: int = field(default=3)

    ingest_time: datetime = field(init=False, factory=datetime.now)
    task_queue: queue.Queue = field(init=False, factory=queue.Queue)
    dead_letter_queue: queue.Queue = field(init=False, factory=queue.Queue)

    turnstile: Turnstile = field(init=False, factory=Turnstile)

    @limiter.ratelimit("nasa-neows", delay=True)
    def ingest_page(self, start: date, end: date):
        neos = download_neos(start, end)
        persist_neos(self.conn, self.ingest_time, neos)

    def worker(self, pbar, lock):
        thread = threading.current_thread()
        while True:
            try:
                self.turnstile.wait()

                # Timeout allows for a thread switch.
                task = self.task_queue.get(timeout=1e-5)

                self.ingest_page(task.start, task.end)
                self.task_queue.task_done()
                with lock:
                    pbar.update(1)
            except queue.Empty:
                # No more items to process.
                break
            except TooManyRequestsException:
                # Stop all threads from starting requests for 60 seconds.
                self.turnstile.stop(60.0)
                # Throttling doesn't count as a retry.
                self.task_queue.put(task)
            except:
                # Some other exception was raised, increment retry count.
                if task.retry_count + 1 >= self.max_retries:
                    self.dead_letter_queue.put(task)
                    self.task_queue.task_done()
                else:
                    self.task_queue.put(task.try_again())

    def run(self):
        num_windows = int((self.end_date - self.start_date) / self.window_size)
        for i in range(num_windows):
            start = self.start_date + i * self.window_size
            end = self.start_date + (i + 1) * self.window_size
            self.task_queue.put_nowait(Task(start, end, 0))

        pbar = tqdm.tqdm(total=num_windows)
        lock = pbar.get_lock()

        for i in range(self.num_workers):
            threading.Thread(
                target=self.worker, name=f"worker #{i}", args=(pbar, lock)
            ).start()

        self.task_queue.join()

        # Dumping a queue: https://stackoverflow.com/a/69095442/946814
        self.dead_letter_queue.put(None)
        get_with_timeout = lambda: self.dead_letter_queue.get(timeout=1e-5)
        dead_letters = list(iter(get_with_timeout, None))

        return dead_letters


#### App entry point


def main():
    logging.basicConfig(
        level=logging.ERROR,
        format="%(levelname)s:%(asctime)s:%(name)s:%(funcName)s:%(message)s",
    )

    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )
    try:
        init_db(conn)
        ingestion = Ingestion(conn, start_date=date(1982, 12, 10))
        dead_tasks = ingestion.run()
        if dead_tasks:
            logger.error("Could not fetch %d tasks: %s", len(dead_tasks), dead_tasks)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
