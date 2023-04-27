"""NASA NEO API consumer sample."""

from datetime import date, datetime, timedelta
import logging
import math
import os
import queue
from random import random
import time
import threading
from typing import Any

from attrs import define, evolve, field, frozen
from dotenv import load_dotenv
import pdb_attach
import psycopg2
from pyrate_limiter import Duration, RequestRate, Limiter
import requests
import tqdm

# Initialization

logger = logging.getLogger(__name__)

load_dotenv()

API_KEY = os.getenv("API_KEY") or "DEMO_KEY"

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME") or DB_USER

db_params = {
    "dbname": DB_NAME,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "host": DB_HOST,
    "port": DB_PORT,
}

pdb_attach.listen(50000)

# Data model


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
                if 0 <= key < len(obj):
                    obj = obj[key]
                else:
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
class NeoCloseApproach:
    """Model for a NEO's close approach to Earth."""

    neo_reference_id: str
    name: str
    absolute_magnitude_h: float
    estimated_diameter_min_ft: float
    estimated_diameter_max_ft: float
    is_potentially_hazardous_asteroid: bool
    is_sentry_object: bool
    close_approach_time: datetime
    close_approach_velocity_mph: float
    close_approach_distance_miles: float

    @staticmethod
    def from_api(d: dict[str, Any]) -> "NeoCloseApproach":
        """Builds an object from its API result."""
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
        is_sentry_object = extract(d, bool, "is_sentry_object")
        close_approach_time = extract(
            d,
            lambda x: datetime.fromtimestamp(x / 1000),
            "close_approach_data",
            0,
            "epoch_date_close_approach",
        )
        close_approach_velocity_mph = extract(
            d, float, "close_approach_data", 0, "relative_velocity", "miles_per_hour"
        )
        close_approach_distance_miles = extract(
            d, float, "close_approach_data", 0, "miss_distance", "miles"
        )

        return NeoCloseApproach(
            neo_reference_id=neo_reference_id,
            name=name,
            absolute_magnitude_h=absolute_magnitude_h,
            estimated_diameter_min_ft=estimated_diameter_min_ft,
            estimated_diameter_max_ft=estimated_diameter_max_ft,
            is_potentially_hazardous_asteroid=is_potentially_hazardous_asteroid,
            is_sentry_object=is_sentry_object,
            close_approach_time=close_approach_time,
            close_approach_velocity_mph=close_approach_velocity_mph,
            close_approach_distance_miles=close_approach_distance_miles,
        )

    def anonymize_reference_id(self) -> str:
        """Mask neo_reference_id as PII.

        Keep only last 4 characters, and change others to '-'.
        """
        refid = self.neo_reference_id
        mask = "-" * (len(refid) - 4)
        last = refid[:4]
        return mask + last

    def as_db_tuple(self, ingest_time: datetime) -> tuple:
        """Export the model as a tuple for DB insertion."""
        return (
            ingest_time,
            self.anonymize_reference_id(),
            self.name,
            self.absolute_magnitude_h,
            self.estimated_diameter_min_ft,
            self.estimated_diameter_max_ft,
            self.is_potentially_hazardous_asteroid,
            self.is_sentry_object,
            self.close_approach_time,
            self.close_approach_velocity_mph,
            self.close_approach_distance_miles,
        )


# Download from source.


class TooManyRequestsException(Exception):
    """Exception for an HTTP 429 Too Many Requests status code."""


def download_neos(start_date: date, end_date: date) -> list[NeoCloseApproach]:
    """Download near Earth objects from NASA API.

    Returns all objects with closest approach to Earth in the [start_date, end_date)
    interval. The API returns dates at most 8 days apart.
    """

    # The API actually includes end_date in the response, so we subtract 1 day to keep
    # it a closed-open interval.
    logger.debug("starting download: [%s, %s)", start_date, end_date)
    resp = requests.get(
        "https://api.nasa.gov/neo/rest/v1/feed",
        params={
            "start_date": start_date.isoformat(),
            "end_date": (end_date - timedelta(days=1)).isoformat(),
            "api_key": API_KEY,
            "detailed": False,
        },
        timeout=10.0,
    )

    remaining = resp.headers["X-RateLimit-Remaining"]
    total = resp.headers["X-RateLimit-Limit"]
    logger.debug("download_page: %s/%s remaining calls", remaining, total)

    if resp.status_code == 429:
        raise TooManyRequestsException()
    assert resp.ok, resp.text

    content = resp.json()

    return [
        NeoCloseApproach.from_api(neo)
        for date_str, date_neos in content["near_earth_objects"].items()
        for neo in date_neos
    ]


# Database persistence


def init_db(conn):
    """Create DB tables."""
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS neo_close_approach (
                    id SERIAL PRIMARY KEY,
                    ingest_time TIMESTAMP WITH TIME ZONE,
                    neo_reference_id VARCHAR,
                    name TEXT,
                    absolute_magnitude_h DOUBLE PRECISION,
                    estimated_diameter_min_ft DOUBLE PRECISION,
                    estimated_diameter_max_ft DOUBLE PRECISION,
                    is_potentially_hazardous_asteroid BOOL,
                    is_sentry_object BOOL,
                    close_approach_time TIMESTAMP,
                    close_approach_velocity_mph DOUBLE PRECISION,
                    close_approach_distance_miles DOUBLE PRECISION
                );"""
            )


def persist_neos(conn, ingest_time: datetime, neos: list[NeoCloseApproach]):
    """Persist NEOs into the database."""
    with conn:
        with conn.cursor() as cur:
            for neo in neos:
                cur.execute(
                    """
                    INSERT INTO neo_close_approach(
                        ingest_time,
                        neo_reference_id,
                        name,
                        absolute_magnitude_h,
                        estimated_diameter_min_ft,
                        estimated_diameter_max_ft,
                        is_potentially_hazardous_asteroid,
                        is_sentry_object,
                        close_approach_time,
                        close_approach_velocity_mph,
                        close_approach_distance_miles
                    ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    neo.as_db_tuple(ingest_time),
                )

    logger.info("Wrote %d NEO close approach rows", len(neos))


# Ingestion control


@define
class Turnstile:
    """Only one thread can pass at a time."""

    sem: threading.Semaphore = field(init=False)

    def __attrs_post_init__(self):
        self.sem = threading.Semaphore(1)

    def wait(self):
        """Wait for your turn through the turnstile."""
        logger.debug("passing through turnstile...")
        self.sem.acquire()
        self.sem.release()

    def stop(self) -> bool:
        """Stop all other threads."""
        if self.sem.acquire(blocking=False):
            logger.debug("stopping turnstile...")
            return True
        # Turnstile is already stopped.
        return False

    def restart(self):
        """Restart the turnstile."""
        logger.debug("restarting turnstile...")
        self.sem.release()


@frozen
class Task:  # pylint: disable=too-few-public-methods
    """Model for a single task."""

    start: date
    end: date
    retry_count: int = field(default=0)

    def try_again(self) -> "Task":
        """Returns a new stack from this one, incrementing retry count."""
        return evolve(self, retry_count=self.retry_count + 1)


# Slightly higher than the posted rate limit of 1000/hr.
# Any overflow will be caught by waiting on the turnstile.
limiter = Limiter(RequestRate(1200, Duration.HOUR))


@define
class Ingestion:  # pylint: disable=too-many-instance-attributes
    """Data ingestion executor."""

    start_date: date = field()
    end_date: date = field(factory=date.today)
    window_size: timedelta = field(default=timedelta(days=8))
    num_workers: int = field(default=15)
    max_retries: int = field(default=3)
    ingest_time: datetime = field(factory=datetime.now)

    task_queue: queue.SimpleQueue = field(init=False, factory=queue.SimpleQueue)
    dead_letter_queue: queue.SimpleQueue = field(init=False, factory=queue.SimpleQueue)

    turnstile: Turnstile = field(init=False, factory=Turnstile)

    @limiter.ratelimit("nasa-neows", delay=True)
    def ingest_page(self, conn, start: date, end: date):
        """Ingests a page of NEOs and persist them to database."""
        neos = download_neos(start, end)
        persist_neos(conn, self.ingest_time, neos)

    # pylint: disable=broad-exception-caught
    def retry_loop(self, conn, task: Task):
        """Retries task with exponential backoff until it succeeds."""
        delay = 16.0
        factor = math.sqrt(2)  # Doubles delay every 2 attempts.
        while True:
            logger.info("Sleeping for %.3fs", delay)
            time.sleep(delay)
            try:
                self.ingest_page(conn, task.start, task.end)
                break
            except TooManyRequestsException:
                delay *= factor
            except Exception:
                # If there's an unexpected exception we fail-open, discarding the task
                # and releasing the turnstile.
                logger.exception(
                    "Unexpected exception during retry loop, discarding task %s", task
                )
                self.dead_letter_queue.put(task)
                break

    def worker(self, pbar, lock):
        """Initialize a thread worker and start loop."""
        time.sleep(random() * 5)  # Stagger thread start
        logger.info("Starting worker")
        conn = psycopg2.connect(**db_params)
        try:
            while self.do_work(conn, pbar, lock):
                pass
        finally:
            conn.close()

    def do_work(self, conn, pbar, lock) -> bool:
        """Get a task from the queue and execute it.

        If there are no more tasks, returns False. Otherwise, returns True.
        """
        logger.info("Approx queue size: %d", self.task_queue.qsize())
        try:
            self.turnstile.wait()

            # 10us timeout allows for a thread switch.
            task = self.task_queue.get(timeout=1e-5)

            self.ingest_page(conn, task.start, task.end)
            with lock:
                pbar.update(1)
            return True
        except queue.Empty:
            # No more items to process.
            logger.debug("empty queue")
            return False
        except TooManyRequestsException:
            # Stop all other threads from starting requests, and tests for renewed quota
            # with a single thread.
            if self.turnstile.stop():
                self.retry_loop(conn, task)
                self.turnstile.restart()
                with lock:
                    pbar.update(1)
            else:
                # Throttling doesn't count as a retry for the task.
                self.task_queue.put(task)
            return True
        except Exception:
            logger.exception("Unexpected exception handling task %s", task)
            # Some other exception was raised, increment retry count.
            if task.retry_count + 1 >= self.max_retries:
                self.dead_letter_queue.put(task)
            else:
                self.task_queue.put(task.try_again())
                logger.warning("Redoing task %s", task)
            return True
        finally:
            with lock:
                # Update progress bar even if nothing was written.
                pbar.update(0)

    def run(self):
        """Starts ingestion."""
        num_windows = int((self.end_date - self.start_date) / self.window_size)
        for i in range(num_windows):
            start = self.start_date + i * self.window_size
            end = self.start_date + (i + 1) * self.window_size
            self.task_queue.put_nowait(Task(start, end, 0))

        pbar = tqdm.tqdm(total=num_windows)
        lock = pbar.get_lock()

        threads = [
            threading.Thread(
                target=self.worker, name=f"worker #{i:02d}", args=(pbar, lock)
            )
            for i in range(self.num_workers)
        ]
        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        # Dumping a queue: https://stackoverflow.com/a/69095442/946814
        def get_with_timeout():
            self.dead_letter_queue.get(timeout=1e-5)

        self.dead_letter_queue.put(None)
        dead_letters = list(iter(get_with_timeout, None))

        return dead_letters


# App entry point

LOG_FORMAT = (
    "[%(asctime)s] "
    "%(levelname)s "
    "[%(threadName)s] "
    "[%(filename)s:%(funcName)s:%(lineno)d] "
    "%(message)s"
)


def main(start_date: date, num_workers: int):
    """App entry point."""
    now = datetime.now()

    logger.setLevel(logging.DEBUG)
    log_handler = logging.FileHandler(f"app_{now:%Y%m%d_%H%M%S}.log")
    log_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(log_handler)

    # FIXME: Use a single connection for all threads.
    #
    # Sharing a connection across threads fails intermittentily with
    #
    #     psycopg2.ProgrammingError: the connection cannot be re-entered recursively
    #
    # I'm using connections only as context managers, so this shouldn't happen.
    #
    # Relevant SO thread: https://stackoverflow.com/q/73803605/946814
    # TODO: open a bug in psycopg2?
    conn = psycopg2.connect(**db_params)
    try:
        init_db(conn)
    finally:
        conn.close()

    ingestion = Ingestion(
        start_date=start_date, num_workers=num_workers, ingest_time=now
    )
    dead_tasks = ingestion.run()
    if dead_tasks:
        logger.error("Could not fetch %d tasks: %s", len(dead_tasks), dead_tasks)


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument("--start-date", type=date.fromisoformat, default="1982-12-10")
    parser.add_argument("--num-workers", type=int, default=15)
    parser.add_argument(
        "--loglevel",
        choices=["debug", "info", "warning", "error", "critical"],
        default="error",
        type=str.lower,
    )

    args = parser.parse_args()

    # TODO: I'd like to log everything at 'loglevel', and the module's logs to
    # 'app.log'. The lines below log everything to stderr, in addition to 'app.log'.
    #
    # loglevel = getattr(logging, args.loglevel.upper())
    # logging.basicConfig(level=loglevel, format=LOG_FORMAT)

    main(args.start_date, args.num_workers)
