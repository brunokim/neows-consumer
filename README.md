# Near Earth Object's consumer

This project demonstrates how to consume objects from a rate-limited API. It exposes:

- Thread synchronization;
- Client rate limiting;
- Response to throttling with exponential backoff;

## NASA NEO-WS API

Near Earth objects (NEOs) are solar system bodies whose orbits approach Earth's orbit.
NASA tracks thousands of these objects by a mandate from the US Congress, in an effort
to detect impacts that may have economical significance or may be life-threatening, and
hopefully allows us to prepare for them.

The NEO WebService API (NEO-WS) provides information about those objects, indexed by their
approach to our planet. This REST interface allows us to query for all objects whose
closest approach lies between two given dates, at most 7 days apart. For example, the query

[https://api.nasa.gov/neo/rest/v1/feed?**start\_date**=2023-01-01&**end_date**=2023-01-08&**api_key**=DEMO_KEY](https://api.nasa.gov/neo/rest/v1/feed?start_date=2023-01-01&end_date=2023-01-08&api_key=DEMO_KEY)

returns 110 objects that have their closest approach between Jan 1st 2023 and Jan 8th 2023 (inclusive).
The largest of them is "17511 (1992 QN)", with approximately 4,900 ft of diameter, that reached its closest approach
to Earth on Jan 6th and missed the planet by 28 million miles (a very safe distance).
The one to get closest was (2022 AV13), with only 6ft of diameter, that on Jan 5th passed mere 68 thousand miles from us.
For comparison, the average distance between the Earth and the Moon is 226 thousand miles.

## Data schema

The data we're interested in can be represented in the following pseudo-grammar:

```none
response ::= {
  "near_earth_objects": {(date: [neo*])*
} ;
neo ::= {
  "neo_reference_id": string,
  "name": string,
  "absolute_magnitude_h": float,
  "estimated_diameter": {
    "feet": {
      "estimated_diameter_min": float,
      "estimated_diameter_max": float
    },
  },
  "is_potentially_hazardous_asteroid": boolean
  "close_approach_data": [close_approach*],
  "is_sentry_object": bool
} ;
close_approach ::= {
  "epoch_date_close_approach": timestamp_millis,
  "relative_velocity": {
    "miles_per_hour": float
  },
  "miss_distance": {
    "miles": float
  },
  "orbiting_body": string
} ;
```

Upon consuming the API, we noticed some quirks with the data:
- Each object should be associated with multiple close approaches, but upon consuming the API there's no instance where an object has more than 1.
- Orbiting body is always "Earth"

Given these, we converted the data to the following output table:

| Field | Type | Description |
|---|---|---|
| id | int | autoincrement database ID |
| ingest_time | timestamp | time when ingestion started |
| neo_reference_id | text | NEO ID, shown only with the last 4 characters |
| absolute_magnitude_h | float | Absolute magnitude H |
| estimated_diameter_min_ft | float | Minimum estimated diameter, in feet |
| estimated_diameter_max_ft | float | Maximum estimated diameter, in feet |
| is_potentially_hazardous_asteroid | bool | Whether the asteroid is considered potentially hazardous |
| is_sentry_object | bool |  |
| close_approach_date | timestamp | Date of closest approach |
| close_approach_velocity_mph | float | Relative velocity at closest approach, in miles per hour |
| close_approach_distance_miles | float | Distance from Earth at closest approach, in miles |

## Ingest design

The API documents a limit of 1000 requests/hour upon registering for an API key. For each request, it
provides the max limit and the current number of requests remaining via custom headers. In practice, the
advertised limit was always of 2000 requests/hour, but some intermediate infrastructure failed to
comply even with the lower limit. The solution was twofold:

- Rate limit in the client, using the [`pyrate_limiter` library](pyratelimiter.readthedocs.io/)
- Upon a request failure with code 429 Too Many Requests, stop all new requests and keep a single thread
  attempting again with exponential backoff. Once it's succesful, restart all other threads.

Threads are synchronized via a [`queue.SimpleQueue`](https://docs.python.org/3/library/queue.html#simplequeue-objects)
that stores all pending tasks.

1. Before starting thread workers, the queue is populated with 8-day windows of start-end dates to be queried, covering the entire desired range.
2. Each thread take one task at a time. In case of failure, the task is inserted back in the queue.
  - Each task has a retry count, that is incremented before reinsertion in case of an unexpected exception.
  - Throttling failures don't count for retries.

## Container

The application is containerized with Docker to create a Postgres database independent from the current environment.

## CLI

- `--num-workers`: number of concurrent workers to download.
- `--start-date`: Start date in the past to start ingestion.
