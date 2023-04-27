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

Returns 72 objects that have their closest approach between Jan 1st 2023 and Jan 8th 2023 (inclusive).

## Response schema

The 
