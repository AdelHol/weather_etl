# Weather ETL Project

This Dagster-based pipeline periodically collects, backfills, and stores weather data (both current and forecasted) from [WeatherAPI](https://www.weatherapi.com/), ensuring robust coverage even in case of job outages.

## Setup

Install locally in editable mode:

```bash
pip install -e ".[dev]"
```

Start the Dagster development server:

```bash
dagster dev
```

Then open [http://localhost:3000](http://localhost:3000) to explore the UI.

## Assets Summary

### `test_db_connection`

Checks the availability of the PostgreSQL database.

### `create_tables`

Creates the necessary schema and tables via SQL located in `sql/create_schema_and_tables.sql`.

### `fetch_and_store_all_cities`

Main ETL logic:

- Fetches current and hourly forecast data for `Prague` and `London`
- Stores current weather in `reporting_data.weather_current`
- Stores forecast data in `reporting_data.weather_forecast`
- Backfills historical 15-minute resolution current measurements up to 6 hours ago using the `history.json` endpoint
- De-duplicates using `ON CONFLICT` constraints
- Logs the number of missing timestamps successfully backfilled

## Database Tables

- `weather_current`: one row per city and 15-minute interval measurement
- `weather_forecast`: forecast per city per hour

Both tables are updated idempotently using `ON CONFLICT`.

## Scheduling

To run the ETL job every 15 minutes:

- Ensure the schedule toggle is ON in the Dagster UI
- Dagster Daemon must be running (handled by `dagster dev`)

## Deployment

This project can be deployed via [Dagster+](https://docs.dagster.io/dagster-plus/).

For more details, refer to the official documentation.

## API Info

- Provider: [WeatherAPI](https://www.weatherapi.com/)
- Endpoints used:
  - `/forecast.json`
  - `/history.json`
- Cities: `Prague`, `London`

## Testing

To run tests:

```bash
pytest weather_etl_tests
```

## Asset Logic

For detailed ETL logic, see:

```text
weather_etl/assets.py
```


