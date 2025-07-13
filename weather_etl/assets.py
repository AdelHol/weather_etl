import os
from datetime import datetime, timedelta
import logging
import requests
from dotenv import load_dotenv
from dagster import asset, Failure

load_dotenv()

API_KEY = os.getenv("WEATHER_API_KEY")
if not API_KEY:
    raise ValueError("Missing WEATHER_API_KEY in environment.")

CITIES = ["Prague", "London"]
SQL_PATH = os.path.join(os.path.dirname(__file__), "sql/create_schema_and_tables.sql")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

INSERT_CURRENT_SQL = """
INSERT INTO reporting_data.weather_current (
    city, as_of, temp_c, temp_f, is_day,
    condition_text, condition_icon, condition_code,
    wind_mph, wind_kph, wind_degree, wind_dir,
    pressure_mb, pressure_in, precip_mm, precip_in,
    humidity, cloud, feelslike_c, feelslike_f,
    windchill_c, windchill_f, heatindex_c, heatindex_f,
    dewpoint_c, dewpoint_f, vis_km, vis_miles,
    gust_mph, gust_kph, uv, fetched_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (city, as_of) DO UPDATE SET fetched_at = EXCLUDED.fetched_at
"""

INSERT_FORECAST_SQL = """
INSERT INTO reporting_data.weather_forecast (
    city, forecast_for, temp_c, temp_f, is_day,
    condition_text, condition_icon, condition_code,
    wind_mph, wind_kph, wind_degree, wind_dir,
    pressure_mb, pressure_in, precip_mm, precip_in,
    humidity, cloud, feelslike_c, feelslike_f,
    windchill_c, windchill_f, heatindex_c, heatindex_f,
    dewpoint_c, dewpoint_f, will_it_rain, chance_of_rain,
    vis_km, vis_miles, gust_mph, gust_kph, uv,
    prediction_generated_at, fetched_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (city, forecast_for, prediction_generated_at) DO UPDATE SET fetched_at = EXCLUDED.fetched_at
"""


# === Helper Functions ===
def fetch_weather_data(city, context, forecast=True):
    """Fetch weather data (forecast or historical) for a specific city using WeatherAPI."""
    url = (
        "http://api.weatherapi.com/v1/forecast.json"
        if forecast
        else "http://api.weatherapi.com/v1/history.json"
    )
    params = {"key": API_KEY, "q": city}
    if forecast:
        params.update({"days": 2, "aqi": "no", "alerts": "no"})
    else:
        dt = context.get("dt")
        params["dt"] = dt.strftime("%Y-%m-%d")
        params["hour"] = dt.hour

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


def insert_current_weather(cur, city, data, fetched_at):
    """Insert current weather data into the weather_current table."""
    curr = data["current"]
    as_of = datetime.fromtimestamp(curr["last_updated_epoch"])
    values = (
        city,
        as_of,
        curr.get("temp_c"),
        curr.get("temp_f"),
        curr.get("is_day"),
        curr["condition"]["text"],
        curr["condition"]["icon"],
        curr["condition"]["code"],
        curr.get("wind_mph"),
        curr.get("wind_kph"),
        curr.get("wind_degree"),
        curr.get("wind_dir"),
        curr.get("pressure_mb"),
        curr.get("pressure_in"),
        curr.get("precip_mm"),
        curr.get("precip_in"),
        curr.get("humidity"),
        curr.get("cloud"),
        curr.get("feelslike_c"),
        curr.get("feelslike_f"),
        curr.get("windchill_c"),
        curr.get("windchill_f"),
        curr.get("heatindex_c"),
        curr.get("heatindex_f"),
        curr.get("dewpoint_c"),
        curr.get("dewpoint_f"),
        curr.get("vis_km"),
        curr.get("vis_miles"),
        curr.get("gust_mph"),
        curr.get("gust_kph"),
        curr.get("uv"),
        fetched_at,
    )
    cur.execute(INSERT_CURRENT_SQL, values)


def insert_forecast_weather(cur, city, forecast_data, gen_time, fetched_at):
    """Insert hourly forecast data for a city into the weather_forecast table."""
    for day in forecast_data["forecast"]["forecastday"]:
        for hour in day["hour"]:
            forecast_time = datetime.fromtimestamp(hour["time_epoch"])
            values = (
                city,
                forecast_time,
                hour.get("temp_c"),
                hour.get("temp_f"),
                hour.get("is_day"),
                hour["condition"]["text"],
                hour["condition"]["icon"],
                hour["condition"]["code"],
                hour.get("wind_mph"),
                hour.get("wind_kph"),
                hour.get("wind_degree"),
                hour.get("wind_dir"),
                hour.get("pressure_mb"),
                hour.get("pressure_in"),
                hour.get("precip_mm"),
                hour.get("precip_in"),
                hour.get("humidity"),
                hour.get("cloud"),
                hour.get("feelslike_c"),
                hour.get("feelslike_f"),
                hour.get("windchill_c"),
                hour.get("windchill_f"),
                hour.get("heatindex_c"),
                hour.get("heatindex_f"),
                hour.get("dewpoint_c"),
                hour.get("dewpoint_f"),
                hour.get("will_it_rain"),
                hour.get("chance_of_rain"),
                hour.get("vis_km"),
                hour.get("vis_miles"),
                hour.get("gust_mph"),
                hour.get("gust_kph"),
                hour.get("uv"),
                gen_time,
                fetched_at,
            )
            cur.execute(INSERT_FORECAST_SQL, values)


def backfill_missing_data(cur, city, context):
    """Backfill missing current weather data (15-min resolution, up to 6 hours back)."""
    backfill_count = 0
    for offset in range(1, 6 * 4 + 1):
        check_time = (datetime.utcnow() - timedelta(minutes=offset * 15)).replace(
            second=0, microsecond=0
        )
        cur.execute(
            "SELECT 1 FROM reporting_data.weather_current WHERE city = %s AND as_of = %s",
            (city, check_time),
        )
        if not cur.fetchone():
            hist_data = fetch_weather_data(city, {"dt": check_time}, forecast=False)
            for hour in (
                hist_data.get("forecast", {}).get("forecastday", [])[0].get("hour", [])
            ):
                if datetime.fromtimestamp(hour["time_epoch"]) == check_time:
                    insert_current_weather(
                        cur, city, {"current": hour}, datetime.utcnow()
                    )
                    backfill_count += 1
    return backfill_count


@asset(required_resource_keys={"pg_conn"})
def test_db_connection(context):
    """Check if a working PostgreSQL connection can be established."""
    try:
        conn = context.resources.pg_conn
        context.log.info("DB connection successful.")
        return True
    except Exception as e:
        context.log.error(f"DB connection failed: {e}")
        raise


@asset(required_resource_keys={"pg_conn"}, deps=[test_db_connection])
def create_tables(context):
    """Create database schema and weather tables from SQL script if not already present."""
    conn = context.resources.pg_conn
    cur = conn.cursor()
    try:
        with open(SQL_PATH, "r") as f:
            sql = f.read()
        for statement in [s for s in sql.split(";") if s.strip()]:
            cur.execute(statement)
        conn.commit()
        context.log.info("Tables and indexes created or already exist.")
        return True
    except Exception as e:
        context.log.error(f"Error creating tables: {e}")
        raise
    finally:
        cur.close()


@asset(required_resource_keys={"pg_conn"})
def fetch_and_store_all_cities(context):
    """Fetch and store current + forecast data for all cities, including backfilling missing records."""
    conn = context.resources.pg_conn
    cur = conn.cursor()
    errors = []
    total_backfill = 0

    for city in CITIES:
        try:
            context.log.info(f"Fetching weather for {city}")
            data = fetch_weather_data(city, context)
            fetched_at = datetime.utcnow()
            insert_current_weather(cur, city, data, fetched_at)
            insert_forecast_weather(cur, city, data, datetime.utcnow(), fetched_at)
            backfill_count = backfill_missing_data(cur, city, context)
            total_backfill += backfill_count
            context.log.info(f"{city} processed with {backfill_count} backfilled.")
        except Exception as e:
            context.log.error(f"Error processing {city}: {e}")
            errors.append(f"{city}: {e}")
        finally:
            conn.commit()

    cur.close()
    conn.close()
    context.log.info(f"Total backfilled: {total_backfill}")
    if errors:
        raise Failure(f"Errors occurred: {errors}")
    return True
