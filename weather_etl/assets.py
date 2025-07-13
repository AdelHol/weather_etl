
import os
from datetime import datetime, timedelta
import logging
import requests
from dagster import asset, Failure

API_KEY = "26769031bd1844a6a6d130049250907"
CITIES = ["Prague", "London"]

SQL_PATH = os.path.join(os.path.dirname(__file__), "sql/create_schema_and_tables.sql")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asset(required_resource_keys={"pg_conn"})
def test_db_connection(context):
    """Verify database connection."""
    try:
        conn = context.resources.pg_conn
        context.log.info("DB connection successful.")
        return True
    except Exception as e:
        context.log.error(f"DB connection failed: {e}")
        raise


@asset(required_resource_keys={"pg_conn"}, deps=[test_db_connection])
def create_tables(context):
    """Create schema and tables based on SQL file."""
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
    """Fetch current and forecast weather for all cities and store in DB, including historical backfill if missing."""
    conn = context.resources.pg_conn
    cur = conn.cursor()
    errors = []
    backfill_count = 0

    for city in CITIES:
        try:
            context.log.info(f"Fetching weather for {city}")
            url = "http://api.weatherapi.com/v1/forecast.json"
            params = {"key": API_KEY, "q": city, "days": 2, "aqi": "no", "alerts": "no"}
            resp = requests.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

            fetched_at = datetime.utcnow()
            curr = data["current"]
            as_of = datetime.fromtimestamp(curr["last_updated_epoch"])
            cur.execute(
                """
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
                """,
                (
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
                ),
            )

            gen_time = datetime.utcnow()
            for day in data["forecast"]["forecastday"]:
                for hour in day["hour"]:
                    f_for = datetime.fromtimestamp(hour["time_epoch"])
                    cur.execute(
                        """
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
                        """,
                        (
                            city,
                            f_for,
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
                        ),
                    )

            # Extended backfill for 15min-resolution data up to 6 hours back
            for offset in range(1, 6 * 4 + 1):  # every 15 min for 6 hours
                quarter_ts = datetime.utcnow() - timedelta(minutes=offset * 15)
                check_time = quarter_ts.replace(second=0, microsecond=0)
                cur.execute(
                    "SELECT 1 FROM reporting_data.weather_current WHERE city = %s AND as_of = %s",
                    (city, check_time),
                )
                if not cur.fetchone():
                    hist_url = "http://api.weatherapi.com/v1/history.json"
                    hist_params = {
                        "key": API_KEY,
                        "q": city,
                        "dt": check_time.strftime("%Y-%m-%d"),
                        "hour": check_time.hour,
                    }
                    hist_resp = requests.get(hist_url, params=hist_params)
                    hist_resp.raise_for_status()
                    hist_data = hist_resp.json()
                    if hist_data.get("forecast", {}).get("forecastday"):
                        for hour_data in hist_data["forecast"]["forecastday"][0].get(
                            "hour", []
                        ):
                            time_epoch = datetime.fromtimestamp(hour_data["time_epoch"])
                            if time_epoch == check_time:
                                context.log.info(
                                    f"Backfilled missing 15-min record for {city} at {check_time}"
                                )
                                backfill_count += 1
                                cur.execute(  # same insert as above
                                    """
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
                                    """,
                                    (
                                        city,
                                        check_time,
                                        hour_data.get("temp_c"),
                                        hour_data.get("temp_f"),
                                        hour_data.get("is_day"),
                                        hour_data["condition"]["text"],
                                        hour_data["condition"]["icon"],
                                        hour_data["condition"]["code"],
                                        hour_data.get("wind_mph"),
                                        hour_data.get("wind_kph"),
                                        hour_data.get("wind_degree"),
                                        hour_data.get("wind_dir"),
                                        hour_data.get("pressure_mb"),
                                        hour_data.get("pressure_in"),
                                        hour_data.get("precip_mm"),
                                        hour_data.get("precip_in"),
                                        hour_data.get("humidity"),
                                        hour_data.get("cloud"),
                                        hour_data.get("feelslike_c"),
                                        hour_data.get("feelslike_f"),
                                        hour_data.get("windchill_c"),
                                        hour_data.get("windchill_f"),
                                        hour_data.get("heatindex_c"),
                                        hour_data.get("heatindex_f"),
                                        hour_data.get("dewpoint_c"),
                                        hour_data.get("dewpoint_f"),
                                        hour_data.get("vis_km"),
                                        hour_data.get("vis_miles"),
                                        hour_data.get("gust_mph"),
                                        hour_data.get("gust_kph"),
                                        hour_data.get("uv"),
                                        fetched_at,
                                    ),
                                )
            conn.commit()
            context.log.info(
                f"Inserted weather + forecast data for {city}. Backfilled {backfill_count} records."
            )
        except Exception as e:
            context.log.error(f"Error for {city}: {e}")
            errors.append(f"{city}: {e}")

    context.log.info(f"Total backfilled records across cities: {backfill_count}")
    cur.close()
    conn.close()
    if errors:
        raise Failure(f"Errors occurred for cities: {errors}")
    return True
