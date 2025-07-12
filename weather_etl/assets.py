# weather_etl/assets.py

import os
from datetime import datetime
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
    """Ověří DB připojení."""
    try:
        conn = context.resources.pg_conn
        context.log.info("DB connection successful.")
        return True
    except Exception as e:
        context.log.error(f"DB connection failed: {e}")
        raise


@asset(required_resource_keys={"pg_conn"}, deps=[test_db_connection])
def create_tables(context):
    """Vytvoří schéma a tabulky podle SQL souboru."""
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
    """Stáhne aktuální i forecast počasí pro všechna města a uloží do DB."""
    conn = context.resources.pg_conn
    cur = conn.cursor()
    errors = []
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
            # Forecast počasí (každá hodina)
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
            conn.commit()
            context.log.info(f"Inserted data for {city}")
        except Exception as e:
            context.log.error(f"Error for {city}: {e}")
            errors.append(f"{city}: {e}")
    cur.close()
    conn.close()
    if errors:
        raise Failure(f"Errors occurred for cities: {errors}")
    return True
