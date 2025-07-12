# weather_etl/resources.py
import psycopg2
from dagster import resource


@resource
def pg_conn_resource(init_context):
    db_params = dict(
        dbname="weatherdb",
        user="weather_user",
        password="supersecret",
        host="localhost",  # nebo 'weather_pg' v docker-compose
        port=5432,
    )
    conn = psycopg2.connect(**db_params)
    try:
        yield conn
    finally:
        conn.close()
