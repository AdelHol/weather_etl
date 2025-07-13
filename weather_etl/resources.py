import os
import psycopg2
from dotenv import load_dotenv
from dagster import resource

load_dotenv()


@resource
def pg_conn_resource(init_context):
    db_params = dict(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", 5432)),
    )

    missing = [k for k, v in db_params.items() if v is None]
    if missing:
        raise ValueError(f"Missing DB environment variables: {', '.join(missing)}")

    conn = psycopg2.connect(**db_params)
    try:
        yield conn
    finally:
        conn.close()
