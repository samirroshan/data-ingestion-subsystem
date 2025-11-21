# db.py
from contextlib import contextmanager
import logging

import psycopg2

logger = logging.getLogger(__name__)


@contextmanager
def get_connection(db_config: dict):
    """
    Open a PostgreSQL connection using settings from config.yaml
    and always close it when done.
    """
    conn = None
    try:
        conn = psycopg2.connect(
            host=db_config["host"],
            port=db_config["port"],
            dbname=db_config["dbname"],
            user=db_config["user"],
            password=db_config["password"],
        )
        logger.info("Opened database connection to %s", db_config["dbname"])
        yield conn
    except Exception:
        logger.exception("Error while connecting to the database")
        raise
    finally:
        if conn is not None:
            conn.close()
            logger.info("Database connection closed")


def create_tables(conn):
    """
    Create tables needed by the IMDB ingestion project.

    `stg_movies` and `stg_rejects` are managed by load_imdb.py /
    pandas, so this function only ensures the extra audit table
    `rejects_raw` exists.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rejects_raw (
                id           SERIAL PRIMARY KEY,
                source_file  TEXT,
                raw_record   JSONB,
                error_reason TEXT NOT NULL,
                rejected_at  TIMESTAMPTZ DEFAULT now()
            );
            """
        )

    conn.commit()
    logger.info("Ensured rejects_raw table exists")
