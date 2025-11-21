import csv
import json
import os
import pandas as pd 
import psycopg2
import yaml

from validator import validate_movie
import logging
from logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

# --- Load configuration from YAML ---
with open("config.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

# DB config
DB_NAME = config["database"]["name"]
DB_USER = config["database"]["user"]
DB_PASSWORD = config["database"]["password"]
DB_HOST = config["database"]["host"]
DB_PORT = config["database"]["port"]

# Paths config
SOURCE_CSV = config["paths"]["source_csv"]
REJECTED_CSV = config["paths"]["rejected_csv"]
LOG_FILE = config["paths"]["log_file"]

# --- set up logging to a file ---
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def to_int(value):
    if value is None or str(value).strip() == "":
        return None
    return int(value)


def to_float(value):
    if value is None or str(value).strip() == "":
        return None
    return float(value)


def load_imdb_csv(path: str | None = None):
    if path is None:
        path = SOURCE_CSV

    # --- 1. Read the CSV file ---
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    print(f"Read {len(rows)} rows from {path}")

    # --- 1b. Open a log file for rejected rows ---
    log_file_path = REJECTED_CSV
    log_exists = os.path.exists(log_file_path)

    log_f = open(log_file_path, "a", newline="", encoding="utf-8")
    log_writer = csv.writer(log_f)

    if not log_exists:
        log_writer.writerow(
            ["source_file", "rank", "title", "year", "rating", "votes", "error_reason"]
        )

    # --- 2. Connect to Postgres ---
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    cur = conn.cursor()

    insert_sql = """
        INSERT INTO stg_movies (
            rank_num,
            title,
            genre,
            description,
            director,
            actors,
            year,
            runtime_minutes,
            rating,
            votes,
            revenue_millions,
            metascore
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    reject_sql = """
        INSERT INTO stg_rejects (
            source_file,
            raw_record,
            error_reason
        )
        VALUES (%s, %s::jsonb, %s)
    """

    inserted = 0
    rejected = 0

    for r in rows:
        # --- 3. Validate the raw row first ---
        is_valid, error_reason = validate_movie(r)

        if not is_valid:
            # bad row -> stg_rejects + log file
            cur.execute(
                reject_sql,
                (
                    path,              # source_file
                    json.dumps(r),     # raw_record as JSON
                    error_reason,      # why it failed
                ),
            )
            rejected += 1

            # write to CSV log
            log_writer.writerow(
                [
                    path,
                    r.get("Rank"),
                    (r.get("Title") or "").strip(),
                    r.get("Year"),
                    r.get("Rating"),
                    r.get("Votes"),
                    error_reason,
                ]
            )

            # write to text log
            logger.warning(
                "Rejected row from %s (Rank=%s, Title=%s): %s",
                path,
                r.get("Rank"),
                (r.get("Title") or "").strip(),
                error_reason,
            )
            continue

        # --- 4. Convert types for valid rows ---
        rank_num = to_int(r.get("Rank"))
        title = (r.get("Title") or "").strip()
        genre = (r.get("Genre") or "").strip()
        description = (r.get("Description") or "").strip()
        director = (r.get("Director") or "").strip()
        actors = (r.get("Actors") or "").strip()
        year = to_int(r.get("Year"))
        runtime_minutes = to_int(r.get("Runtime (Minutes)"))
        rating = to_float(r.get("Rating"))
        votes = to_int(r.get("Votes"))
        revenue_millions = to_float(r.get("Revenue (Millions)"))
        metascore = to_float(r.get("Metascore"))

        cur.execute(
            insert_sql,
            (
                rank_num,
                title,
                genre,
                description,
                director,
                actors,
                year,
                runtime_minutes,
                rating,
                votes,
                revenue_millions,
                metascore,
            ),
        )
        inserted += 1

    conn.commit()
    cur.close()
    conn.close()
    log_f.close()

    print(f"Inserted {inserted} rows into stg_movies")
    print(f"Rejected {rejected} rows into stg_rejects")
    logger.info("Run complete: inserted=%d, rejected=%d", inserted, rejected)

def export_clean_movies_to_csv():
    """
    Read cleaned data from stg_movies and write it to clean_imdb_movies.csv
    """
    # connect using the same DB settings you already use
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )

    query = "SELECT * FROM stg_movies ORDER BY rank_num;"
    df = pd.read_sql(query, conn)
    conn.close()

    # write cleaned data to a new CSV file
    output_path = "clean_imdb_movies.csv"
    df.to_csv(output_path, index=False)

    print(f"Wrote {len(df)} cleaned rows to {output_path}")
    logger.info("Exported %d cleaned rows to %s", len(df), output_path)

if __name__ == "__main__":
    # adjust this if you’re reading the CSV path from config.yaml
    load_imdb_csv("imdb_movie_dataset.csv")
    export_clean_movies_to_csv()
