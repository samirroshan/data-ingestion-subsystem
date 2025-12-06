# ingestion_flow.py
import json
import psycopg2

from data_reader import read_movies
from validator import validate_movie


# --- DB connection details (same as DBeaver) ---
DB_NAME = "ingestion"
DB_USER = "revature"
DB_PASSWORD = "12345"   # <-- change to your actual password if different
DB_HOST = "localhost"
DB_PORT = 5432


def to_int(value):
    if value is None or str(value).strip() == "":
        return None
    return int(value)


def to_float(value):
    if value is None or str(value).strip() == "":
        return None
    return float(value)


def run_ingestion(path: str):
    # 1. Read the data
    rows = read_movies(path)
    print(f"Read {len(rows)} rows from {path}")

    # 2. Connect to Postgres
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
        # 3. Validate row
        is_valid, error_reason = validate_movie(r)

        if not is_valid:
            # bad row -> stg_rejects
            cur.execute(
                reject_sql,
                (
                    path,          # source_file
                    json.dumps(r), # raw_record
                    error_reason,  # error_reason
                ),
            )
            rejected += 1
            continue

        # 4. Transform + insert row
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

    print(f"Inserted {inserted} rows into stg_movies")
    print(f"Rejected {rejected} rows into stg_rejects")


if __name__ == "__main__":
    run_ingestion("imdb_movie_dataset.csv")
