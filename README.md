# IMDB Data Ingestion Subsystem

This project is a small ETL-style subsystem that ingests an IMDB movie dataset from CSV, validates each row, and loads clean and rejected records into PostgreSQL and audit files.

It demonstrates:
- CSV ingestion and validation
- Separation of clean vs invalid data
- Loading rejected records into an audit table
- Centralized logging
- Automated tests with pytest and coverage

---

## Architecture

**Input**

- `imdb_movie_dataset.csv` – raw IMDB movie data

**Core components**

- `data_reader.py`  
  - `read_movies(csv_path)` – streams rows from the raw CSV as dictionaries.

- `validator.py`  
  - `validate_movie(row)` – applies business rules:
    - Required fields: Title, Rank, Year, Runtime, Rating, Votes, Revenue, Metascore  
    - Type checks (int/float) and value ranges (e.g., Rating 0–10, Metascore 0–100, non-negative Revenue/Votes)
  - Returns `(is_valid: bool, error_reason: str)`.

- `load_imdb.py`  
  - Reads `imdb_movie_dataset.csv`
  - Validates each row with `validate_movie`
  - Writes:
    - **Valid** rows → `stg_movies` table in PostgreSQL  
    - **Rejected** rows → `stg_rejects` table in PostgreSQL  
    - **All cleaned movies** (deduped from DB) → `clean_imdb_movies.csv`
    - **Row-level reject audit file** → `rejected_rows.csv`
  - Logs every rejected row and a summary of counts.

- `db.py`  
  - `get_connection(db_config)` – context manager for PostgreSQL connections using `psycopg2`.
  - `create_tables(conn)` – ensures `rejects_raw` audit table exists.

- `loaders.py`  
  - `insert_rejects(conn, rejects)` – bulk-inserts rejected rows into `rejects_raw` audit table.

- `load_rejects_to_db.py`  
  - Reads `rejected_rows.csv`
  - Loads all rejected rows into `rejects_raw` for long-term audit/analytics.

- `logging_config.py`  
  - Centralized logging configuration for console + file logging.
  - All scripts log to `ingestion.log`.

---

## Data Flow

1. **Raw CSV → Staging / Rejects**
   - `python load_imdb.py`
   - Valid rows → `stg_movies`
   - Invalid rows → `stg_rejects`
   - All rejected rows also written to `rejected_rows.csv`
   - Cleaned dataset exported to `clean_imdb_movies.csv`

2. **Rejected CSV → Audit Table**
   - `python load_rejects_to_db.py`
   - Loads `rejected_rows.csv` into `rejects_raw` with:
     - `source_file`
     - `raw_record` (JSONB)
     - `error_reason`
     - `rejected_at` timestamp

3. **Logging**
   - Every rejected row is logged with:
     - timestamp
     - source file
     - movie title / rank
     - error reason
   - Pipeline summary and DB interactions are logged to `ingestion.log`.

---

## Tech Stack

- **Language:** Python 3.13
- **Database:** PostgreSQL 15 (local)
- **Libraries:**
  - `pandas`
  - `psycopg2-binary`
  - `PyYAML`
  - `pytest`, `pytest-cov` for testing

---

## Configuration

Configuration is stored in `config.yaml`:

```yaml
paths:
  source_csv: imdb_movie_dataset.csv
  clean_csv: clean_imdb_movies.csv
  rejected_csv: rejected_rows.csv
  log_file: ingestion.log

db:
  host: localhost
  port: 5432
  dbname: ingestion
  user: revature
  password: YOUR_PASSWORD_HERE

database:   # used by load_imdb.py (same DB as above)
  name: ingestion
  user: revature
  password: YOUR_PASSWORD_HERE
  host: localhost
  port: 5432


In terminal:
    run "pytest"