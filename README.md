# Data Ingestion Subsystem

The **Data Ingestion Subsystem** is a production-style ETL component built in Python that processes **IMDB Movie Rating Data** from CSV files. The dataset includes detailed movie information and rating metrics such as:

- Title  
- Genre  
- Description  
- Director  
- Cast  
- Release year  
- Runtime (minutes)  
- **IMDB rating (user score)**  
- **Votes (movie popularity)**  
- **Revenue (in millions USD)**  
- **Metascore (critic rating)**  

This subsystem performs:

- **Extraction** of raw IMDB movie rating data  
- **Validation & transformation** using a configurable schema  
- **Loading** into PostgreSQL staging and reject tables  
- **Error tracking** for invalid or inconsistent movie records  

It is designed to mimic how real data engineering teams handle ingestion and data quality pipelines.

---

## Features

- 🧾 **YAML-driven configuration** for flexible dataset definitions  
- ✅ **Row-level validation** with typed fields and error messages  
- 🧱 **Staging table** for clean, validated movie records  
- ❌ **Reject table** for invalid rows (with error reasons)  
- 🧪 **PyTest suite** for validation and reliability  
- 🔁 Can ingest additional datasets by simply creating new YAML configs  

---

## Dataset Description — IMDB Movie Rating Data

The system ingests **IMDB movie rating data**, a structured dataset containing:

- Rank  
- Movie title  
- Genre  
- Description  
- Director  
- Actors  
- Year of release  
- Runtime (minutes)  
- **IMDB user rating (float)**  
- **Number of votes submitted by users**  
- **Box office revenue in millions USD**  
- **Metascore (critic-based rating)**  

These fields are validated and cleaned using the schema defined in the YAML configuration file.

---

## How to RUN?
# 🚀 How to Run the IMDB Data Ingestion Pipeline

This project provides a complete ETL-style data ingestion subsystem for validating and loading IMDB movie data into a PostgreSQL database.

---

## 1. Clone the Repository

```bash
git clone https://github.com/samirroshan/data-ingestion-subsystem.git
cd data-ingestion-subsystem
```

---

## 2. Create & Activate the Virtual Environment

```bash
python -m venv venv
source venv/bin/activate      # macOS / Linux
# venv\Scripts\activate       # Windows
```

---

## 3. Install Dependencies

```bash
pip install -r requirements.txt
```

If no `requirements.txt` exists:

```bash
pip install pandas psycopg2-binary pyyaml pytest pytest-cov
```

---

## 4. Set Up PostgreSQL Tables

```bash
createdb imdb
```

Then open `psql` and run:

```sql
CREATE TABLE IF NOT EXISTS stg_movies (
    rank_num          INTEGER,
    title             TEXT,
    genre             TEXT,
    description       TEXT,
    director          TEXT,
    actors            TEXT,
    year              INTEGER,
    runtime_minutes   INTEGER,
    rating            DOUBLE PRECISION,
    votes             INTEGER,
    revenue_millions  DOUBLE PRECISION,
    metascore         DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS stg_rejects (
    id           SERIAL PRIMARY KEY,
    source_file  TEXT,
    raw_record   JSONB,
    error_reason TEXT,
    created_at   TIMESTAMP DEFAULT NOW()
);
```

---

## 5. Configure Your `config.yaml`

```yaml
database:
  name: imdb
  user: postgres
  password: your_password_here
  host: localhost
  port: 5432

paths:
  source_csv: data/imdb_movie_dataset.csv
  rejected_csv: outputs/rejected_rows.csv
  log_file: outputs/ingestion.log
```

Make sure the CSV exists at:

```
data/imdb_movie_dataset.csv
```

---

## 6. Run the Pipeline

From the project root:

```bash
PYTHONPATH=src python src/load_imdb.py
```

### Expected Output

```
Read 1000 rows from data/imdb_movie_dataset.csv
Inserted 838 rows into stg_movies
Rejected 162 rows into stg_rejects
Wrote 10056 cleaned rows to outputs/clean_imdb_movies.csv
```

### What Happens in the Run

- Extract raw CSV rows  
- Validate each row using custom business rules  
- Insert valid rows → `stg_movies`  
- Insert invalid rows → `stg_rejects` + CSV reject log  
- Generate analytics-ready clean dataset → `outputs/clean_imdb_movies.csv`

---

## 7. Run Unit Tests & Coverage

```bash
pytest --cov=src
```

---

## ✅ Pipeline Ready

You now have a fully functional, reproducible ETL ingestion subsystem.


## Project Structure

```text
data-ingestion-subsystem/
├── configs/
│   └── imdb_ingestion.yaml
├── data/
│   └── imdb_movie_dataset.csv
├── db/
│   └── db_connection.py
├── ingestion/
│   ├── validator.py
│   └── ingestion_service.py
├── migrations/
│   ├── create_staging_tables.sql
│   ├── create_reject_tables.sql
│   └── create_final_tables.sql
├── tests/
├── main.py
├── requirements.txt
└── README.md
