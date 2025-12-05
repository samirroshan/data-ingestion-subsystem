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
