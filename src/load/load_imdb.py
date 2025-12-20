# load_imdb.py
import csv
import json
import os
import yaml

from pyspark.sql import SparkSession 
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType

import logging
# from validator import validate_movie # We will use Spark-native validation
from src.Main.logging_config import setup_logging

"""
IMDB ingestion and export script driven by config.yaml.

Reads raw movie rows from the source CSV, validates each record, and loads
cleaned data into the stg_movies table in PostgreSQL while logging invalid
rows to both a rejects table and a separate CSV + log file. Also supports
exporting all cleaned movies from stg_movies into outputs/clean_imdb_movies.csv
for downstream analysis.
"""

# Determine the project root (one level up from src/)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Determine the absolute path to the jar file
JAR_PATH = os.path.join(PROJECT_ROOT, "postgresql-42.7.3.jar")

spark = SparkSession.builder \
    .appName("IMDB_Ingestion_Spark") \
    .config("spark.driver.extraClassPath", JAR_PATH) \
    .config("spark.jars", JAR_PATH) \
    .config("spark.sql.ansi.enabled", "false") \
    .getOrCreate()


# --- Load configuration from YAML ---
CONFIG_PATH = os.path.join(PROJECT_ROOT, "config", "config.yaml")

if not os.path.exists(CONFIG_PATH):
    # Fallback to just "config/config.yaml" if running from root and logic fails
    CONFIG_PATH = "config/config.yaml"

# Pass the correct config path to setup_logging
setup_logging(CONFIG_PATH)
logger = logging.getLogger(__name__)

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

# DB config
DB_NAME = config["database"]["name"]
DB_USER = config["database"]["user"]
DB_PASSWORD = str(config["database"]["password"])
DB_HOST = config["database"]["host"]
DB_PORT = config["database"]["port"]

# Paths config
# Resolve paths relative to PROJECT_ROOT just in case, or rely on them being relative to CWD if running from root.
# However, user runs from src/, so relative paths in config (like "data/...") might break if they assume CWD=root.
# Let's check config content again. config says:
#   source_csv: data/imdb_movie_dataset.csv
# If running from src/, "data/..." isn't there. It's "../data/..."
# We should probably fix CWD or resolve these paths.

SOURCE_CSV = config["paths"]["source_csv"]
REJECTED_CSV = config["paths"]["rejected_csv"]
LOG_FILE = config["paths"]["log_file"]

# If paths are relative, make them absolute based on PROJECT_ROOT
if not os.path.isabs(SOURCE_CSV):
    SOURCE_CSV = os.path.join(PROJECT_ROOT, SOURCE_CSV)
if not os.path.isabs(REJECTED_CSV):
    REJECTED_CSV = os.path.join(PROJECT_ROOT, REJECTED_CSV)
if not os.path.isabs(LOG_FILE):
    LOG_FILE = os.path.join(PROJECT_ROOT, LOG_FILE)

# --- set up logging to a file ---
# Re-configure basic config? logging_config.py does basicConfig.
# But we might need to update the filename if it was relative.
# logging_config.py reads the config itself.
# We passed CONFIG_PATH to setup_logging.
# setup_logging reads config, gets log_file, and does Path(log_file).
# If log_file in config is "logs/ingestion.log" and we are in "src/", Path("logs/...") will try "src/logs/..." which might be wrong.
# "logs" is in root.
# So we need to ensure setup_logging handles paths correctly too?
# Or just chdir to project root?
# Changing CWD is risky but often easiest for "run from anywhere" scripts relying on relative paths.
# Let's try fixing paths safely first.

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    force=True # Force update if setup_logging did something wrong?
)
logger = logging.getLogger(__name__)

def validate_movie_spark(df):
    """
    Validates the dataframe using PySpark functions.
    Matches logic in validator.py
    """
    
    # Helper conditions
    # Title: required
    cond_title_missing = (F.col("Title").isNull()) | (F.trim(F.col("Title")) == "")
    
    # Rank: positive int
    cond_rank_invalid = (
        F.col("Rank").cast("int").isNull() | 
        (F.col("Rank").cast("int") <= 0)
    )

    # Year: 1900-2030
    cond_year_invalid = (
        F.col("Year").cast("int").isNull() |
        (~F.col("Year").cast("int").between(1900, 2030))
    )

    # Runtime: 1-400
    cond_runtime_invalid = (
        (F.col("Runtime (Minutes)").isNotNull()) & (F.trim(F.col("Runtime (Minutes)")) != "") & 
        (
             F.col("Runtime (Minutes)").cast("int").isNull() |
             (~F.col("Runtime (Minutes)").cast("int").between(1, 400))
        )
    )
    # Note: validator.py says required, so if empty/null it's also error
    cond_runtime_missing = (F.col("Runtime (Minutes)").isNull()) | (F.trim(F.col("Runtime (Minutes)")) == "")

    # Rating: 0-10
    cond_rating_missing = (F.col("Rating").isNull()) | (F.trim(F.col("Rating")) == "")
    cond_rating_invalid = (
        (F.col("Rating").isNotNull()) & (F.trim(F.col("Rating")) != "") &
        (
             F.col("Rating").cast("float").isNull() |
             (~F.col("Rating").cast("float").between(0, 10))
        )
    )

    # Votes: non-negative
    cond_votes_missing = (F.col("Votes").isNull()) | (F.trim(F.col("Votes")) == "")
    cond_votes_invalid = (
        (F.col("Votes").isNotNull()) & (F.trim(F.col("Votes")) != "") &
        (
             F.col("Votes").cast("int").isNull() |
             (F.col("Votes").cast("int") < 0)
        )
    )

    # Revenue: non-negative
    cond_revenue_missing = (F.col("Revenue (Millions)").isNull()) | (F.trim(F.col("Revenue (Millions)")) == "")
    cond_revenue_invalid = (
         (F.col("Revenue (Millions)").isNotNull()) & (F.trim(F.col("Revenue (Millions)")) != "") &
         (
              F.col("Revenue (Millions)").cast("float").isNull() |
              (F.col("Revenue (Millions)").cast("float") < 0)
         )
    )

    # Metascore: 0-100
    cond_metascore_missing = (F.col("Metascore").isNull()) | (F.trim(F.col("Metascore")) == "")
    cond_metascore_invalid = (
         (F.col("Metascore").isNotNull()) & (F.trim(F.col("Metascore")) != "") &
         (
              F.col("Metascore").cast("int").isNull() |
              (~F.col("Metascore").cast("int").between(0, 100))
         )
    )


    validated_df = df.withColumn("error_reason", 
        F.concat_ws("; ",
            F.when(cond_title_missing, "Missing Title"),
            F.when(cond_rank_invalid, "Rank invalid (must be positive integer)"),
            F.when(cond_year_invalid, "Year out of allowed range (1900-2030)"),
            F.when(cond_runtime_missing, "Missing Runtime"),
            F.when(cond_runtime_invalid, "Runtime out of range 1–400 minutes"),
            F.when(cond_rating_missing, "Missing Rating"),
            F.when(cond_rating_invalid, "Rating out of range 0–10"),
            F.when(cond_votes_missing, "Missing Votes"),
            F.when(cond_votes_invalid, "Votes must be non-negative"),
            F.when(cond_revenue_missing, "Missing Revenue"),
            F.when(cond_revenue_invalid, "Revenue must be non-negative"),
            F.when(cond_metascore_missing, "Missing Metascore"),
            F.when(cond_metascore_invalid, "Metascore out of range 0–100")
        )
    )

    validated_df = validated_df.withColumn("error_reason", F.trim(F.col("error_reason")))
    
    return validated_df

def load_imdb_spark(path: str | None = None):
    if path is None:
        path = SOURCE_CSV
    
    print(f"Reading from: {path}")
    # Enable robust CSV reading
    df = spark.read.csv(
        path, 
        header=True, 
        inferSchema=False, 
        quote="\"", 
        escape="\"",
        multiLine=True
    )

    row_count = df.count()
    print(f"Total rows read: {row_count}")
    return df

def process_and_split(df, path):

    df_with_errors = validate_movie_spark(df)
    
    # Valid = error_reason is empty string (or null, depending on concat_ws behavior with no matches? concat_ws returns "" if all nulls/empty)
    # Actually concat_ws returns empty string if no matches.
    
    valid_df = df_with_errors.filter((F.col("error_reason") == "") | (F.col("error_reason").isNull()))
    invalid_df = df_with_errors.filter((F.col("error_reason") != "") & (F.col("error_reason").isNotNull()))

    clean_df = valid_df.select(
        F.col("Rank").cast("int").alias("rank_num"),
        F.trim(F.col("Title")).alias("title"),
        F.trim(F.col("Genre")).alias("genre"),
        F.trim(F.col("Description")).alias("description"),
        F.trim(F.col("Director")).alias("director"),
        F.trim(F.col("Actors")).alias("actors"),
        F.col("Year").cast("int").alias("year"),
        F.col("Runtime (Minutes)").cast("int").alias("runtime_minutes"),
        F.col("Rating").cast("float").alias("rating"),
        F.col("Votes").cast("int").alias("votes"),
        F.col("Revenue (Millions)").cast("float").alias("revenue_millions"),
        F.col("Metascore").cast("int").alias("metascore")
    )

    rejects_to_load = invalid_df.select(
        F.lit(path).alias("source_file"),
        F.to_json(F.struct([F.col(c) for c in df.columns])).alias("raw_record"),
        F.col("error_reason")
    )

    return clean_df, rejects_to_load

def save_to_db_spark(clean_df, rejects_df):
    # 1. Define Connection Properties
    jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
    connection_properties = {
        "user": DB_USER,
        "password": DB_PASSWORD,
        "driver": "org.postgresql.Driver",
        "stringtype": "unspecified"
    }

    print("Writing valid records to stg_movies...")
    # 2. Write Clean Data to stg_movies
    # 'append' mode adds to the table without deleting old data
    clean_df.write.jdbc(
        url=jdbc_url, 
        table="stg_movies", 
        mode="append", 
        properties=connection_properties
    )

    print("Writing rejected records to stg_rejects...")
    # 3. Write Rejects to stg_rejects
    rejects_df.write.jdbc(
        url=jdbc_url, 
        table="stg_rejects", 
        mode="append", 
        properties=connection_properties
    )
    
    print("Successfully loaded data to PostgreSQL via Spark.")

def export_clean_movies_to_csv():
    #read from stg_movies and export to csv

    # global spark  # uses existing spark cond
    # Re-use the spark session created at module level

    jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
    properties = {
        "user": DB_USER,
        "password": DB_PASSWORD,
        "driver": "org.postgresql.Driver"
    }

    # we have to read tables into spark DF
    df = spark.read.jdbc(url=jdbc_url, table="stg_movies", properties=properties)

    #write DF to CSV
    output_folder = "outputs/clean_imdb_movies_spark"
    # Ensure raw output dir exists isn't strictly necessary for spark write, but 'outputs' param was wrong
    df.coalesce(1).write.option("header", True).mode("overwrite").csv(output_folder)
    print(f"Exported stg_movies to {output_folder}")

if __name__ == "__main__":
    logger.info("Starting the spark pipeline")
    
    # 1. Load Data
    raw_data_df = load_imdb_spark()
    
    # 2. Process
    clean_movies_df, rejected_rows_df = process_and_split(raw_data_df, SOURCE_CSV)

    # Show some info
    print("Clean Data Sample:")
    clean_movies_df.show(5)
    print("Rejected Data Sample:")
    rejected_rows_df.show(5, truncate=False)

    try:
        # 3. Save
        save_to_db_spark(clean_movies_df, rejected_rows_df)

        final_inserted = clean_movies_df.count()
        final_rejected = rejected_rows_df.count()
        
        print("Job Complete!")
        print(f"Successfully inserted: {final_inserted} rows")
        print(f"Rejected: {final_rejected} rows")
        logger.info("Run complete: inserted=%d, rejected=%d", final_inserted, final_rejected)
        
    except Exception as e:
        logger.error("Spark job failed: %s", str(e))
        print(f"Error: {e}")
        # raise e # optional, to fail the job explicitly
        
    spark.stop()