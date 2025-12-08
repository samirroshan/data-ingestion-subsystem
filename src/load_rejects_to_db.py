# load_rejects_to_db.py
import csv
import logging
import yaml

from src.db import get_connection, create_tables
from src.loaders import insert_rejects
from src.logging_config import setup_logging


logger = logging.getLogger(__name__)


def main():
    # 1) Configure logging (file + console)
    setup_logging()

    # 2) Load DB + paths config
    with open("config.yaml") as f:
        cfg = yaml.safe_load(f)

    logger.info("Starting load_rejects_to_db run")

    # 3) Read rejected_rows.csv
    rejects = []
    rejected_csv_path = cfg["paths"]["rejected_csv"]

    with open(rejected_csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rejects.append(
                {
                    "source_file": row.get("source_file", rejected_csv_path),
                    "raw_record": row,  # store whole CSV row as JSONB
                    "error_reason": row.get("error_reason", ""),
                }
            )

    logger.info("Read %d rejected rows from %s", len(rejects), rejected_csv_path)

    # 4) Insert into DB
    with get_connection(cfg["db"]) as conn:
        create_tables(conn)
        insert_rejects(conn, rejects)

    logger.info("Finished load_rejects_to_db run")


if __name__ == "__main__":
    main()
