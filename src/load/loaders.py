# loaders.py
from typing import Iterable, Dict, Any
import json
import logging

logger = logging.getLogger(__name__)
"""
Loader utilities for rejected IMDB records.

Provides a helper to bulk-insert invalid movie rows into the rejects_raw
audit table in PostgreSQL, storing the source file, full raw record as JSON,
and the associated validation error reason, with simple logging for observability.
"""


def insert_rejects(conn, rejects: Iterable[Dict[str, Any]]) -> None:
    """
    Insert invalid records into the `rejects_raw` table.

    Each reject dict is expected to have:
      - "source_file": str
      - "raw_record": dict (the original row)
      - "error_reason": str
    """
    rejects = list(rejects)
    if not rejects:
        logger.info("No rejects to insert into database")
        return

    rows = [
        (
            r.get("source_file"),
            json.dumps(r.get("raw_record", {})),
            r.get("error_reason", "Unknown error"),
        )
        for r in rejects
    ]

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO rejects_raw (source_file, raw_record, error_reason)
            VALUES (%s, %s, %s);
            """,
            rows,
        )

    conn.commit()
    logger.info("Inserted %d rejected rows into rejects_raw table", len(rows))
