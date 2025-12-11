# tests/test_data_reader.py
import os
import sys

"""
Pytest suite for the data_reader module.

Verifies that read_movies successfully reads the IMDB CSV file and
returns a non-empty list of row dicts with expected columns such as "Title".
"""

# Make sure we can import data_reader from project root
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import src.data_reader as data_reader


def test_read_movies_returns_rows():
    csv_path = "data/imdb_movie_dataset.csv"
    rows = list(data_reader.read_movies(csv_path))

    # basic sanity checks
    assert len(rows) > 0
    assert "Title" in rows[0]
