# tests/test_data_reader.py
import os
import sys

# Make sure we can import data_reader from project root
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import src.data_reader as data_reader


def test_read_movies_returns_rows():
    """Basic sanity check that the CSV reader returns movie rows."""
    # ⚠️ Adjust this call to match your actual function name.
    # For example, if you have read_movies or read_csv, change it here.
    rows = list(data_reader.read_movies("imdb_movie_dataset.csv"))

    # There should be at least one row
    assert len(rows) > 0

    first = rows[0]
    # These keys should match your CSV headers
    assert "Title" in first
    assert "Year" in first
    assert "Rating" in first
