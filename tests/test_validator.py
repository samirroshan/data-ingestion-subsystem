# tests/test_validator.py
import os
import sys

# Make sure the project root (where validator.py lives) is on sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import src.validator as validator


def test_valid_movie_passes_validation():
    """A fully valid movie row should pass with no errors."""
    row = {
        "Title": "Guardians of the Galaxy",
        "Rank": "1",
        "Year": "2014",
        "Runtime (Minutes)": "121",
        "Rating": "8.1",
        "Votes": "757074",
        "Revenue (Millions)": "333.13",
        "Metascore": "76",
    }

    is_valid, error_reason = validator.validate_movie(row)

    assert is_valid is True
    assert error_reason == ""


def test_missing_revenue_is_rejected():
    """Missing Revenue (Millions) should cause validation to fail."""
    row = {
        "Title": "Mindhorn",
        "Rank": "8",
        "Year": "2016",
        "Runtime (Minutes)": "89",
        "Rating": "6.4",
        "Votes": "2490",
        "Revenue (Millions)": "",  # <- missing
        "Metascore": "60",
    }

    is_valid, error_reason = validator.validate_movie(row)

    assert is_valid is False
    assert "Missing Revenue" in error_reason


def test_rating_out_of_range_is_rejected():
    """Rating above 10 should be rejected."""
    row = {
        "Title": "Fake Overrated Movie",
        "Rank": "999",
        "Year": "2020",
        "Runtime (Minutes)": "100",
        "Rating": "11.0",  # <- invalid
        "Votes": "1000",
        "Revenue (Millions)": "10.0",
        "Metascore": "50",
    }

    is_valid, error_reason = validator.validate_movie(row)

    assert is_valid is False
    assert "Rating out of range" in error_reason
