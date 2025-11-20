# data_reader.py
import csv


def read_imdb_csv(path: str):
    """
    Read the IMDb CSV into a list of dicts.
    Each dict is one movie row, with column names as keys.
    """
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)
