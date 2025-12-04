# data_reader.py
from typing import Iterator, Dict
import csv


def read_movies(csv_path: str) -> Iterator[Dict[str, str]]:
    """
    Read the raw IMDB CSV and yield each row as a dict.

    Parameters
    ----------
    csv_path : str
        Path to the IMDB CSV file, e.g. "imdb_movie_dataset.csv".

    Yields
    ------
    Dict[str, str]
        One dictionary per movie row, with keys like:
        "Rank", "Title", "Genre", "Description", "Director", "Actors",
        "Year", "Runtime (Minutes)", "Rating", "Votes",
        "Revenue (Millions)", "Metascore".
    """
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Each row is already a dict from column name -> string value
            yield row
