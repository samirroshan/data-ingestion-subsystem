# validator.py
# validator.py
"""
Row-level validator for IMDB movie records.

Checks required fields (Title, Rank, Year, Runtime, Rating, Votes,
Revenue, Metascore) for presence, correct types, and reasonable ranges.
Returns a (is_valid, error_reason) tuple so the ingestion pipeline can
either load clean rows or route bad ones into rejection paths with a
human-readable error summary.
"""

def validate_movie(row: dict) -> tuple[bool, str]:
    """
    Return (is_valid, error_reason).
    error_reason is empty string if the row is valid.
    """
    errors = []

    # ---- Title (required) ----
    title = (row.get("Title") or "").strip()
    if not title:
        errors.append("Missing Title")

    # ---- Rank (required, positive int) ----
    try:
        rank_val = int(row.get("Rank"))
        if rank_val <= 0:
            errors.append("Rank must be positive")
    except (TypeError, ValueError):
        errors.append("Rank is not an integer")

    # ---- Year (required, within reasonable range) ----
    try:
        year_val = int(row.get("Year"))
        if year_val < 1900 or year_val > 2030:
            errors.append("Year out of allowed range")
    except (TypeError, ValueError):
        errors.append("Year is not an integer")

    # ---- Runtime (Minutes) – required, sane range ----
    runtime_raw = (row.get("Runtime (Minutes)") or "").strip()
    if runtime_raw == "":
        errors.append("Missing Runtime")
    else:
        try:
            runtime_val = int(runtime_raw)
            if runtime_val <= 0 or runtime_val > 400:
                errors.append("Runtime out of range 1–400 minutes")
        except ValueError:
            errors.append("Runtime is not an integer")

    # ---- Rating – required, 0–10 ----
    rating_raw = (row.get("Rating") or "").strip()
    if rating_raw == "":
        errors.append("Missing Rating")
    else:
        try:
            rating_val = float(rating_raw)
            if rating_val < 0 or rating_val > 10:
                errors.append("Rating out of range 0–10")
        except ValueError:
            errors.append("Rating is not a number")

    # ---- Votes – required, non-negative integer ----
    votes_raw = (row.get("Votes") or "").strip()
    if votes_raw == "":
        errors.append("Missing Votes")
    else:
        try:
            votes_val = int(votes_raw)
            if votes_val < 0:
                errors.append("Votes must be non-negative")
        except ValueError:
            errors.append("Votes is not an integer")

    # ---- Revenue (Millions) – required, non-negative ----
    revenue_raw = (row.get("Revenue (Millions)") or "").strip()
    if revenue_raw == "":
        errors.append("Missing Revenue")
    else:
        try:
            revenue_val = float(revenue_raw)
            if revenue_val < 0:
                errors.append("Revenue must be non-negative")
        except ValueError:
            errors.append("Revenue is not a number")

    # ---- Metascore – required, 0–100 ----
    metascore_raw = (row.get("Metascore") or "").strip()
    if metascore_raw == "":
        errors.append("Missing Metascore")
    else:
        try:
            metascore_val = int(metascore_raw)
            if metascore_val < 0 or metascore_val > 100:
                errors.append("Metascore out of range 0–100")
        except ValueError:
            errors.append("Metascore is not an integer")

    # ---- Final decision ----
    if errors:
        return False, "; ".join(errors)

    return True, ""
