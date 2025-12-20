def to_int(value):
    if value is None or str(value).strip() == "":
        return None
    return int(value)


def to_float(value):
    if value is None or str(value).strip() == "":
        return None
    return float(value)
