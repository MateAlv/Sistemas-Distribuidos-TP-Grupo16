def notebook_bank_id(value: str) -> str:
    """Match pandas read_csv numeric inference for bank id columns."""
    value = (value or "").strip()
    if not value:
        return ""
    if value.isdigit():
        return str(int(value))
    return value
