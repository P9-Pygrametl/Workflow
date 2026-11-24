"""Read the tuning parameters shared by every implementation."""

import os


def get_int(name, default):
    """Return an integer environment override, or the default value."""
    raw = os.getenv(name)

    if raw is None or raw == "":
        return default

    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from None

    if value < 1:
        raise ValueError(f"{name} must be positive, got {value}")

    return value
