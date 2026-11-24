"""Tuning parameters, search levels, and profile application.

The defaults in ``DEFAULT_PARAMS`` match the values hardcoded in the
implementations before tuning was introduced. A profile therefore only
changes the knobs it explicitly names.
"""

from dataclasses import replace

from run_all.config import Mode


DEFAULT_PARAMS = {
    "batchsize": 500,
    "queuesize": 10,
    "bulksize": 250000,
    "fetchsize": 5000,
    "heap": "4g",
}

LEVELS = {
    "batchsize": [250, 500, 1000, 2500],
    "queuesize": [10, 50, 100, 200],
    "bulksize": [50000, 100000, 250000, 500000],
    "fetchsize": [1000, 5000, 20000, 100000],
    "heap": ["4g", "6g", "8g"],
}

ENV_NAMES = {
    "batchsize": "ETL_BATCHSIZE",
    "queuesize": "ETL_QUEUESIZE",
    "bulksize": "ETL_BULKSIZE",
    "fetchsize": "ETL_FETCHSIZE",
}

# These legacy Jython names would otherwise override the tuned values.
STALE_ENV = tuple(ENV_NAMES.values()) + (
    "JYTHON_BATCHSIZE",
    "JYTHON_QUEUESIZE",
    "JYTHON_PROCESS_BATCHSIZE",
    "JYTHON_PROCESS_QUEUESIZE",
)

MODE_KNOBS = {
    "cpygrametl1_db": ("bulksize", "fetchsize"),
    "cpygrametl2_db": ("batchsize", "queuesize", "bulksize", "fetchsize"),
    "cpygrametl1_async_db": ("bulksize", "fetchsize"),
    "cpygrametl2_async_db": ("batchsize", "queuesize", "bulksize", "fetchsize"),
    "pipeline sequential": ("bulksize", "fetchsize"),
    "pipeline multiprocessing": (
        "batchsize",
        "queuesize",
        "bulksize",
        "fetchsize",
    ),
    "pipeline free-threading": (
        "batchsize",
        "queuesize",
        "bulksize",
        "fetchsize",
    ),
    "jython1": ("bulksize", "fetchsize", "heap"),
    "jython2": ("batchsize", "queuesize", "bulksize", "fetchsize", "heap"),
}


def knobs_for(mode: Mode):
    """Return the knobs that apply to one mode, in a stable order."""
    try:
        return MODE_KNOBS[mode.implementation]
    except KeyError:
        raise ValueError(
            f"No tuning knobs declared for implementation {mode.implementation!r}"
        ) from None


def default_params(mode: Mode):
    """Return the starting parameters for one mode."""
    return {knob: DEFAULT_PARAMS[knob] for knob in knobs_for(mode)}


def params_to_env(params):
    """Translate parameter names into child-process environment variables."""
    return {
        ENV_NAMES[name]: str(value)
        for name, value in params.items()
        if name in ENV_NAMES
    }


def build_environment(base, overrides):
    """Build a child environment where a profile fully controls the knobs."""
    environment = dict(base)

    for name in STALE_ENV:
        environment.pop(name, None)

    environment.update(overrides)
    return environment


def apply_params(mode: Mode, params, profile="tuned"):
    """Return a copy of mode with parameter overrides applied."""
    if not params:
        return mode

    environment = dict(mode.env)
    environment.update(params_to_env(params))

    heap = params.get("heap") if mode.runtime.startswith("jython") else None

    return replace(
        mode,
        env=environment,
        heap=heap if heap is not None else mode.heap,
        params=dict(params),
        profile=profile,
    )
