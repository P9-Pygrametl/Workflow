
from collections import defaultdict
from functools import wraps
from time import perf_counter_ns


def instrument_scd(dimension):
    """Measure SCD method calls and SQL activity.

    Instruments only SQL executed while scdensure() is active.
    Assumes a single ETL processing thread.
    """
    stats = defaultdict(
        lambda: {"calls": 0, "nanoseconds": 0}
    )
    connection = dimension.targetconnection
    active_depth = 0

    def record(name, elapsed):
        stats[name]["calls"] += 1
        stats[name]["nanoseconds"] += elapsed

    # Instrument warehouse SQL execution.
    original_execute = connection.execute

    @wraps(original_execute)
    def measured_execute(*args, **kwargs):
        sql = args[0] if args else kwargs.get("stmt", "")
        sql_text = str(sql).strip()
        operation = (
            sql_text.split(None, 1)[0].upper()
            if sql_text else "UNKNOWN"
        )

        # Only count SQL originating from scdensure().
        measuring = active_depth > 0

        if not measuring:
            return original_execute(*args, **kwargs)

        start = perf_counter_ns()
        try:
            return original_execute(*args, **kwargs)
        finally:
            elapsed = perf_counter_ns() - start

            record("sql_total", elapsed)
            record(f"sql_{operation.lower()}", elapsed)

            if sql == dimension.updatetodatesql:
                record("sql_update_validto", elapsed)

    connection.execute = measured_execute

    # Instrument lookup, getbykey and insert.
    for method_name in ("lookup", "getbykey", "insert"):
        original = getattr(dimension, method_name)

        def make_wrapper(name, method):
            @wraps(method)
            def wrapper(*args, **kwargs):
                start = perf_counter_ns()

                try:
                    result = method(*args, **kwargs)

                    if name == "lookup" and result is None:
                        stats["lookup_not_found"]["calls"] += 1

                    return result
                finally:
                    record(
                        name,
                        perf_counter_ns() - start,
                    )

            return wrapper

        setattr(
            dimension,
            method_name,
            make_wrapper(method_name, original),
        )

    # Instrument scdensure and mark its execution scope.
    original_scdensure = dimension.scdensure

    @wraps(original_scdensure)
    def measured_scdensure(*args, **kwargs):
        nonlocal active_depth

        start = perf_counter_ns()
        active_depth += 1

        try:
            return original_scdensure(*args, **kwargs)
        finally:
            active_depth -= 1
            record(
                "scdensure",
                perf_counter_ns() - start,
            )

    dimension.scdensure = measured_scdensure

    def report():
        total = stats["scdensure"]["nanoseconds"]

        print("\nSCDENSURE DIAGNOSTICS")
        print("-" * 75)

        names = (
            "scdensure",
            "lookup",
            "getbykey",
            "insert",
            "sql_total",
            "sql_select",
            "sql_insert",
            "sql_update",
            "sql_update_validto",
            "sql_delete",
        )

        for name in names:
            values = stats[name]
            calls = values["calls"]
            seconds = values["nanoseconds"] / 1e9
            avg_us = (
                values["nanoseconds"] / calls / 1000
                if calls else 0
            )
            share = (
                100 * values["nanoseconds"] / total
                if total else 0
            )

            print(
                f"{name:20} "
                f"calls={calls:>10,} "
                f"time={seconds:>9.3f}s "
                f"avg={avg_us:>9.2f}us "
                f"share={share:>6.1f}%"
            )

        print(
            "\nMembers not found:",
            f"{stats['lookup_not_found']['calls']:,}"
        )

        print(
            "SQL statements per SCD call:",
            f"{stats['sql_total']['calls'] / stats['scdensure']['calls']:.4f}"
            if stats["scdensure"]["calls"] else "N/A",
        )

        print(
            "\nNote: Timings are nested; "
            "method and SQL percentages must not be summed."
        )

    return report
