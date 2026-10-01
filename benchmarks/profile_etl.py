import time

from cpygrametl1_db import (
    create_etl,
    extractdomaininfo,
    extractserverinfo,
    run_etl,
)

import pygrametl


def timed_function(
    function,
    timing_name,
    timings,
    timings_cpu,
):
    def wrapper(*args, **kwargs):
        wall_start = time.perf_counter()
        cpu_start = time.process_time()

        try:
            return function(*args, **kwargs)
        finally:
            timings[timing_name] += (
                time.perf_counter() - wall_start
            )
            timings_cpu[timing_name] += (
                time.process_time() - cpu_start
            )

    return wrapper


class TimedIterable:
    def __init__(self, source, timings, timings_cpu):
        self.source = source
        self.timings = timings
        self.timings_cpu = timings_cpu
        self.rows = 0

    def __iter__(self):
        iterator = iter(self.source)

        while True:
            wall_start = time.perf_counter()
            cpu_start = time.process_time()

            try:
                row = next(iterator)
            except StopIteration:
                self.timings["extraction_merge"] += (
                    time.perf_counter() - wall_start
                )
                self.timings_cpu["extraction_merge"] += (
                    time.process_time() - cpu_start
                )
                return

            self.timings["extraction_merge"] += (
                time.perf_counter() - wall_start
            )
            self.timings_cpu["extraction_merge"] += (
                time.process_time() - cpu_start
            )

            self.rows += 1

            yield row


class TimedProxy:
    def __init__(self, obj, methods, timings, timings_cpu):
        self._obj = obj

        for method_name, timing_name in methods.items():
            method = getattr(obj, method_name)

            setattr(
                self,
                method_name,
                timed_function(
                    method,
                    timing_name,
                    timings,
                    timings_cpu,
                ),
            )

    def __getattr__(self, name):
        return getattr(self._obj, name)


def profile():
    timings = {
        "initialisation": 0.0,
        "extraction_merge": 0.0,
        "transformation": 0.0,
        "page_dimension": 0.0,
        "date_dimension": 0.0,
        "test_dimension": 0.0,
        "fact_insert": 0.0,
        "commit": 0.0,
        "connection_close": 0.0,
    }

    timings_cpu = {
        name: 0.0 for name in timings
    }

    print("Creating ETL...")

    wall_start = time.perf_counter()
    cpu_start = time.process_time()

    (
        connection,
        sourceconn1,
        sourceconn2,
        inputdata,
        pagedim,
        datedim,
        testdim,
        facttbl,
    ) = create_etl()

    timings["initialisation"] = (
        time.perf_counter() - wall_start
    )
    timings_cpu["initialisation"] = (
        time.process_time() - cpu_start
    )

    timed_input = TimedIterable(
        inputdata,
        timings,
        timings_cpu,
    )

    timed_extractdomaininfo = timed_function(
        extractdomaininfo,
        "transformation",
        timings,
        timings_cpu,
    )

    timed_extractserverinfo = timed_function(
        extractserverinfo,
        "transformation",
        timings,
        timings_cpu,
    )

    timed_getint = timed_function(
        pygrametl.getint,
        "transformation",
        timings,
        timings_cpu,
    )

    timed_pagedim = TimedProxy(
        pagedim,
        {
            "scdensure": "page_dimension",
        },
        timings,
        timings_cpu,
    )

    timed_datedim = TimedProxy(
        datedim,
        {
            "ensure": "date_dimension",
        },
        timings,
        timings_cpu,
    )

    timed_testdim = TimedProxy(
        testdim,
        {
            "lookup": "test_dimension",
        },
        timings,
        timings_cpu,
    )

    timed_facttbl = TimedProxy(
        facttbl,
        {
            "insert": "fact_insert",
        },
        timings,
        timings_cpu,
    )

    timed_connection = TimedProxy(
        connection,
        {
            "commit": "commit",
            "close": "connection_close",
        },
        timings,
        timings_cpu,
    )

    timed_sourceconn1 = TimedProxy(
        sourceconn1,
        {
            "close": "connection_close",
        },
        timings,
        timings_cpu,
    )

    timed_sourceconn2 = TimedProxy(
        sourceconn2,
        {
            "close": "connection_close",
        },
        timings,
        timings_cpu,
    )

    print("Profiling ETL...")

    wall_start = time.perf_counter()
    cpu_start = time.process_time()

    run_etl(
        timed_connection,
        timed_sourceconn1,
        timed_sourceconn2,
        timed_input,
        timed_pagedim,
        timed_datedim,
        timed_testdim,
        timed_facttbl,
        extract_domain=timed_extractdomaininfo,
        extract_server=timed_extractserverinfo,
        get_int=timed_getint,
    )

    main_seconds = time.perf_counter() - wall_start
    main_cpu_seconds = time.process_time() - cpu_start

    total_profiled_wall = (
        timings["initialisation"]
        + main_seconds
    )

    total_profiled_cpu = (
        timings_cpu["initialisation"]
        + main_cpu_seconds
    )

    measured = sum(timings.values())
    measured_cpu = sum(timings_cpu.values())

    timings["other"] = max(
        0.0,
        total_profiled_wall - measured,
    )

    timings_cpu["other"] = max(
        0.0,
        total_profiled_cpu - measured_cpu,
    )

    timings_waiting = {
        name: max(
            0.0,
            timings[name] - timings_cpu[name],
        )
        for name in timings
    }

    result = {
        "rows": timed_input.rows,
        "total_profiled_wall_seconds": total_profiled_wall,
        "total_profiled_cpu_seconds": total_profiled_cpu,
        "timings": timings,
        "timings_cpu": timings_cpu,
        "timings_waiting": timings_waiting,
    }

    print()
    print("Phase profile")
    print("-" * 78)
    print(
        f"{'phase':22}"
        f"{'wall':>10} "
        f"{'wall%':>7} "
        f"{'cpu':>10} "
        f"{'waiting':>10} "
        f"{'cpu%':>7}"
    )
    print("-" * 78)

    for name, seconds in timings.items():
        percentage = (
            seconds / total_profiled_wall * 100
            if total_profiled_wall > 0
            else 0.0
        )

        cpu_seconds = timings_cpu[name]
        waiting_seconds = timings_waiting[name]

        cpu_percentage = (
            cpu_seconds / total_profiled_wall * 100
            if total_profiled_wall > 0
            else 0.0
        )

        print(
            f"{name:22}"
            f"{seconds:9.2f}s "
            f"{percentage:6.2f}% "
            f"{cpu_seconds:9.2f}s "
            f"{waiting_seconds:9.2f}s "
            f"{cpu_percentage:6.2f}%"
        )

    print("-" * 78)
    print(
        f"{'total':22}"
        f"{total_profiled_wall:9.2f}s "
        f"{100.00:6.2f}% "
        f"{total_profiled_cpu:9.2f}s "
        f"{max(0.0, total_profiled_wall - total_profiled_cpu):9.2f}s "
        f"{(total_profiled_cpu / total_profiled_wall * 100) if total_profiled_wall > 0 else 0.0:6.2f}%"
    )

    return result