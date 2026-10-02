import time

from cpygrametl1_db import (
    create_etl,
    extractdomaininfo,
    extractserverinfo,
    run_etl,
)

from benchmarks.timing import (
    TimedIterable,
    TimedProxy,
    wrap_with_timing,
)

import pygrametl


def profile():
    """Profile the ETL execution by phase.

    The ETL components are wrapped so that selected operations accumulate
    wall-clock and Python CPU time for their corresponding phases. Time
    not attributed to an explicitly measured phase is recorded as
    ``other``.

    Waiting time for each phase is estimated as the difference between
    wall-clock and Python CPU time.

    Returns:
        A dictionary containing the processed row count, total profiled
        wall-clock and CPU times, and per-phase wall-clock, CPU, and
        estimated waiting times.
    """
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

    timed_extractdomaininfo = wrap_with_timing(
        extractdomaininfo,
        "transformation",
        timings,
        timings_cpu,
    )

    timed_extractserverinfo = wrap_with_timing(
        extractserverinfo,
        "transformation",
        timings,
        timings_cpu,
    )

    timed_getint = wrap_with_timing(
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

    etl_wall_seconds = time.perf_counter() - wall_start
    etl_cpu_seconds = time.process_time() - cpu_start

    total_profiled_wall = (
        timings["initialisation"]
        + etl_wall_seconds
    )

    total_profiled_cpu = (
        timings_cpu["initialisation"]
        + etl_cpu_seconds
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