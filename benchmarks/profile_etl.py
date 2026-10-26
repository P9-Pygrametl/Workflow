"""Phase profiler for synchronous and asynchronous pygrametl ETL workflows."""

import asyncio
import time

import pygrametl

from shared.cpygrametl1_db import (
    create_etl as create_sync_etl,
    extractdomaininfo,
    extractserverinfo,
    run_etl as run_sync_etl,
)
from shared.cpygrametl1_async_db import (
    create_etl as create_async_etl,
    run_etl as run_async_etl,
)
from benchmarks.timing import TimedIterable, TimedProxy, wrap_with_timing


PHASES = (
    "initialisation",
    "extraction_merge",
    "transformation",
    "page_dimension",
    "date_dimension",
    "test_dimension",
    "fact_insert",
    "commit",
    "connection_close",
)


class TimedAsyncIterable:
    """Time asynchronous requests for joined rows and count yielded rows.

    Timing includes time spent awaiting the next row, but excludes work the
    caller performs after receiving that row. This mirrors TimedIterable's
    intended phase boundary for the synchronous pipeline.
    """

    def __init__(self, source, timings, timings_cpu):
        self.source = source
        self.timings = timings
        self.timings_cpu = timings_cpu
        self.rows = 0

    async def __aiter__(self):
        iterator = aiter(self.source)
        try:
            while True:
                wall_start = time.perf_counter()
                cpu_start = time.process_time()
                try:
                    row = await anext(iterator)
                except StopAsyncIteration:
                    break
                finally:
                    self.timings["extraction_merge"] += (
                        time.perf_counter() - wall_start
                    )
                    self.timings_cpu["extraction_merge"] += (
                        time.process_time() - cpu_start
                    )
                self.rows += 1
                yield row
        finally:
            close = getattr(iterator, "aclose", None)
            if close is not None:
                wall_start = time.perf_counter()
                cpu_start = time.process_time()
                try:
                    await close()
                finally:
                    self.timings["extraction_merge"] += (
                        time.perf_counter() - wall_start
                    )
                    self.timings_cpu["extraction_merge"] += (
                        time.process_time() - cpu_start
                    )


class TimedAsyncCloseProxy:
    """Forward attributes, measuring completion of an awaited close()."""

    def __init__(self, target, timings, timings_cpu):
        self._target = target
        self._timings = timings
        self._timings_cpu = timings_cpu

    def __getattr__(self, name):
        return getattr(self._target, name)

    async def close(self):
        wall_start = time.perf_counter()
        cpu_start = time.process_time()
        try:
            await self._target.close()
        finally:
            self._timings["connection_close"] += (
                time.perf_counter() - wall_start
            )
            self._timings_cpu["connection_close"] += (
                time.process_time() - cpu_start
            )


def _new_timings():
    timings = {name: 0.0 for name in PHASES}
    timings_cpu = {name: 0.0 for name in PHASES}
    return timings, timings_cpu


def _wrap_etl(etl, timings, timings_cpu, *, async_mode):
    (
        connection,
        sourceconn1,
        sourceconn2,
        inputdata,
        pagedim,
        datedim,
        testdim,
        facttbl,
    ) = etl

    if async_mode:
        timed_input = TimedAsyncIterable(inputdata, timings, timings_cpu)
        timed_sourceconn1 = TimedAsyncCloseProxy(
            sourceconn1, timings, timings_cpu
        )
        timed_sourceconn2 = TimedAsyncCloseProxy(
            sourceconn2, timings, timings_cpu
        )
    else:
        timed_input = TimedIterable(inputdata, timings, timings_cpu)
        timed_sourceconn1 = TimedProxy(
            sourceconn1, {"close": "connection_close"}, timings, timings_cpu
        )
        timed_sourceconn2 = TimedProxy(
            sourceconn2, {"close": "connection_close"}, timings, timings_cpu
        )

    timed_extractdomaininfo = wrap_with_timing(
        extractdomaininfo, "transformation", timings, timings_cpu
    )
    timed_extractserverinfo = wrap_with_timing(
        extractserverinfo, "transformation", timings, timings_cpu
    )
    timed_getint = wrap_with_timing(
        pygrametl.getint, "transformation", timings, timings_cpu
    )
    timed_pagedim = TimedProxy(
        pagedim, {"scdensure": "page_dimension"}, timings, timings_cpu
    )
    timed_datedim = TimedProxy(
        datedim, {"ensure": "date_dimension"}, timings, timings_cpu
    )
    timed_testdim = TimedProxy(
        testdim, {"lookup": "test_dimension"}, timings, timings_cpu
    )
    timed_facttbl = TimedProxy(
        facttbl, {"insert": "fact_insert"}, timings, timings_cpu
    )
    timed_connection = TimedProxy(
        connection,
        {"commit": "commit", "close": "connection_close"},
        timings,
        timings_cpu,
    )

    arguments = (
        timed_connection,
        timed_sourceconn1,
        timed_sourceconn2,
        timed_input,
        timed_pagedim,
        timed_datedim,
        timed_testdim,
        timed_facttbl,
    )
    keyword_arguments = {
        "extract_domain": timed_extractdomaininfo,
        "extract_server": timed_extractserverinfo,
        "get_int": timed_getint,
    }
    return timed_input, arguments, keyword_arguments


def _result(timings, timings_cpu, rows, total_wall, total_cpu):
    timings["other"] = max(0.0, total_wall - sum(timings.values()))
    timings_cpu["other"] = max(0.0, total_cpu - sum(timings_cpu.values()))
    timings_waiting = {
        name: max(0.0, timings[name] - timings_cpu[name])
        for name in timings
    }
    result = {
        "rows": rows,
        "total_profiled_wall_seconds": total_wall,
        "total_profiled_cpu_seconds": total_cpu,
        "timings": timings,
        "timings_cpu": timings_cpu,
        "timings_waiting": timings_waiting,
    }
    _print_profile(result)
    return result


def _print_profile(result):
    timings = result["timings"]
    timings_cpu = result["timings_cpu"]
    timings_waiting = result["timings_waiting"]
    total_wall = result["total_profiled_wall_seconds"]
    total_cpu = result["total_profiled_cpu_seconds"]

    print("\nPhase profile")
    print("-" * 78)
    print(
        f"{'phase':22}{'wall':>10} {'wall%':>7} "
        f"{'cpu':>10} {'waiting':>10} {'cpu%':>7}"
    )
    print("-" * 78)
    for name, seconds in timings.items():
        percentage = seconds / total_wall * 100 if total_wall > 0 else 0.0
        cpu_seconds = timings_cpu[name]
        cpu_percentage = cpu_seconds / total_wall * 100 if total_wall > 0 else 0.0
        print(
            f"{name:22}{seconds:9.2f}s {percentage:6.2f}% "
            f"{cpu_seconds:9.2f}s {timings_waiting[name]:9.2f}s "
            f"{cpu_percentage:6.2f}%"
        )
    print("-" * 78)
    print(
        f"{'total':22}{total_wall:9.2f}s {100.0:6.2f}% "
        f"{total_cpu:9.2f}s {max(0.0, total_wall-total_cpu):9.2f}s "
        f"{(total_cpu/total_wall*100) if total_wall>0 else 0.0:6.2f}%"
    )


def _profile_sync():
    timings, timings_cpu = _new_timings()
    print("Creating ETL...")
    wall_start = time.perf_counter()
    cpu_start = time.process_time()
    etl = create_sync_etl()
    timings["initialisation"] = time.perf_counter() - wall_start
    timings_cpu["initialisation"] = time.process_time() - cpu_start

    timed_input, args, kwargs = _wrap_etl(
        etl, timings, timings_cpu, async_mode=False
    )
    print("Profiling ETL...")
    wall_start = time.perf_counter()
    cpu_start = time.process_time()
    run_sync_etl(*args, **kwargs)
    total_wall = timings["initialisation"] + time.perf_counter() - wall_start
    total_cpu = timings_cpu["initialisation"] + time.process_time() - cpu_start
    return _result(timings, timings_cpu, timed_input.rows, total_wall, total_cpu)


async def _profile_async():
    timings, timings_cpu = _new_timings()
    print("Creating asynchronous ETL...")
    wall_start = time.perf_counter()
    cpu_start = time.process_time()
    etl = await create_async_etl()
    timings["initialisation"] = time.perf_counter() - wall_start
    timings_cpu["initialisation"] = time.process_time() - cpu_start

    timed_input, args, kwargs = _wrap_etl(
        etl, timings, timings_cpu, async_mode=True
    )
    print("Profiling asynchronous ETL...")
    wall_start = time.perf_counter()
    cpu_start = time.process_time()
    await run_async_etl(*args, **kwargs)
    total_wall = timings["initialisation"] + time.perf_counter() - wall_start
    total_cpu = timings_cpu["initialisation"] + time.process_time() - cpu_start
    return _result(timings, timings_cpu, timed_input.rows, total_wall, total_cpu)


def profile(mode="sync"):
    """Profile the requested sequential ETL implementation by phase.

    ``mode='sync'`` preserves the original profiler's behaviour.
    ``mode='async'`` profiles asynchronous extraction and merge joining,
    with the unchanged synchronous DW operations.

    Each phase's waiting time is estimated as max(0, wall - process CPU).
    This is a proxy for non-CPU time, not a measurement of database latency.
    """
    if mode == "sync":
        return _profile_sync()
    if mode == "async":
        return asyncio.run(_profile_async())
    raise ValueError("mode must be 'sync' or 'async'")
