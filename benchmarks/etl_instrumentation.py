# Note: Instrumentation is apparently a well-known term in programming meaning
# something along the lines of "adding code to a program whose purpose is to 
# observe what the program is doing while it runs, without changing what the
# program actually computes".

import importlib
import time

from benchmark_config import PHASES
from benchmark_results import print_profile_summary
from timing_wrappers import TimedIterable, TimedProxy, timed_function
from warehouse import reset_warehouse

MODULE_NAME = "cpygrametl1_db"


def create_timings():
    timings = {phase: 0.0 for phase in PHASES if phase != "other"}
    return timings, {name: 0.0 for name in timings}


def measure_etl(module, timings, timings_cpu):
    module.inputdata = TimedIterable(module.inputdata, timings, timings_cpu)
    for function_name in ("extractdomaininfo", "extractserverinfo"):
        setattr(
            module,
            function_name,
            timed_function(
                getattr(module, function_name),
                "transformation",
                timings,
                timings_cpu,
            ),
        )

    module.pygrametl = TimedProxy(
        module.pygrametl,
        {"getint": "transformation"},
        timings,
        timings_cpu,
    )
    for attribute, method, phase in (
        ("pagedim", "scdensure", "page_dimension"),
        ("datedim", "ensure", "date_dimension"),
        ("testdim", "lookup", "test_dimension"),
        ("facttbl", "insert", "fact_insert"),
    ):
        setattr(
            module,
            attribute,
            TimedProxy(
                module.__dict__[attribute],
                {method: phase},
                timings,
                timings_cpu,
            ),
        )

    module.connection = TimedProxy(
        module.connection,
        {"commit": "commit", "close": "connection_close"},
        timings,
        timings_cpu,
    )
    for attribute in ("sourceconn1", "sourceconn2"):
        setattr(
            module,
            attribute,
            TimedProxy(
                module.__dict__[attribute],
                {"close": "connection_close"},
                timings,
                timings_cpu,
            ),
        )


def profile(implementation):
    timings, timings_cpu = create_timings()
    print(f"Resetting warehouse for {implementation}...")
    reset_warehouse()

    print(f"Importing {MODULE_NAME}...")
    wall_start = time.perf_counter()
    cpu_start = time.process_time()
    module = importlib.import_module(MODULE_NAME)
    timings["initialisation"] = time.perf_counter() - wall_start
    timings_cpu["initialisation"] = time.process_time() - cpu_start

    measure_etl(module, timings, timings_cpu)
    print(f"Profiling {implementation} ETL...")
    wall_start = time.perf_counter()
    cpu_start = time.process_time()
    module.main()
    main_seconds = time.perf_counter() - wall_start
    main_cpu_seconds = time.process_time() - cpu_start

    total_profiled_wall = timings["initialisation"] + main_seconds
    total_profiled_cpu = timings_cpu["initialisation"] + main_cpu_seconds
    timings["other"] = max(0.0, total_profiled_wall - sum(timings.values()))
    timings_cpu["other"] = max(0.0, total_profiled_cpu - sum(timings_cpu.values()))
    timings_waiting = {
        name: max(0.0, timings[name] - timings_cpu[name])
        for name in timings
    }
    result = {
        "implementation": implementation,
        "rows": module.inputdata.rows,
        "total_profiled_wall_seconds": total_profiled_wall,
        "total_profiled_cpu_seconds": total_profiled_cpu,
        "timings": timings,
        "timings_cpu": timings_cpu,
        "timings_waiting": timings_waiting,
    }
    print_profile_summary(
        timings,
        timings_cpu,
        timings_waiting,
        total_profiled_wall,
        total_profiled_cpu,
    )
    return result