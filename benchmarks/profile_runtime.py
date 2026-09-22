import importlib
import subprocess
import time

from benchmark_config import DW_DATABASE, PHASES, ROOT

MODULE_NAME = "cpygrametl1_db"

def reset_warehouse():
    subprocess.run(
        ["psql", DW_DATABASE, "-f", str(ROOT / "starschema.sql")],
        check=True,
        stdout=subprocess.DEVNULL,
    )


def timed_function(function, timing_name, timings, timings_cpu):
    def wrapper(*args, **kwargs):
        wall_start = time.perf_counter()
        cpu_start = time.process_time()
        try:
            return function(*args, **kwargs)
        finally:
            timings[timing_name] += time.perf_counter() - wall_start
            timings_cpu[timing_name] += time.process_time() - cpu_start

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
                self._record("extraction_merge", wall_start, cpu_start)
                return

            self._record("extraction_merge", wall_start, cpu_start)
            self.rows += 1
            yield row

    def _record(self, timing_name, wall_start, cpu_start):
        self.timings[timing_name] += time.perf_counter() - wall_start
        self.timings_cpu[timing_name] += time.process_time() - cpu_start


class TimedProxy:
    def __init__(self, obj, methods, timings, timings_cpu):
        self._obj = obj
        for method_name, timing_name in methods.items():
            setattr(
                self,
                method_name,
                timed_function(
                    getattr(obj, method_name),
                    timing_name,
                    timings,
                    timings_cpu,
                ),
            )

    def __getattr__(self, name):
        return getattr(self._obj, name)


def create_timings():
    timings = {phase: 0.0 for phase in PHASES if phase != "other"}
    return timings, {name: 0.0 for name in timings}


def instrument_module(module, timings, timings_cpu):
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

    instrument_module(module, timings, timings_cpu)
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
    print_profile_table(
        timings,
        timings_cpu,
        timings_waiting,
        total_profiled_wall,
        total_profiled_cpu,
    )
    return result


def print_profile_table(timings, timings_cpu, timings_waiting, total_wall, total_cpu):
    print()
    print("Phase profile")
    print("-" * 78)
    print(
        f"{'phase':22}{'wall':>10} {'wall%':>7} "
        f"{'cpu':>10} {'waiting':>10} {'cpu%':>7}"
    )
    print("-" * 78)
    for name, seconds in timings.items():
        percentage = seconds / total_wall * 100 if total_wall else 0.0
        cpu_seconds = timings_cpu[name]
        cpu_percentage = cpu_seconds / total_wall * 100 if total_wall else 0.0
        print(
            f"{name:22}{seconds:9.2f}s {percentage:6.2f}% "
            f"{cpu_seconds:9.2f}s {timings_waiting[name]:9.2f}s "
            f"{cpu_percentage:6.2f}%"
        )
    print("-" * 78)
    waiting = max(0.0, total_wall - total_cpu)
    cpu_percentage = total_cpu / total_wall * 100 if total_wall else 0.0
    print(
        f"{'total':22}{total_wall:9.2f}s {100.00:6.2f}% "
        f"{total_cpu:9.2f}s {waiting:9.2f}s {cpu_percentage:6.2f}%"
    )
