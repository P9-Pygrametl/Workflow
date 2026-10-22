import os
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
import psycopg
import psutil
import pygrametl
from benchmarks.timing import TimedIterable, TimedProxy, wrap_with_timing
from Shared.cpygrametl2_db import (
    create_etl,
    run_etl,
)
from datagenerator.datagenerator_db import generate

PAGES = 100  # Define the workload scale (number of pages to generate)

load_dotenv(ROOT / ".env")

DW_DATABASE = os.getenv("DW_DATABASE")
DW_HOST = os.getenv("DW_HOST", "localhost")
DW_PORT_RAW = os.getenv("DW_PORT", "5432")

try:
    DW_PORT = int(DW_PORT_RAW)
except ValueError as error:
    raise ValueError(
        f"DW_PORT must be an integer, got {DW_PORT_RAW!r}"
    ) from error

PHASES = [
    "data_generation",
    "schema_reset",
    "initialisation",
    "extraction_merge",
    "transformation",
    "page_dimension",
    "date_dimension",
    "test_dimension",
    "fact_insert",
    "commit",
    "connection_close",
]


def reset_warehouse():
    schema = (ROOT / "starschema.sql").read_text()

    connection = psycopg.connect(
        host=DW_HOST,
        port=DW_PORT,
        dbname=DW_DATABASE,
        user=os.getenv("USERNAME"),
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute(schema)
        connection.commit()
    finally:
        connection.close()


def monitor_resources(stop_event, stats, interval=0.05):
    """Monitors RSS, PSS, and CPU usage of the active process tree in the background."""
    root_pid = os.getpid()
    samples = []
    
    # Keep a persistent cache of Process objects so their CPU counters persist
    process_cache = {}

    while not stop_event.is_set():
        timestamp = time.perf_counter()
        
        try:
            root_proc = psutil.Process(root_pid)
            # Find current active processes in the tree
            active_pids = [root_pid] + [p.pid for p in root_proc.children(recursive=True)]
        except psutil.Error:
            break

        # Clean up cached processes that are no longer active
        for pid in list(process_cache.keys()):
            if pid not in active_pids:
                del process_cache[pid]

        # Populate/retrieve from cache
        for pid in active_pids:
            if pid not in process_cache:
                try:
                    p = psutil.Process(pid)
                    # First call establishes the baseline comparison point
                    p.cpu_percent(interval=None)
                    process_cache[pid] = p
                except psutil.Error:
                    pass

        total_cpu = 0.0

        # Aggregate metrics across the cached process tree
        for pid, proc in list(process_cache.items()):
            try:
                if proc.status() == psutil.STATUS_ZOMBIE:
                    continue
                
                # This second call now has a historical point of reference in the cache!
                total_cpu += proc.cpu_percent(interval=None)
            except psutil.Error:
                # Process died since active_pids fetch
                if pid in process_cache:
                    del process_cache[pid]


            mem_info = proc.memory_full_info()
            rss_mb = mem_info.rss / (1024 * 1024)
            pss_mb = (getattr(mem_info, "pss", mem_info.rss)) / (1024 * 1024)

        samples.append({
            "time": timestamp,
            "rss_mb": rss_mb,
            "pss_mb": pss_mb,
            "cpu_percent": total_cpu,
        })

        time.sleep(interval)

    stats["samples"] = samples


def run_etl_worker(results_dict, exception_holder):
    """Worker target that runs the ETL workload and times its phases.

    Uses light local time calls on hot-paths to avoid blocking the DB loop.
    """
    try:
        # Structure to hold phase timestamps
        phase_events = {phase: {"start": 0.0, "end": 0.0} for phase in PHASES}
        timings = {phase: 0.0 for phase in PHASES}
        timings_cpu = {phase: 0.0 for phase in PHASES}

        # --- DATA GENERATION PHASE ---
        phase_events["data_generation"]["start"] = time.perf_counter()
        gen_wall_start = time.perf_counter()
        gen_cpu_start = time.process_time()

        print(f"Generating source data for pages={PAGES}...")
        generate(PAGES)

        timings["data_generation"] = time.perf_counter() - gen_wall_start
        timings_cpu["data_generation"] = time.process_time() - gen_cpu_start
        phase_events["data_generation"]["end"] = time.perf_counter()

        # --- SCHEMA RESET PHASE ---
        phase_events["schema_reset"]["start"] = time.perf_counter()
        reset_wall_start = time.perf_counter()
        reset_cpu_start = time.process_time()

        print("Resetting warehouse...")
        reset_warehouse()

        timings["schema_reset"] = time.perf_counter() - reset_wall_start
        timings_cpu["schema_reset"] = time.process_time() - reset_cpu_start
        phase_events["schema_reset"]["end"] = time.perf_counter()

        # --- 1. INITIALISATION PHASE ---
        phase_events["initialisation"]["start"] = time.perf_counter()
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

        timings["initialisation"] = time.perf_counter() - wall_start
        timings_cpu["initialisation"] = time.process_time() - cpu_start
        phase_events["initialisation"]["end"] = time.perf_counter()

        # Wrap components using standard lightning-fast timing objects
        timed_input = TimedIterable(inputdata, timings, timings_cpu)

        timed_getint = wrap_with_timing(
            pygrametl.getint, "transformation", timings, timings_cpu
        )

        def record_start(phase):
            if not phase_events[phase]["start"]:
                phase_events[phase]["start"] = time.perf_counter()

        def record_end(phase):
            phase_events[phase]["end"] = time.perf_counter()

        # Intercepting methods to capture physical phase start/end times
        class InterceptProxy(TimedProxy):
            def __init__(self, target, mapping, timings_dict, cpu_timings_dict):
                super().__init__(
                    target, mapping, timings_dict, cpu_timings_dict
                )
                self._mapping = mapping

            def __getattr__(self, name):
                attr = super().__getattr__(name)
                phase = self._mapping.get(name)
                if phase:

                    def wrapper(*args, **kwargs):
                        record_start(phase)
                        try:
                            return attr(*args, **kwargs)
                        finally:
                            record_end(phase)

                    return wrapper
                return attr

        timed_pagedim = InterceptProxy(
            pagedim, {"scdensure": "page_dimension"}, timings, timings_cpu
        )
        timed_datedim = InterceptProxy(
            datedim, {"ensure": "date_dimension"}, timings, timings_cpu
        )
        timed_testdim = InterceptProxy(
            testdim, {"lookup": "test_dimension"}, timings, timings_cpu
        )
        timed_facttbl = InterceptProxy(
            facttbl, {"insert": "fact_insert"}, timings, timings_cpu
        )

        timed_connection = InterceptProxy(
            connection,
            {"commit": "commit", "close": "connection_close"},
            timings,
            timings_cpu,
        )
        timed_sourceconn1 = InterceptProxy(
            sourceconn1, {"close": "connection_close"}, timings, timings_cpu
        )
        timed_sourceconn2 = InterceptProxy(
            sourceconn2, {"close": "connection_close"}, timings, timings_cpu
        )

        print("Profiling ETL phases inside worker thread...")

        phase_events["extraction_merge"]["start"] = time.perf_counter()
        phase_events["transformation"]["start"] = time.perf_counter()

        run_etl(
            timed_connection,
            timed_sourceconn1,
            timed_sourceconn2,
            timed_input,
            timed_pagedim,
            timed_datedim,
            timed_testdim,
            timed_facttbl,
            get_int=timed_getint,
        )

        phase_events["extraction_merge"]["end"] = time.perf_counter()
        phase_events["transformation"]["end"] = time.perf_counter()

        for phase in ["commit", "connection_close"]:
            if not phase_events[phase]["start"]:
                phase_events[phase]["start"] = phase_events[
                    "extraction_merge"
                ]["start"]
            if not phase_events[phase]["end"]:
                phase_events[phase]["end"] = time.perf_counter()

        # Save successful outputs to sharing container
        results_dict.update({
            "phase_events": phase_events,
            "timings": timings,
            "timings_cpu": timings_cpu,
        })
    except Exception as error:
        exception_holder["error"] = error


def run_and_profile():
    """Spawns the ETL thread, monitors resources in the background, and prints report."""
    results = {}
    exception_holder = {}

    # Set up resource monitoring in a separate daemon thread
    stop_event = threading.Event()
    resource_stats = {}
    monitor_thread = threading.Thread(
        target=monitor_resources,
        args=(stop_event, resource_stats),
        daemon=True,
    )

    print("Starting background resource monitor thread...")
    monitor_thread.start()

    # Create and start the ETL worker thread
    etl_thread = threading.Thread(
        target=run_etl_worker,
        args=(results, exception_holder),
    )

    print("Starting target ETL worker thread...")
    etl_thread.start()

    # Wait for the ETL thread to finish
    etl_thread.join()

    # Stop monitoring now that the run has finished
    stop_event.set()
    monitor_thread.join()

    # Handle exceptions if any occurred inside the worker thread
    if "error" in exception_holder:
        raise exception_holder["error"]

    if not results:
        raise RuntimeError(
            "ETL thread terminated without returning performance results."
        )

    phase_events = results["phase_events"]
    timings = results["timings"]
    timings_cpu = results["timings_cpu"]

    samples = resource_stats.get("samples", [])

    # Process overall statistics
    rss_vals = [s["rss_mb"] for s in samples] if samples else []
    pss_vals = [s["pss_mb"] for s in samples] if samples else []
    cpu_vals = [s["cpu_percent"] for s in samples] if samples else []

    max_rss = max(rss_vals) if rss_vals else 0.0
    avg_rss = sum(rss_vals) / len(rss_vals) if rss_vals else 0.0

    max_pss = max(pss_vals) if pss_vals else 0.0
    avg_pss = sum(pss_vals) / len(pss_vals) if pss_vals else 0.0

    avg_cpu = sum(cpu_vals) / len(cpu_vals) if cpu_vals else 0.0

    # Calculate average and max memory per phase
    phase_memory = {}
    for phase in PHASES:
        p_start = phase_events[phase]["start"]
        p_end = phase_events[phase]["end"]

        p_samples = [s for s in samples if p_start <= s["time"] <= p_end]

        if p_samples:
            rss_s = [s["rss_mb"] for s in p_samples]
            pss_s = [s["pss_mb"] for s in p_samples]
            phase_memory[phase] = {
                "rss_avg": sum(rss_s) / len(rss_s),
                "rss_max": max(rss_s),
                "pss_avg": sum(pss_s) / len(pss_s),
                "pss_max": max(pss_s),
            }
        elif samples:
            closest_sample = min(
                samples, key=lambda s: abs(s["time"] - p_start)
            )
            phase_memory[phase] = {
                "rss_avg": closest_sample["rss_mb"],
                "rss_max": closest_sample["rss_mb"],
                "pss_avg": closest_sample["pss_mb"],
                "pss_max": closest_sample["pss_mb"],
            }
        else:
            phase_memory[phase] = {
                "rss_avg": 0.0,
                "rss_max": 0.0,
                "pss_avg": 0.0,
                "pss_max": 0.0,
            }

    # Print Consolidated Results Report
    print("\n" + "=" * 105)
    print(
        f"{'PHASE PROFILING & RESOURCE METRICS (WORKLOAD: ' + str(PAGES) + ' PAGES)':^105}"
    )
    print("=" * 105)
    total_wall = sum(timings.values())

    print(
        f"  {'Phase':20} | {'Wall':8} | {'CPU Time':8} | "
        f"{'Avg RSS':11} | {'Max RSS':11} | {'Avg PSS':11} | {'Max PSS':11}"
    )
    print("-" * 105)

    for phase in PHASES:
        pct = (timings[phase] / total_wall * 100) if total_wall > 0 else 0.0
        wall_str = f"{timings[phase]:.2f}s ({pct:3.0f}%)"
        cpu_str = f"{timings_cpu[phase]:.2f}s"

        rss_avg = (
            f"{phase_memory[phase]['rss_avg']:7.1f} MB"
            if phase_memory[phase]["rss_avg"] > 0
            else "N/A"
        )
        rss_max = (
            f"{phase_memory[phase]['rss_max']:7.1f} MB"
            if phase_memory[phase]["rss_max"] > 0
            else "N/A"
        )
        pss_avg = (
            f"{phase_memory[phase]['pss_avg']:7.1f} MB"
            if phase_memory[phase]["pss_avg"] > 0
            else "N/A"
        )
        pss_max = (
            f"{phase_memory[phase]['pss_max']:7.1f} MB"
            if phase_memory[phase]["pss_max"] > 0
            else "N/A"
        )

        print(
            f"  {phase:20} | {wall_str:10} | {cpu_str:8} | "
            f"{rss_avg:11} | {rss_max:11} | {pss_avg:11} | {pss_max:11}"
        )

    print("\n" + "=" * 105)
    print(
        f"'Total ETL Execution Time (excluding Data Generation/Reset): "
        f"{total_wall - timings['data_generation'] - timings['schema_reset']:.2f}s'"
    )
    print("\n" + "=" * 105)
    print(f"{'AGGREGATE HARDWARE PERFORMANCE':^105}")
    print("=" * 105)
    print(f"  Average Process CPU Utilisation:      {avg_cpu:6.2f}%")
    print(
        f"  Average Resident Memory (RSS):        {avg_rss:6.2f} MB  |  "
        f"Peak Resident Memory (RSS): {max_rss:6.2f} MB"
    )
    print(
        f"  Average Proportional Memory (PSS):    {avg_pss:6.2f} MB  |  "
        f"Peak Proportional Memory (PSS): {max_pss:6.2f} MB"
    )
    print("=" * 105)


if __name__ == "__main__":
    run_and_profile()