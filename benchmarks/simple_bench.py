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
from shared.cpygrametl2_db import (
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

# Top-level non-overlapping phases whose wall times sum to total wall time
MAJOR_PHASES = [
    "data_generation",
    "schema_reset",
    "initialisation",
    "etl_loop",
    "commit",
    "connection_close",
]

# Parent-thread sub-timings inside etl_loop (for bottleneck breakdown only;
# decoupled tables reflect enqueue time rather than worker execution time)
SUB_PHASES = [
    "extraction_merge",
    "transformation",
    "page_dimension",
    "date_dimension",
    "test_dimension",
    "fact_insert",
]


def get_tree_cpu_seconds(root_pid):
    """Returns cumulative (user + system) CPU seconds for the root process and all active children."""
    total = 0.0
    try:
        root = psutil.Process(root_pid)
        procs = [root] + root.children(recursive=True)
    except psutil.Error:
        return 0.0

    for proc in procs:
        try:
            times = proc.cpu_times()
            total += times.user + times.system
        except psutil.Error:
            pass
    return total


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
    """Monitors total RSS, PSS, and CPU usage across the entire active process tree."""
    root_pid = os.getpid()
    samples = []
    process_cache = {}
    reaped_cpu_time = 0.0
    last_cpu_time = {}

    while not stop_event.is_set():
        timestamp = time.perf_counter()

        try:
            root_proc = psutil.Process(root_pid)
            active_procs = [root_proc] + root_proc.children(recursive=True)
            active_pids = {p.pid for p in active_procs}
        except psutil.Error:
            break

        # Reap final CPU seconds from children that exited since the last sample
        for pid in list(process_cache.keys()):
            if pid not in active_pids:
                reaped_cpu_time += last_cpu_time.pop(pid, 0.0)
                del process_cache[pid]

        for proc in active_procs:
            if proc.pid not in process_cache:
                try:
                    proc.cpu_percent(interval=None)  # Initialize baseline
                    process_cache[proc.pid] = proc
                except psutil.Error:
                    pass

        total_cpu = 0.0
        total_rss_mb = 0.0
        total_pss_mb = 0.0

        for pid, proc in list(process_cache.items()):
            try:
                if proc.status() == psutil.STATUS_ZOMBIE:
                    continue

                total_cpu += proc.cpu_percent(interval=None)
                c_times = proc.cpu_times()
                last_cpu_time[pid] = c_times.user + c_times.system

                mem_info = proc.memory_full_info()
                total_rss_mb += mem_info.rss / (1024 * 1024)
                total_pss_mb += getattr(mem_info, "pss", mem_info.rss) / (
                    1024 * 1024
                )
            except psutil.Error:
                reaped_cpu_time += last_cpu_time.pop(pid, 0.0)
                process_cache.pop(pid, None)

        cumulative_tree_cpu = reaped_cpu_time + sum(last_cpu_time.values())

        samples.append({
            "time": timestamp,
            "rss_mb": total_rss_mb,
            "pss_mb": total_pss_mb,
            "cpu_percent": total_cpu,
            "tree_cpu_time": cumulative_tree_cpu,
        })

        time.sleep(interval)

    stats["samples"] = samples


def run_etl_worker(results_dict, exception_holder):
    """Worker target that runs the ETL workload and times its phases."""
    try:
        root_pid = os.getpid()
        phase_events = {
            phase: {"start": 0.0, "end": 0.0} for phase in MAJOR_PHASES
        }
        timings = {phase: 0.0 for phase in MAJOR_PHASES + SUB_PHASES}
        timings_cpu = {phase: 0.0 for phase in MAJOR_PHASES + SUB_PHASES}

        def start_major(phase):
            phase_events[phase]["start"] = time.perf_counter()
            return time.perf_counter(), get_tree_cpu_seconds(root_pid)

        def end_major(phase, w_start, c_start):
            phase_events[phase]["end"] = time.perf_counter()
            timings[phase] = phase_events[phase]["end"] - w_start
            timings_cpu[phase] = max(
                0.0, get_tree_cpu_seconds(root_pid) - c_start
            )

        # --- 1. DATA GENERATION PHASE ---
        w0, c0 = start_major("data_generation")
        print(f"Generating source data for pages={PAGES}...")
        generate(PAGES)
        end_major("data_generation", w0, c0)

        # --- 2. SCHEMA RESET PHASE ---
        w0, c0 = start_major("schema_reset")
        print("Resetting warehouse...")
        reset_warehouse()
        end_major("schema_reset", w0, c0)

        # --- 3. INITIALISATION PHASE ---
        w0, c0 = start_major("initialisation")
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
        end_major("initialisation", w0, c0)

        # Wrap sub-operations for parent-side breakdown inside etl_loop
        timed_input = TimedIterable(inputdata, timings, timings_cpu)
        timed_getint = wrap_with_timing(
            pygrametl.getint, "transformation", timings, timings_cpu
        )

        # Intercept commit and close as distinct major phases
        class LifecycleProxy:
            def __init__(self, target):
                self._target = target

            def commit(self):
                cw0, cc0 = start_major("commit")
                try:
                    return self._target.commit()
                finally:
                    end_major("commit", cw0, cc0)

            def close(self):
                # Accumulate across sourceconn1, sourceconn2, and shrdconn
                if not phase_events["connection_close"]["start"]:
                    phase_events["connection_close"]["start"] = (
                        time.perf_counter()
                    )
                cw0 = time.perf_counter()
                cc0 = get_tree_cpu_seconds(root_pid)
                try:
                    return self._target.close()
                finally:
                    phase_events["connection_close"]["end"] = (
                        time.perf_counter()
                    )
                    timings["connection_close"] += (
                        phase_events["connection_close"]["end"] - cw0
                    )
                    timings_cpu["connection_close"] += max(
                        0.0, get_tree_cpu_seconds(root_pid) - cc0
                    )

            def __getattr__(self, name):
                return getattr(self._target, name)

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

        timed_connection = LifecycleProxy(connection)
        timed_sourceconn1 = LifecycleProxy(sourceconn1)
        timed_sourceconn2 = LifecycleProxy(sourceconn2)

        print("Profiling ETL phases inside worker thread...")

        # --- 4. ETL LOOP PHASE (from start of run_etl until commit begins) ---
        etl_w0, etl_c0 = start_major("etl_loop")

        # Wrap run_etl so we close etl_loop right when commit starts
        orig_commit = timed_connection.commit

        def commit_with_etl_end():
            if not phase_events["etl_loop"]["end"]:
                end_major("etl_loop", etl_w0, etl_c0)
            return orig_commit()

        timed_connection.commit = commit_with_etl_end

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

        # Fallback if commit was never called
        if not phase_events["etl_loop"]["end"]:
            end_major("etl_loop", etl_w0, etl_c0)

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

    stop_event = threading.Event()
    resource_stats = {}
    monitor_thread = threading.Thread(
        target=monitor_resources,
        args=(stop_event, resource_stats),
        daemon=True,
    )

    print("Starting background resource monitor thread...")
    monitor_thread.start()

    etl_thread = threading.Thread(
        target=run_etl_worker,
        args=(results, exception_holder),
    )

    print("Starting target ETL worker thread...")
    etl_thread.start()
    etl_thread.join()

    stop_event.set()
    monitor_thread.join()

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

    rss_vals = [s["rss_mb"] for s in samples]
    pss_vals = [s["pss_mb"] for s in samples]
    cpu_vals = [s["cpu_percent"] for s in samples]

    max_rss = max(rss_vals) if rss_vals else 0.0
    avg_rss = sum(rss_vals) / len(rss_vals) if rss_vals else 0.0
    max_pss = max(pss_vals) if pss_vals else 0.0
    avg_pss = sum(pss_vals) / len(pss_vals) if pss_vals else 0.0
    avg_cpu = sum(cpu_vals) / len(cpu_vals) if cpu_vals else 0.0

    # Calculate memory and tree CPU per non-overlapping major phase
    phase_memory = {}
    for phase in MAJOR_PHASES:
        p_start = phase_events[phase]["start"]
        p_end = phase_events[phase]["end"]
        p_samples = [s for s in samples if p_start <= s["time"] <= p_end]

        if p_samples:
            rss_s = [s["rss_mb"] for s in p_samples]
            pss_s = [s["pss_mb"] for s in p_samples]
            # Use monitor samples if child processes exited before end_major snapshot
            sampled_cpu_delta = (
                p_samples[-1]["tree_cpu_time"] - p_samples[0]["tree_cpu_time"]
            )
            if sampled_cpu_delta > timings_cpu[phase]:
                timings_cpu[phase] = sampled_cpu_delta

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
        f"{'MAJOR PHASE PROFILING & TREE RESOURCE METRICS (WORKLOAD: ' + str(PAGES) + ' PAGES)':^105}"
    )
    print("=" * 105)
    total_wall = sum(timings[p] for p in MAJOR_PHASES)

    print(
        f"  {'Major Phase':20} | {'Wall':11} | {'Tree CPU':8} | "
        f"{'Avg RSS':11} | {'Max RSS':11} | {'Avg PSS':11} | {'Max PSS':11}"
    )
    print("-" * 105)

    for phase in MAJOR_PHASES:
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
            f"  {phase:20} | {wall_str:11} | {cpu_str:8} | "
            f"{rss_avg:11} | {rss_max:11} | {pss_avg:11} | {pss_max:11}"
        )

    print("\n" + "-" * 105)
    print(f"{'PARENT-THREAD SUB-TIMINGS INSIDE ETL LOOP (ENQUEUE / LOOKUP OVERHEAD)':^105}")
    print("-" * 105)
    for sub in SUB_PHASES:
        print(
            f"  {sub:20} | Wall: {timings[sub]:6.2f}s | Parent CPU: {timings_cpu[sub]:6.2f}s"
        )

    etl_only_wall = (
        total_wall - timings["data_generation"] - timings["schema_reset"]
    )
    print("\n" + "=" * 105)
    print(
        f"Total ETL Execution Time (excluding Data Generation/Reset): {etl_only_wall:.2f}s"
    )
    print("=" * 105)
    print(f"{'AGGREGATE PROCESS TREE HARDWARE PERFORMANCE':^105}")
    print("=" * 105)
    print(f"  Average Process Tree CPU Utilisation: {avg_cpu:6.2f}%")
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