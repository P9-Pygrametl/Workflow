import sys
from pathlib import Path

# 1. SETUP SYS.PATH FIRST (Must be done before importing local modules)
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# 2. STANDARD LIBRARY IMPORTS
import os
import threading
import time

# 3. EXTERNAL AND PROJECT IMPORTS
from dotenv import load_dotenv
import psycopg
import psutil
import pygrametl
from benchmarks.timing import TimedIterable, TimedProxy, wrap_with_timing
from cpygrametl2_db import (
    create_etl,
    extractdomaininfo,
    extractserverinfo,
    run_etl,
)
from datagenerator.datagenerator_db import generate

# ==============================================================================
# CONFIGURATION
# ==============================================================================
PAGES = 100  # Define the workload scale (number of pages to generate)
# ==============================================================================

# Load environment variables from the repository root
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
    """Reset the PostgreSQL data warehouse using the star schema."""
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


def monitor_resources(stop_event, stats, pid, interval=0.05):
    """Monitors RSS, PSS, and multi-threaded process CPU usage accurately."""
    try:
        proc = psutil.Process(pid)
        proc.cpu_percent(interval=None)

        samples = []

        while not stop_event.is_set():
            if not proc.is_running() or proc.status() == psutil.STATUS_ZOMBIE:
                break

            timestamp = time.perf_counter()
            
            # Fetch RSS and PSS memory figures safely
            mem_info = proc.memory_full_info()
            rss_mb = mem_info.rss / (1024 * 1024)
            
            # PSS is only supported on Linux and macOS; fallback to RSS on Windows
            pss_mb = (getattr(mem_info, "pss", mem_info.rss)) / (1024 * 1024)
            
            process_cpu = proc.cpu_percent(interval=None)
            
            samples.append({
                "time": timestamp,
                "rss_mb": rss_mb,
                "pss_mb": pss_mb,
                "cpu_percent": process_cpu,
            })

            time.sleep(interval)

        stats["samples"] = samples
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass


def run_and_profile():
    """Runs the profiling setup while measuring system resource usage."""
    print(f"Generating source data for pages={PAGES}...")
    generate(PAGES)

    print("Resetting warehouse...")
    reset_warehouse()

    # Structure to hold phase timestamps
    phase_events = {phase: {"start": 0.0, "end": 0.0} for phase in PHASES}
    timings = {phase: 0.0 for phase in PHASES}
    timings_cpu = {phase: 0.0 for phase in PHASES}

    stop_event = threading.Event()
    resource_stats = {}
    monitor_thread = threading.Thread(
        target=monitor_resources,
        args=(stop_event, resource_stats, os.getpid()),
        daemon=True,
    )

    print("Starting background resource monitor...")
    monitor_thread.start()

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

    # Wrap components
    timed_input = TimedIterable(inputdata, timings, timings_cpu)

    timed_extractdomaininfo = wrap_with_timing(
        extractdomaininfo, "transformation", timings, timings_cpu
    )
    timed_extractserverinfo = wrap_with_timing(
        extractserverinfo, "transformation", timings, timings_cpu
    )
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
            super().__init__(target, mapping, timings_dict, cpu_timings_dict)
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

    print("Profiling ETL phases...")
    
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
        extract_domain=timed_extractdomaininfo,
        extract_server=timed_extractserverinfo,
        get_int=timed_getint,
    )

    phase_events["extraction_merge"]["end"] = time.perf_counter()
    phase_events["transformation"]["end"] = time.perf_counter()

    for phase in ["commit", "connection_close"]:
        if not phase_events[phase]["start"]:
            phase_events[phase]["start"] = phase_events["extraction_merge"]["start"]
        if not phase_events[phase]["end"]:
            phase_events[phase]["end"] = time.perf_counter()

    # Stop resource tracking
    stop_event.set()
    monitor_thread.join()

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
        
        p_samples = [
            s for s in samples 
            if p_start <= s["time"] <= p_end
        ]
        
        if p_samples:
            rss_s = [s["rss_mb"] for s in p_samples]
            pss_s = [s["pss_mb"] for s in p_samples]
            phase_memory[phase] = {
                "rss_avg": sum(rss_s) / len(rss_s),
                "rss_max": max(rss_s),
                "pss_avg": sum(pss_s) / len(pss_s),
                "pss_max": max(pss_s)
            }
        elif samples:
            closest_sample = min(samples, key=lambda s: abs(s["time"] - p_start))
            phase_memory[phase] = {
                "rss_avg": closest_sample["rss_mb"],
                "rss_max": closest_sample["rss_mb"],
                "pss_avg": closest_sample["pss_mb"],
                "pss_max": closest_sample["pss_mb"]
            }
        else:
            phase_memory[phase] = {
                "rss_avg": 0.0, "rss_max": 0.0,
                "pss_avg": 0.0, "pss_max": 0.0
            }

    # Print Consolidated Results Report
    print("\n" + "=" * 105)
    print(f"{'PHASE PROFILING & RESOURCE METRICS (WORKLOAD: ' + str(PAGES) + ' PAGES)':^105}")
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
        
        rss_avg = f"{phase_memory[phase]['rss_avg']:7.1f} MB" if phase_memory[phase]["rss_avg"] > 0 else "N/A"
        rss_max = f"{phase_memory[phase]['rss_max']:7.1f} MB" if phase_memory[phase]["rss_max"] > 0 else "N/A"
        pss_avg = f"{phase_memory[phase]['pss_avg']:7.1f} MB" if phase_memory[phase]["pss_avg"] > 0 else "N/A"
        pss_max = f"{phase_memory[phase]['pss_max']:7.1f} MB" if phase_memory[phase]["pss_max"] > 0 else "N/A"
        
        print(
            f"  {phase:20} | {wall_str:10} | {cpu_str:8} | "
            f"{rss_avg:11} | {rss_max:11} | {pss_avg:11} | {pss_max:11}"
        )

    print("\n" + "=" * 105)
    print(f"{'AGGREGATE HARDWARE PERFORMANCE':^105}")
    print("=" * 105)
    print(f"  Average Process CPU Utilisation:      {avg_cpu:6.2f}%")
    print(f"  Average Resident Memory (RSS):        {avg_rss:6.2f} MB  |  Peak Resident Memory (RSS): {max_rss:6.2f} MB")
    print(f"  Average Proportional Memory (PSS):    {avg_pss:6.2f} MB  |  Peak Proportional Memory (PSS): {max_pss:6.2f} MB")
    print("=" * 105)


if __name__ == "__main__":
    run_and_profile()