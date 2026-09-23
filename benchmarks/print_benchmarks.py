import statistics

from benchmark_config import PHASES, IMPLEMENTATIONS

def print_summary(results):
    print("\nBenchmark summary")
    print("-" * 60)

    workload_sizes = sorted({result["workload_pages"] for result in results})
    for pages in workload_sizes:
        latencies = sorted(
            {
                result["source_rtt_ms"]
                for result in results
                if result["workload_pages"] == pages
            }
        )

        for source_rtt_ms in latencies:
            print(f"\nWorkload: pages={pages}, RTT latency={source_rtt_ms} ms")
            name, = IMPLEMENTATIONS
            matching = [
                result
                for result in results
                if result["workload_pages"] == pages
                and result["source_rtt_ms"] == source_rtt_ms
                and result["implementation"] == name
            ]
            print_benchmark_summary(name, matching)


def print_benchmark_summary(name, results):
    wall_times = [result["clean_wall_seconds"] for result in results]
    cpu_times = [result["python_cpu_seconds"] for result in results]
    waiting_times = [result["waiting_seconds"] for result in results]
    cpu_percentages = [result["cpu_percent"] for result in results]

    print(
        f"{name:10} median wall={statistics.median(wall_times):.2f}s | "
        f"median Python CPU={statistics.median(cpu_times):.2f}s | "
        f"median waiting={statistics.median(waiting_times):.2f}s"
    )
    print(
        f"{'':10} median CPU utilisation="
        f"{statistics.median(cpu_percentages):.2f}%"
    )
    print("  Median phase wall / CPU / waiting:")

    for phase in PHASES:
        percentages = [result[f"profile_{phase}_percent"] for result in results]
        cpu_seconds = [
            result[f"profile_{phase}_cpu_seconds"] for result in results
        ]
        waiting_seconds = [
            result[f"profile_{phase}_waiting_seconds"] for result in results
        ]
        print(
            f"    {phase:20} {statistics.median(percentages):6.2f}% "
            f"CPU={statistics.median(cpu_seconds):.2f}s "
            f"waiting={statistics.median(waiting_seconds):.2f}s"
        )


def print_profile_summary(timings, timings_cpu, timings_waiting, total_wall, total_cpu):
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