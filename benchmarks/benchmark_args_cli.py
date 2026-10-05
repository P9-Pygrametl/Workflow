import argparse


DEFAULT_PAGE_SIZES = [100]
DEFAULT_SOURCE_RTT_MS = [0]


def parse_args():
    """Parse command-line options for workload sizes and source latency."""
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark the PostgreSQL ETL implementation "
            "across one or more workload sizes."
        )
    )

    parser.add_argument(
        "--page-sizes",
        nargs="+",
        dest="page_sizes",
        type=int,
        default=DEFAULT_PAGE_SIZES,
        help=(
            "Workload sizes expressed as generator page counts. "
            "Example: --page-sizes 10 25 50 100"
        ),
    )

    parser.add_argument(
        "--source-rtt-latency",
        nargs="+",
        dest="source_rtt_latency",
        type=int,
        default=None,
        help=(
            "Simulated source round-trip times in milliseconds. "
            "Example: --source-rtt-latency 0 50 100"
        ),
    )

    args = parser.parse_args()
    args.source_rtt_latency_requested = args.source_rtt_latency is not None
    if args.source_rtt_latency is None:
        args.source_rtt_latency = DEFAULT_SOURCE_RTT_MS
    return args


def validate_page_sizes(sizes):
    """Validate that all requested page sizes are positive."""
    invalid_sizes = [size for size in sizes if size < 1]

    if invalid_sizes:
        raise ValueError(
            f"All sizes must be positive integers, got {invalid_sizes!r}"
        )


def validate_source_latency(latencies):
    """Validate that all requested source latencies are non-negative."""
    invalid_latencies = [
        latency
        for latency in latencies
        if latency < 0
    ]

    if invalid_latencies:
        raise ValueError(
            "Latency values must be non-negative integers, "
            f"got {invalid_latencies!r}"
        )