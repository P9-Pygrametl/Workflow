import time


def wrap_with_timing(
    function,
    timing_name,
    timings,
    timings_cpu,
):
    """Wrap a function so its execution time is accumulated.

    Each call to the wrapped function measures both wall-clock and
    Python CPU time. The measurements are added to the supplied timing
    dictionaries under timing_name.

    Timing is recorded even if the wrapped function raises an exception.

    Args:
        function: Function to wrap with timing measurements.
        timing_name: Key used to store the accumulated measurements.
        timings: Dictionary containing accumulated wall-clock timings.
        timings_cpu: Dictionary containing accumulated CPU timings.

    Returns:
        A wrapped version of the supplied function.
    """
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
    """Wrap an iterable and measure the time spent retrieving its rows.

    Each call to the underlying iterator is timed and accumulated as
    part of the extraction_merge phase. The number of rows produced by
    the iterable is also counted.
    """

    def __init__(self, source, timings, timings_cpu):
        """Create a timed wrapper around an iterable source.

        Args:
            source: Iterable source to wrap.
            timings: Dictionary containing accumulated wall-clock timings.
            timings_cpu: Dictionary containing accumulated CPU timings.
        """
        self.source = source
        self.timings = timings
        self.timings_cpu = timings_cpu
        self.rows = 0

    def __iter__(self):
        """Yield rows while measuring time spent retrieving each row."""
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
    """Proxy an object while timing selected method calls.

    Methods specified in the methods mapping are replaced with timed
    wrappers. Calls to all other attributes and methods are forwarded
    to the original object.
    """

    def __init__(self, obj, methods, timings, timings_cpu):
        """Create a proxy with timing enabled for selected methods.

        Args:
            obj: Object to proxy.
            methods: Mapping from method names to timing phase names.
            timings: Dictionary containing accumulated wall-clock timings.
            timings_cpu: Dictionary containing accumulated CPU timings.
        """
        self._obj = obj

        for method_name, timing_name in methods.items():
            method = getattr(obj, method_name)

            setattr(
                self,
                method_name,
                wrap_with_timing(
                    method,
                    timing_name,
                    timings,
                    timings_cpu,
                ),
            )

    def __getattr__(self, name):
        """Forward attributes not defined by the proxy to the original object."""
        return getattr(self._obj, name)