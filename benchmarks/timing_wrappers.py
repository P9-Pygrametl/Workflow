import time


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