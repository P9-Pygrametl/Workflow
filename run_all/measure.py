"""Run a child process and measure wall time, CPU time, and peak memory."""

import resource
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass

import psutil

from run_all.config import ROOT


POLL_INTERVAL = 0.05
ERROR_TAIL_CHARS = 2000
PSUTIL_ERRORS = (
    psutil.NoSuchProcess,
    psutil.ZombieProcess,
    psutil.AccessDenied,
)

VERSION_CODE = (
    "import sys\n"
    "print('PYVER=' + sys.version.split()[0])\n"
    "try:\n"
    "    import pygrametl\n"
    "    print('PGVER=' + str(getattr(pygrametl, '__version__', 'unknown')))\n"
    "except Exception:\n"
    "    print('PGVER=unknown')\n"
)


@dataclass
class Measurement:
    wall_seconds: float | None = None
    cpu_seconds: float | None = None
    peak_pss_mb: float | None = None
    peak_rss_mb: float | None = None
    returncode: int | None = None
    error: str | None = None

    @property
    def cpu_percent(self):
        if not self.wall_seconds or self.cpu_seconds is None:
            return None
        return self.cpu_seconds / self.wall_seconds * 100


def get_versions(interpreter):
    """Return (runtime_version, pygrametl_version) for an interpreter."""
    versions = {"PYVER": "unknown", "PGVER": "unknown"}

    try:
        completed = subprocess.run(
            interpreter + ["-c", VERSION_CODE],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=60,
        )
    except (OSError, subprocess.TimeoutExpired):
        return versions["PYVER"], versions["PGVER"]

    for line in completed.stdout.splitlines():
        key, _, value = line.partition("=")
        if key in versions:
            versions[key] = value

    return versions["PYVER"], versions["PGVER"]


def tree_memory_bytes(root_pid):
    """Return summed PSS and RSS for a process tree.
    
    Jython launches Java as a child process, so the root PID alone
    would miss the JVM.
    """
    try:
        root = psutil.Process(root_pid)
        processes = [root] + root.children(recursive=True)
    except PSUTIL_ERRORS:
        return None, 0

    total_pss = 0 if sys.platform == "linux" else None
    total_rss = 0

    for process in processes:
        try:
            total_rss += process.memory_info().rss

            if total_pss is not None:
                total_pss += process.memory_full_info().pss
        except PSUTIL_ERRORS:
            pass

    return total_pss, total_rss


def cpu_time():
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    return usage.ru_utime + usage.ru_stime


def run_child(command, environment):
    """Run a command to completion and measure it.

    The window covers interpreter startup through connection close.
    """
    cpu_start = cpu_time()
    wall_start = time.perf_counter()
    peak_pss = None
    peak_rss = 0

    try:
        with tempfile.TemporaryFile("w+") as out, tempfile.TemporaryFile("w+") as err:
            process = subprocess.Popen(
                command,
                cwd=ROOT,
                env=environment,
                stdout=out,
                stderr=err,
                text=True,
            )
            while process.poll() is None:
                pss, rss = tree_memory_bytes(process.pid)

                if pss is not None:
                    peak_pss = max(peak_pss or 0, pss)

                peak_rss = max(peak_rss, rss)
                time.sleep(POLL_INTERVAL)

            out.seek(0)
            err.seek(0)
            output = err.read().strip() or out.read().strip()
    except OSError as exception:
        return Measurement(error=str(exception))

    failed = process.returncode != 0
    return Measurement(
        wall_seconds=time.perf_counter() - wall_start,
        cpu_seconds=cpu_time() - cpu_start,
        peak_pss_mb=peak_pss / (1024 * 1024) if peak_pss else None,
        peak_rss_mb=peak_rss / (1024 * 1024) if peak_rss else None,
        returncode=process.returncode,
        error=output[-ERROR_TAIL_CHARS:] if failed and output else None,
    )
