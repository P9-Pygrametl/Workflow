"""Run a child process and measure wall time, CPU time, and peak memory."""

import resource
import subprocess
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


def tree_pss_bytes(root_pid):
    """Summed PSS of a process and its descendants.

    Jython launches Java as a child process, so the root PID alone
    would miss the JVM.
    """
    try:
        root = psutil.Process(root_pid)
        processes = [root] + root.children(recursive=True)
    except PSUTIL_ERRORS:
        return 0

    total = 0
    for process in processes:
        try:
            total += process.memory_full_info().pss
        except PSUTIL_ERRORS:
            pass
    return total


def cpu_time():
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    return usage.ru_utime + usage.ru_stime


def run_child(command, environment):
    """Run a command to completion and measure it.

    The window covers interpreter startup through connection close.
    """
    cpu_start = cpu_time()
    wall_start = time.perf_counter()
    peak_pss = 0

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
                peak_pss = max(peak_pss, tree_pss_bytes(process.pid))
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
        peak_pss_mb=peak_pss / (1024 * 1024) or None,
        returncode=process.returncode,
        error=output[-ERROR_TAIL_CHARS:] if failed and output else None,
    )
