import subprocess

from benchmark_config import DW_DATABASE, ROOT


def reset_warehouse():
    subprocess.run(
        ["psql", DW_DATABASE, "-f", str(ROOT / "starschema.sql")],
        check=True,
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
    )