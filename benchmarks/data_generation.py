import subprocess
import sys

from benchmark_config import ROOT
from toxiproxy import reset_latency_toxics

def generate_sources(pages):
    print(f"Generating source data for pages={pages}...")
    subprocess.run(
        [sys.executable, str(ROOT / "datagenerator/datagenerator_db.py"), "--pages", str(pages)],
        check=True,
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
    )
    reset_latency_toxics(strict=True)