# PostgreSQL ETL Benchmark

This project benchmarks a pygrametl ETL that extracts from a generated
PostgreSQL source database and loads a PostgreSQL warehouse. The benchmark
reports wall-clock and Python CPU time for several workload sizes, and can
simulate network latency on the source connection with Toxiproxy.

The source and target databases both run in Docker (see
`docker-compose.yml`), alongside Toxiproxy and a small SQLite web UI for
inspecting benchmark results.

## Requirements

- Docker with the Compose plugin
- Python 3 using uv


## Setup
Create your local environment file and set `USERNAME` to your system user:

### env
```bash
cp example.env .env
$EDITOR .env
```

### Docker
Start the containers:

```bash
docker compose up -d
docker compose ps
```

Wait until `source-db` and `dw-db` both report `healthy`.

### venv
Create the virtual environment and install dependencies:
```bash
uv python install 3.15 3.15t

uv venv --python 3.15 .venv --clear
uv pip install --python .venv/bin/python -e ../pygrametl #/path/to/pygrametl
uv pip install --python .venv/bin/python -r requirements.txt

uv venv --python 3.15t .venv-t --clear
uv pip install --python .venv-t/bin/python -e ../pygrametl #/path/to/pygrametl
uv pip install --python .venv-t/bin/python -r requirements.txt
```

### Running it 

```bash
# Run with default:
.venv/bin/python -m run_all.main 

# Or with explicit page size and repeat:
.venv/bin/python -m run_all.main --page-sizes 25 50 75 100 --repeats 10 
```

Benchmarks load `data/tuned.json` when it exists. Use `--defaults` to ignore
the tuned profile, or `--tuned path.json` to select another one.

### Tuning the implementations

`run_all.tune` uses coordinate descent: for each mode it evaluates every
level of one knob while the others are fixed, repeating passes until the
configuration stops changing. The default is two repeats per level and at
most three passes. After the search, it selects the configuration with the
lowest median wall time among configurations measured at least four times,
then confirms that configuration with two more runs.

```bash
# See the search plan without running it:
.venv/bin/python -m run_all.tune --dry-run

# Measure the default configuration at the target size before committing:
.venv/bin/python -m run_all.tune --probe --pages 100 --repeats 1

# Full search at 100 pages:
.venv/bin/python -m run_all.tune --pages 100 --repeats 3

# Smoke-test two modes at a small size, without touching data/tuned.json:
.venv/bin/python -m run_all.tune \
  --pages 1 --repeats 1 --passes 1 \
  --modes cpygrametl1_db@cpython-3.15 jython2@jython-2.7 \
  --output /tmp/opencode/tuned-smoke.json \
  --db /tmp/opencode/tuning-smoke.db
```

Every trial is written to `data/tuning.db`, and the winner for each mode is
merged into `data/tuned.json`. Re-running the command resumes from the trial
database; add `--skip-generate` to reuse the existing source data, or
`--fresh` to delete the trial database and start over.


## Ports

| Port  | Service                                  |
|-------|------------------------------------------|
| 55432 | PostgreSQL source database (direct)      |
| 15432 | PostgreSQL source through Toxiproxy      |
| 55433 | PostgreSQL target warehouse              |
| 8474  | Toxiproxy API                            |
| 8080  | sqlite-web UI for benchmark results      |

## Running the benchmark

```bash
python3 benchmarks/benchmark.py --page-sizes 1
```

`--page-sizes` takes one or more generator page counts and runs each workload
size in turn. `--page-sizes 1` is a quick check; the default is `100`.

Each configuration runs `REPEATS` times (set in `.env`, default `1`).
Use 5 or more for meaningful medians.

Results are written to `data/benchmark_results.db` (SQLite). You can browse
them at <http://localhost:8080>.

### Simulated source latency

Latency experiments route the source connection through Toxiproxy.

To request one or more round-trip times in milliseconds:

```bash
python3 benchmarks/benchmark.py \
  --page-sizes 1 --source-rtt-latency 0 50 100
```

With `--source-rtt-latency 0` the proxy is a zero-latency baseline, which is
the right comparison point for the latency runs.

## Stopping and cleaning up

```bash
docker compose down        # stop containers, keep data volumes
docker compose down -v     # also delete the source and warehouse volumes
```

The generated source and warehouse data live in the `source_pgdata` and
`dw_pgdata` volumes and survive a plain `down`.

## Legacy pipeline

The original Jython pygrametl pipeline is kept for comparison only and is
documented separately in [`jython/README.md`](jython/README.md).