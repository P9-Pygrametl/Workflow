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
- Python 3 with `venv`

## Setup

Create your local environment file and set `USERNAME` to your system user:

```bash
cp example.env .env
$EDITOR .env
```

Create the virtual environment and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Start the containers:

```bash
docker compose up -d
docker compose ps
```

Wait until `source-db` and `dw-db` both report `healthy`.

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

Results are written to `data/benchmark_results.db` (SQLite). You can browse
them at <http://localhost:8080>.

### Simulated source latency

Latency experiments route the source connection through Toxiproxy. Create the
proxy once:

```bash
curl -X POST http://localhost:8474/proxies \
  -H "Content-Type: application/json" \
  -d '{"name":"source-postgres","listen":"0.0.0.0:15432","upstream":"source-db:5432"}'
```

Then point the benchmark at the proxy port and request one or more round-trip
times in milliseconds:

```bash
SOURCE_PORT=15432 python3 benchmarks/benchmark.py \
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

## Legacy pipelines

Older Jython and CSV-based pipelines are kept for comparison only and are
documented separately in [`docs/legacy.md`](docs/legacy.md).
