## Contents

- [Contents](#contents)
- [Setup](#setup)
- [Running the ETL](#running-the-etl)
- [Benchmarking](#benchmarking)
- [Network-latency benchmarking (Docker + Toxiproxy)](#network-latency-benchmarking-docker--toxiproxy)
- [Legacy setups](#legacy-setups)

## Setup

The `setup.sh` script automates the installation of PostgreSQL and Python dependencies, configures the necessary databases (`pygrametl_source` and `pygrametl_dw`), and generates your Python virtual environment and `.env` file.

1. Edit `setup.sh` and set the `DB_USER` variable to your desired PostgreSQL username.
2. Run the script:

```bash
./setup.sh
```

> **Important:** The setup script gives the chosen Postgres user superuser permissions.

## Running the ETL

```bash
python3 datagenerator/datagenerator_db.py
python3 cpygrametl1_db.py
```

## Benchmarking

The benchmark compares `cpygrametl1.py` (CSV source) and `cpygrametl1_db.py`
(PostgreSQL source). For each page size it regenerates the source data itself
and resets the warehouse before every run, so you don't need to run the
generator first.

```bash
python3 benchmarks/benchmark.py
```

### Options

| Option | Meaning | Default |
| --- | --- | --- |
| `--page-sizes N [N ...]` | Workload sizes, as generator page counts | `100` |
| `--source-rtt-latency MS [MS ...]` | Simulated round-trip times to the PostgreSQL source, in milliseconds. Needs [Toxiproxy](#network-latency-benchmarking-docker--toxiproxy) | `0` |

Every page size is run at every latency, for example:

```bash
SOURCE_PORT=15432 \
python3 benchmarks/benchmark.py --page-sizes 1 2 --source-rtt-latency 0 50 100
```

> **Important:** `--source-rtt-latency` only has an effect when
> `SOURCE_PORT=15432`. On any other port the ETL bypasses the proxy and
> the latency is never applied.

For each RTT, the script splits the delay evenly between the upstream and
downstream directions. It clears any leftover latency before generating data
and again when it finishes.

### Environment variables

| Variable | Meaning | Default |
| --- | --- | --- |
| `REPEATS` | Repetitions per configuration | `1` |
| `BENCHMARK_IMPLEMENTATION` | `both`, `csv`, or `db` / `database` | `both` |

With `REPEATS=1` each result is a single measurement, so use `REPEATS=3` or
more if you want to compare small differences.

### Results

Results are appended to a SQLite database, one row per run:

```text
data/benchmark_results.db    (table: results)
```

## Network-latency benchmarking (Docker + Toxiproxy)

The setup uses PostgreSQL and Toxiproxy containers, with these paths:

```text
localhost:5432   -> local PostgreSQL
 
localhost:55432  -> Docker PostgreSQL (direct)
 
localhost:15432  -> Toxiproxy -> Docker PostgreSQL
```

The toxiproxy-init container automatically configures the proxy during startup.
Use the 0 ms run through Toxiproxy as the baseline for latency experiments,
not local PostgreSQL on port 5432.

### Start the containers

Docker must be installed and running.

```bash
docker compose -f docker-compose.network-benchmark.yml up -d
```

### Run the benchmark

**Docker PostgreSQL directly** (no Toxiproxy):

```bash
SOURCE_PORT=55432 \
python3 benchmarks/benchmark.py
```

**Through Toxiproxy**:

```bash
SOURCE_PORT=15432 \
python3 benchmarks/benchmark.py
```

### Stop the containers

The generated source data lives in a Docker volume and survives stopping the container. To
delete the volume as well:

```bash
docker compose -f docker-compose.network-benchmark.yml down -v
```

## Legacy setups

Older ways of running the ETL. The benchmark doesn't need these. Complete
[Setup](#setup) first.

### Jython (`pygrametl1.py`)

The original Jython-based example (`pygrametl1.py`).

Change lines 20-21 in `pygrametl1.py` to use your system username instead of
`chr`. On Arch:

```bash
yay -S jython postgresql-jdbc
 
sudo ln -s /opt/jython/bin/jython /usr/local/bin/jython
hash -r
 
sudo /opt/jython/bin/pip-jython install "pygrametl==2.6"
 
sudo -u postgres createdb -O YOURUSERNAME YOURUSERNAME
psql -f starschema.sql

python3 datagenerator/datagenerator.py
 
jython -J-cp /usr/share/java/postgresql-jdbc/postgresql.jar pygrametl1.py
```
