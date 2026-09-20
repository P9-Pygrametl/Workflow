# pygrametl ETL Benchmarks

## Contents
- [Setup](#setup)
- [Running the ETL](#running-the-etl)
- [Benchmarking](#benchmarking)
- [Network-latency benchmarking](#network-latency-benchmarking-docker--toxiproxy)
- [Legacy Setups](#legacy-setups)

## Setup

### 1. Python environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. PostgreSQL

This installs PostgreSQL directly on your machine. The [Docker setup](#network-latency-benchmarking-docker--toxiproxy)
moves the source database into a container instead.

**Arch Linux**
 
```bash
yay -S postgresql
sudo -u postgres initdb -D /var/lib/postgres/data
sudo systemctl enable --now postgresql
sudo -u postgres createuser YOURUSERNAME
```
 
**macOS**
 
```bash
brew install postgresql
brew services start postgresql
```

### 3. Configuration

Create a `.env` file in the project root based on `example.env`:

```env
USERNAME="your_username"
SOURCE_DATABASE="pygrametl_source"
DW_DATABASE="pygrametl_dw"
SOURCE_HOST="localhost"
SOURCE_PORT="5432"
 
# Needed for latency benchmarks
TOXIPROXY_API="http://localhost:8474"
TOXIPROXY_PROXY="source-postgres"
```

| Variable | Meaning |
|---|---|
| `USERNAME` | Your system username, used to connect to PostgreSQL |
| `SOURCE_DATABASE` | Database holding the PostgreSQL source data |
| `DW_DATABASE` | Target data warehouse database |
| `SOURCE_HOST`, `SOURCE_PORT` | Where the source database runs (defaults to local PostgreSQL) |
| `TOXIPROXY_API`, `TOXIPROXY_PROXY` | Toxiproxy API URL and proxy name, required for non-zero latency |

### 4. Databases and schema

**Arch Linux**
 
```bash
sudo -u postgres createdb -O YOURUSERNAME pygrametl_source
sudo -u postgres createdb -O YOURUSERNAME pygrametl_dw
```
 
**macOS**
 
```bash
createdb pygrametl_source
createdb pygrametl_dw
```
 
Create the warehouse schema:
 
```bash
psql pygrametl_dw -f starschema.sql
```

## Running the ETL

```bash
python3 datagenerator/datagenerator_db.py
python3 cpygrametl1_db.py
```

### Verifying the result
 
The ETL writes to the `pygrametlexa` schema in `DW_DATABASE`:
 
```bash
psql pygrametl_dw
```
 
```sql
SELECT COUNT(*) FROM pygrametlexa.testresults;
SELECT COUNT(*) FROM pygrametlexa.page;
```

With the default generated data, the expected result is:
 
```text
testresults: 9,000,000 rows
page:          974,535 rows
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
|---|---|---|
| `--page-sizes N [N ...]` | Workload sizes, as generator page counts | `100` |
| `--source-rtt-latency MS [MS ...]` | Simulated round-trip times to the PostgreSQL source, in milliseconds. Needs [Toxiproxy](#network-latency-benchmarking-docker--toxiproxy) | `0` |

Every page size is run at every latency, for example:
 
```bash
SOURCE_HOST=localhost SOURCE_PORT=15432 \
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
|---|---|---|
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

### 1. Start the containers
 
Docker must be installed and running.
 
```bash
docker compose \
  -f docker-compose.network-benchmark.yml \
  --env-file .env \
  up -d
```
 
Check that both containers are running. PostgreSQL should eventually report
`healthy`:
 
```bash
docker compose \
  -f docker-compose.network-benchmark.yml \
  ps
```
 
### 2. Create the Toxiproxy proxy
 
This forwards port `15432` to the PostgreSQL container. Check first with
`curl http://localhost:8474/proxies`, and skip this step if `source-postgres`
is already listed.
 
```bash
curl -X POST http://localhost:8474/proxies \
  -H "Content-Type: application/json" \
  -d '{
    "name": "source-postgres",
    "listen": "0.0.0.0:15432",
    "upstream": "source-db:5432"
  }'
```

### 3. Run the benchmark
 
**Docker PostgreSQL directly** (no Toxiproxy):
 
```bash
SOURCE_HOST=localhost SOURCE_PORT=55432 \
python3 benchmarks/benchmark.py
```
 
**Through Toxiproxy**:

```bash
SOURCE_HOST=localhost SOURCE_PORT=15432 \
python3 benchmarks/benchmark.py
```

Use the 0 ms run through Toxiproxy as the baseline for latency experiments,
not local PostgreSQL on port 5432.

### 4. Stop the containers
 
```bash
docker compose \
  -f docker-compose.network-benchmark.yml \
  down
```
 
The generated source data lives in a Docker volume and survives this. To
delete the volume as well:
 
```bash
docker compose \
  -f docker-compose.network-benchmark.yml \
  down -v
```

## Legacy setups

Older ways of running the ETL. The benchmark doesn't need these. Each section
lists its own steps. PostgreSQL itself is installed as described in
[Setup](#setup).

### Jython ('pygrametl1.py')

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

#### CPython, CSV Source (cpygrametl1.py'):

Setup .env with your system username.

**Arch**:

```
sudo -u postgres createdb -O YOURUSERNAME YOURUSERNAME
psql -f starschema.sql

python3 ./datagenerator/datagenerator.py
python3 cpygrametl1.py //or python3 cpygrametl1spy3.py
```

**MacOS**

```bash
createdb "$(whoami)"
psql -f starschema.sql

python3 ./datagenerator/datagenerator.py
python3 cpygrametl1.py //or python3 cpygrametl1spy3.py
```
