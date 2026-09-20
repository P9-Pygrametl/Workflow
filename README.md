Arch Linux:
This creates a postgres instance on your machine which is not optimal, but for a starting point it may be fine

In pygrametl1.py change line 20-21 to use your system username instead of chr

Then afterwards run these commands:

```
python3 -m venv .venv
source .venv/bin/activate 

python3 ./datagenerator/datagenerator.py

yay -S jython postgresql-jdbc postgresql

sudo ln -s /opt/jython/bin/jython /usr/local/bin/jython
hash -r

sudo -u postgres initdb -D /var/lib/postgres/data

sudo systemctl enable --now postgresql

sudo /opt/jython/bin/pip-jython install "pygrametl==2.6"

sudo -u postgres createuser YOURUSERNAME
sudo -u postgres createdb -O YOURUSERNAME YOURUSERNAME

psql -f starschema.sql

jython -J-cp /usr/share/java/postgresql-jdbc/postgresql.jar pygrametl1.py
```


CPython:

Setup .env with your system username
Arch:
```
python3 -m venv .venv
source .venv/bin/activate 

python3 ./datagenerator/datagenerator.py

yay -S postgresql

sudo -u postgres initdb -D /var/lib/postgres/data

sudo systemctl enable --now postgresql

sudo -u postgres createuser YOURUSERNAME
sudo -u postgres createdb -O YOURUSERNAME YOURUSERNAME

pip install -r requirements.txt

psql -f starschema.sql

python3 cpygrametl1.py //or python3 cpygrametl1spy3.py
```

MacOS:
```
python3 -m venv .venv
source .venv/bin/activate

python3 ./datagenerator/datagenerator.py

brew install postgresql
brew services start postgresql

createdb "$(whoami)"

pip install -r requirements.txt

psql -f starschema.sql

python3 cpygrametl1.py //or python3 cpygrametl1spy3.py
```


## PostgreSQL Source Database

The project supports benchmarking the original CSV-based ETL against an
equivalent PostgreSQL source.

Create a `.env` file in the project root based on `example.env`:

```env
USERNAME="your_username"
SOURCE_DATABASE="pygrametl_source"
DW_DATABASE="pygrametl_dw"
SOURCE_HOST="localhost"
SOURCE_PORT="5432"
```

`SOURCE_HOST` and `SOURCE_PORT` specify where the PostgreSQL source database
is running. The defaults above use a PostgreSQL instance running locally.

Create the source and target databases:

```bash
createdb pygrametl_source
createdb pygrametl_dw
```

Create the warehouse schema:

```bash
psql pygrametl_dw < starschema.sql
```

Generate the original CSV source data:

```bash
python3 datagenerator/datagenerator.py
```

Generate the equivalent PostgreSQL source data:

```bash
python3 datagenerator/datagenerator_db.py
```

With the default generator settings, the PostgreSQL source contains:

```text
downloadlog: 1,800,000 rows
testresults: 9,000,000 rows
```

The PostgreSQL-source ETL can then be run with:

```bash
python3 cpygrametl1_db.py
```

The target warehouse is stored in `pygrametl_dw` using the
`pygrametlexa` schema.

To verify the result:

```bash
psql pygrametl_dw
```

Then:

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

The benchmark compares:

- `cpygrametl1.py`: CSV source
- `cpygrametl1_db.py`: PostgreSQL source

Run the benchmark with:

```bash
python3 benchmarks/benchmark.py --page-sizes 10 25 50 100
```

The benchmark regenerates the CSV and PostgreSQL source data for each
requested workload size, then resets the target warehouse between runs and
records clean wall-clock and Python CPU measurements. It also performs
separate profiled runs to estimate where time is spent during the ETL.

The values passed to `--page-sizes` are generator page counts. You can provide
any number of positive integers to compare different workload sizes in one run.

Results are written to:

```text
benchmarks/benchmark_results.csv
```

The benchmark results file is generated locally and should not be committed.

The number of repetitions can be configured using `REPEATS` in
`benchmarks/benchmark.py`.

## Docker PostgreSQL Source and Network-Latency Benchmarking

A Docker-based PostgreSQL source can be used to investigate the effect of
network overhead and artificial latency on database extraction.

The target warehouse remains the normal local `pygrametl_dw` database.
Only the source database is moved into Docker.

The setup uses:

- PostgreSQL in Docker
- Toxiproxy in Docker
- port `55432` for direct access to the Docker PostgreSQL source
- port `15432` for access through Toxiproxy
- port `8474` for the Toxiproxy API

This gives the following paths:

```text
localhost:5432
    -> local PostgreSQL

localhost:55432
    -> Docker PostgreSQL directly

localhost:15432
    -> Toxiproxy
    -> Docker PostgreSQL
```

### Start the Docker Environment

Docker must be installed and running.

Start the PostgreSQL and Toxiproxy containers:

```bash
docker compose \
  -f docker-compose.network-benchmark.yml \
  --env-file .env \
  up -d
```

Check that both containers are running:

```bash
docker compose \
  -f docker-compose.network-benchmark.yml \
  ps
```

The PostgreSQL container should eventually report `healthy`.

### Configure Toxiproxy

Create a proxy which forwards port `15432` to the PostgreSQL container:

```bash
curl -X POST http://localhost:8474/proxies \
  -H "Content-Type: application/json" \
  -d '{
    "name": "source-postgres",
    "listen": "0.0.0.0:15432",
    "upstream": "source-db:5432"
  }'
```

Verify the proxy:

```bash
curl http://localhost:8474/proxies
```

Initially the proxy contains no toxics, meaning that no artificial latency
is added.

### Generate the Docker Source Data

Generate the PostgreSQL source data directly against the Docker PostgreSQL
instance:

```bash
SOURCE_HOST=localhost SOURCE_PORT=55432 \
python3 datagenerator/datagenerator_db.py
```

The values supplied before the command override `SOURCE_HOST` and
`SOURCE_PORT` from `.env` for that command only.

Verify the generated data:

```bash
psql \
  -h localhost \
  -p 55432 \
  -U "$(whoami)" \
  -d pygrametl_source
```

Then:

```sql
SELECT COUNT(*) FROM downloadlog;
SELECT COUNT(*) FROM testresults;
```

The expected counts are:

```text
downloadlog:  1,800,000
testresults:  9,000,000
```

### Benchmark the Docker Database Directly

To benchmark PostgreSQL running in Docker without Toxiproxy:

```bash
SOURCE_HOST=localhost SOURCE_PORT=55432 \
python3 benchmarks/benchmark.py
```

### Benchmark Through Toxiproxy

To benchmark the same Docker PostgreSQL database through Toxiproxy:

```bash
SOURCE_HOST=localhost SOURCE_PORT=15432 \
python3 benchmarks/benchmark.py
```

With no toxics configured, this represents the Docker + Toxiproxy
zero-latency baseline.

This baseline should be used when evaluating artificial network latency,
rather than comparing latency experiments directly against the normal
local PostgreSQL instance.

### Stop the Docker Environment

Stop the containers with:

```bash
docker compose \
  -f docker-compose.network-benchmark.yml \
  down
```

The generated PostgreSQL source data is stored in a Docker volume and is
therefore preserved when the containers are stopped.

To also delete the generated database volume:

```bash
docker compose \
  -f docker-compose.network-benchmark.yml \
  down -v
```