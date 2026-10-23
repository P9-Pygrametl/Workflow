# Legacy Jython pipeline

`pygrametl1.py` and `pygrametl2.py` are the original pygrametl examples running
under Jython. They are kept for comparison against the Docker-based benchmark
in the [README](../README.md).

Both scripts read from the Docker PostgreSQL source database and write to the
Docker warehouse. Connections come from `.env`: `SOURCE_HOST`, `SOURCE_PORT`,
`SOURCE_DATABASE`, `DW_HOST`, `DW_PORT`, `DW_DATABASE`, and `USERNAME`. No
native PostgreSQL instance is required.

## Requirements

Jython 2.7 requires Java 8, and pygrametl 2.6 is the last release that supports
Python 2.7. On Arch Linux:

```bash
sudo pacman -S jdk8-openjdk jython
sudo archlinux-java set java-8-openjdk
yay -S postgresql-jdbc

/opt/jython/bin/pip-jython install --user "pygrametl==2.6"
```

The Arch package does not put Jython on `PATH`, so use
`/opt/jython/bin/jython` explicitly. The PostgreSQL JDBC driver must be on the
JVM classpath with `-J-cp`.

## Run

Run these commands from the repository root. Start both databases and load the
source data:

```bash
docker compose up -d source-db dw-db

.venv/bin/python -m datagenerator.datagenerator_db --pages 100
```

Use `--pages 1` for a quick smoke test. With `--pages 100`, the source database
contains 900,000 download rows and 4,500,000 test result rows.

Run the sequential pipeline:

```bash
/opt/jython/bin/jython \
  -J-Xmx4g \
  -J-cp /usr/share/java/postgresql-jdbc/postgresql.jar \
  jython/pygrametl1.py
```

Run the parallel pipeline:

```bash
/opt/jython/bin/jython \
  -J-Xmx4g \
  -J-cp /usr/share/java/postgresql-jdbc/postgresql.jar \
  jython/pygrametl2.py
```

## Tuning

`tune.py` compares heap and GC settings using wall and CPU time:

```bash
.venv/bin/python jython/tune.py --pages 10 --repeats 3
```

It generates the source data unless `--skip-generate` is given and writes
results to `data/jython_tuning.csv`.
