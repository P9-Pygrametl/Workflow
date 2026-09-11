# Workflow

This folder contains both the Java/Jython version and the regular Python3 version.

## Benchmark output

Both ETL scripts now record time spent in the main phases to a CSV file under:

- `Workflow/benchmarks/python3/`
- `Workflow/benchmarks/java/`

Each CSV contains the following columns:

- `runtime`
- `phase`
- `seconds`
- `rows`

The measured phases are:

- `row_transform`
- `dimension_lookup`
- `fact_insert`
- `commit_and_close`
- `total`

This makes it easy to compare where time is spent between the Python3 and Java/Jython workflows.

## Java/Jython setup

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