# Legacy pipelines

These pipelines predate the Docker-based PostgreSQL benchmark and are kept for
historical comparison only. They are not part of the main workflow described
in the [README](../README.md).

All of them expect a **native** PostgreSQL instance on `localhost:5432` and the
`pygrametl_dw` database.

## Arch Linux

Install and start PostgreSQL, then create your user and database:

```bash
yay -S postgresql

sudo -u postgres initdb -D /var/lib/postgres/data
sudo systemctl enable --now postgresql

sudo -u postgres createuser YOURUSERNAME
sudo -u postgres createdb -O YOURUSERNAME YOURUSERNAME
```

### Jython (pygrametl on the JVM)

Runs `pygrametl1.py` / `pygrametl2.py` under Jython.

```bash
yay -S jython postgresql-jdbc

sudo ln -s /opt/jython/bin/jython /usr/local/bin/jython
hash -r

sudo /opt/jython/bin/pip-jython install "pygrametl==2.6"
```

Edit the username referenced in `pygrametl1.py`, then generate the CSV source
data, apply the schema, and run:

```bash
python3 ./datagenerator/datagenerator.py
psql -f starschema.sql
jython -J-cp /usr/share/java/postgresql-jdbc/postgresql.jar pygrametl1.py
```

### CPython CSV pipeline

Runs `cpygrametl1.py` / `cpygrametl1psy3.py`, loading the generated
`DownloadLog.csv` and `TestResults.csv` into the warehouse.

```bash
python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt

python3 ./datagenerator/datagenerator.py
psql -f starschema.sql

python3 cpygrametl1.py   # or cpygrametl1psy3.py
```

## macOS

Install and start PostgreSQL and create your database:

```bash
brew install postgresql
brew services start postgresql

createdb "$(whoami)"
```

### CPython CSV pipeline

Runs `cpygrametl1.py` / `cpygrametl1psy3.py`, loading the generated
`DownloadLog.csv` and `TestResults.csv` into the warehouse.

```bash
python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt

python3 ./datagenerator/datagenerator.py
psql -f starschema.sql

python3 cpygrametl1.py   # or cpygrametl1psy3.py
```

> Jython is documented for Arch Linux only; the original setup used `yay`
> packages and systemd.
