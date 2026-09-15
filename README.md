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

pip install pygrametl psycopg2 psycopg python-dotenv

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

pip install pygrametl psycopg2 psycopg python-dotenv

psql -f starschema.sql

python3 cpygrametl1.py //or python3 cpygrametl1spy3.py
```


## PostgreSQL Source Database

The project now supports benchmarking the existing CSV-based ETL against an equivalent PostgreSQL source.

Create a `.env` file in the project root based on `example.env`:

```env
USERNAME="your_username"
SOURCE_DATABASE="pygrametl_source"
DW_DATABASE="pygrametl_dw"
```
Create the two PostgreSQL databases:
```
createdb pygrametl_source
createdb pygrametl_dw
```
Create the warehouse schema: 
```
psql pygrametl_dw < starschema.sql
```
Generate the original CSV source data:
```
python3 datagenerator/datagenerator.py
```
Generate the equivalent PostgreSQL source data:
```
python3 datagenerator/datagenerator_db.py
```
With the default generator settings, the PostgreSQL source contains:
```
downloadlog: 1,800,000 rows
testresults: 9,000,000 rows
```
