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

Change to system username in cpygreametl1.py line 11
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

pip install pygrametl psycopg2

psql -f starschema.sql

python3 cpygrametl1.py
```