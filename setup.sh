#!/bin/bash
set -e

OS=$(uname -s)

echo "Detecting operating system..."

# Set this to the Postgres role that will own pygrametl_source and
# pygrametl_dw. This value is also written into .env as USERNAME.
DB_USER="postgres"

if [ "$DB_USER" = "youruser" ]; then
    echo "Please edit setup.sh and set DB_USER to your database username."
    exit 1
fi

create_role_if_missing() {
    # $1 = role name
    if sudo -u postgres psql -tAc "SELECT 1 FROM pg_roles WHERE rolname='$1'" | grep -q 1; then
        echo "Role '$1' already exists."
    else
        echo "Creating role '$1'..."
        sudo -u postgres createuser "$1"
    fi
    sudo -u postgres psql -c "ALTER ROLE \"$1\" WITH CREATEDB;"
}

recreate_db() {
    # $1 = database name, $2 = owner role
    echo "Recreating database '$1'..."
    sudo -u postgres dropdb --if-exists "$1"
    sudo -u postgres createdb -O "$2" "$1"
}

if [ "$OS" = "Darwin" ]; then
    echo "macOS detected. Using Homebrew."
    brew update
    brew install postgresql python3
    brew services start postgresql
    sleep 2 # Wait for Postgres to start

    if ! psql -tAc "SELECT 1 FROM pg_roles WHERE rolname='${DB_USER}'" postgres | grep -q 1; then
        echo "Creating role '${DB_USER}'..."
        createuser -d "$DB_USER"
    else
        echo "Role '${DB_USER}' already exists."
    fi

    echo "Recreating database 'pygrametl_source'..."
    dropdb --if-exists pygrametl_source
    createdb -O "$DB_USER" pygrametl_source
    echo "Recreating database 'pygrametl_dw'..."
    dropdb --if-exists pygrametl_dw
    createdb -O "$DB_USER" pygrametl_dw
    psql -d pygrametl_dw -f starschema.sql

elif [ "$OS" = "Linux" ]; then
    if [ -f /etc/os-release ]; then
        . /etc/os-release

        if [[ "$ID" == "arch" || "$ID_LIKE" == *"arch"* ]]; then
            echo "Arch Linux detected."
            yay -S --noconfirm postgresql python

            if [ ! -d "/var/lib/postgres/data/base" ]; then
                sudo -u postgres initdb -D /var/lib/postgres/data
            fi

            sudo systemctl enable --now postgresql
            sudo systemctl restart postgresql
            sleep 2

        elif [[ "$ID" == "ubuntu" || "$ID" == "debian" || "$ID_LIKE" == *"debian"* ]]; then
            echo "Ubuntu/Debian detected."
            sudo apt update
            sudo apt install -y postgresql python3-venv libpq-dev

            sudo service postgresql start
            sleep 2

        else
            echo "Unsupported Linux distribution. Exiting."
            exit 1
        fi

        # Linux common DB setup using the entered role
        create_role_if_missing "$DB_USER"
        recreate_db pygrametl_source "$DB_USER"
        recreate_db pygrametl_dw "$DB_USER"
        sudo -u postgres psql -d pygrametl_dw -f starschema.sql
    else
        echo "Cannot determine Linux distribution. Exiting."
        exit 1
    fi
else
    echo "Unsupported Operating System. Exiting."
    exit 1
fi

echo "Setting up Python environment..."
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

if [ -f .env ]; then
    echo ".env already exists — leaving it as is."
else
    echo "Generating .env file..."
    cat <<EOF > .env
USERNAME="${DB_USER}"
SOURCE_DATABASE="pygrametl_source"
DW_DATABASE="pygrametl_dw"
SOURCE_HOST="localhost"
SOURCE_PORT="5432"

# Needed for latency benchmarks
TOXIPROXY_API="http://localhost:8474"
TOXIPROXY_PROXY="source-postgres"
EOF
fi

echo "Setup complete! You can now activate your environment, start the Docker containers, and run the ETL:"
echo "  source .venv/bin/activate"
echo "  docker compose -f docker-compose.network-benchmark.yml up -d"
echo "  python3 datagenerator/datagenerator_db.py"
echo "  python3 benchmarks/benchmark.py"