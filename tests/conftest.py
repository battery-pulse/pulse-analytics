
import os
import subprocess

import duckdb
import pytest
from trino.dbapi import connect as trino_connect

def pytest_addoption(parser):
    parser.addoption(
        "--db", action="store", default="duckdb", choices=["duckdb", "trino"],
        help="Choose the database to run tests against: 'duckdb' or 'trino'"
    )

@pytest.fixture(scope="session")
def duckdb_connection():
    db_path = os.getenv("DUCKDB_PATH", ":memory:")
    conn = duckdb.connect(database=db_path)
    yield conn
    conn.close()

@pytest.fixture(scope="session")
def trino_connection():
    trino_host = os.getenv("TRINO_HOST", "localhost")
    trino_port = int(os.getenv("TRINO_PORT", 8080))
    trino_user = os.getenv("TRINO_USER", "your_user")
    trino_catalog = os.getenv("TRINO_CATALOG", "analytics")

    conn = trino_connect(
        host=trino_host,
        port=trino_port,
        user=trino_user,
        catalog=trino_catalog,
        schema="public"
    )
    yield conn
    conn.close()

@pytest.fixture
def db_connection(request, duckdb_connection, trino_connection):
    """
    Fixture to switch between DuckDB and Trino connections based on the CLI option.
    """
    db_choice = request.config.getoption("--db")
    if db_choice == "duckdb":
        return duckdb_connection
    elif db_choice == "trino":
        return trino_connection
    else:
        raise ValueError("Invalid database option. Use 'duckdb' or 'trino'.")

@pytest.fixture
def db_cursor(db_connection):
    """
    Fixture to provide a cursor for the selected database connection.
    """
    cursor = db_connection.cursor()
    yield cursor
    cursor.close()


@pytest.fixture(scope="function")
def launch_dbt(request):
    db_choice = request.config.getoption("--db")
    if db_choice == "duckdb":
        # Run local dbt via shell script for DuckDB
        subprocess.run(["./run_dbt_local.sh"], check=True)
    elif db_choice == "trino":
        # Run dbt in Kubernetes via shell script for Trino
        subprocess.run(["./run_dbt_kubernetes.sh"], check=True)
