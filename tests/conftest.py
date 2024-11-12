import os
import subprocess
import stat

import duckdb
import pandas as pd
import pytest
import trino.dbapi
from pyspark.sql import SparkSession

from pulse_telemetry.utils import channel, telemetry_generator
from pulse_telemetry.sparklib import telemetry, statistics_step, statistics_cycle


current_file_path = os.path.abspath(__file__)
current_dir = os.path.dirname(current_file_path)
dbt_dir = os.path.join(current_dir, "../dbt/")
scripts_dir = os.path.join(current_dir, "scripts")
manifests_dir = os.path.join(current_dir, "manifests")


# Determines target environment for DBT

def pytest_addoption(parser):
    parser.addoption(
        "--dbt-target",
        action="store",
        default="duckdb",
        choices=["duckdb", "trino"],
        help="Choose the database to run tests against: 'duckdb' or 'trino'",
    )


@pytest.fixture(scope="session")
def dbt_target(request):
    return request.config.getoption("--dbt-target")


# Sets up environment (either local environment or kubernetes cluster)

def duckdb_environment():
    # catalog and target schema
    os.environ["DUCKDB_PATH"] = os.path.join(current_dir, "test.duckdb")  # 'test' is the catalog
    os.environ["DUCKDB_TARGET_SCHEMA"] = "analytics"
    # telemetry source
    os.environ["DBT_TELEMETRY_CATALOG"] = "test"
    os.environ["DBT_TELEMETRY_SCHEMA"] = "telemetry"
    os.environ["DBT_TELEMETRY_TABLE"] = "telemetry"
    os.environ["DBT_STATISTICS_STEP_TABLE"] = "statistics_step"
    os.environ["DBT_STATISTICS_CYCLE_TABLE"] = "statistics_cycle"
    # metadata source
    os.environ["DBT_METADATA_CATALOG"] = "test"
    os.environ["DBT_METADATA_SCHEMA"] = "metadata"
    os.environ["DBT_DEVICE_METADATA_TABLE"] = "device_metadata"
    os.environ["DBT_PART_METADATA_TABLE"] = "part_metadata"
    os.environ["DBT_DEVICE_PART_TESTS_TABLE"] = "device_part_tests"


def trino_environment():
    setup_script_path = os.path.join(scripts_dir, "setup_kubernetes.sh")
    subprocess.run([setup_script_path], cwd=manifests_dir, check=True)


@pytest.fixture(scope="session")
def setup_environment(dbt_target):
    match dbt_target:
        case "duckdb":
            duckdb_environment()
            yield
            duckdb_path = os.environ["DUCKDB_PATH"]
            if os.path.exists(duckdb_path):
                os.remove(duckdb_path)
        case "trino":
            trino_environment()
            yield
            subprocess.run(["kind", "delete", "cluster"], check=True)


# Seeds database with sources

def telemetry_sources(num_channels):
    spark = SparkSession.builder.appName("Testing").getOrCreate()
    buffer = channel.LocalBuffer()
    channel.run_with_timeout(
        source=telemetry_generator.telemetry_generator,
        sink=buffer,
        topic="telemetry",
        num_channels=num_channels,
        timeout_seconds=3,
        acquisition_frequency=10,
        points_per_step=5,
        lower_voltage_limit=3,  # V
        upper_voltage_limit=4,  # V
        current=1.0,  # A
    )
    telemetry_df = buffer.dataframe(spark, telemetry.telemetry_schema)
    statistics_step_df = statistics_step.statistics_step(telemetry_df)
    statistics_cycle_df = statistics_cycle.statistics_cycle(statistics_step_df)
    return telemetry_df.toPandas(), statistics_step_df.toPandas(), statistics_cycle_df.toPandas()


def metadata_sources(statistics_cycle_first, statistics_cycle_second):
    # device-part-tests
    first_devices = statistics_cycle_first["device_id"].unique()
    first_tests = statistics_cycle_first["test_id"].unique()
    first_parts = list(range(len(first_tests)))
    first_df = pd.DataFrame({
        "device_id": first_devices,
        "test_id": first_tests,
        "part_id": first_parts,
    })
    second_devices = statistics_cycle_second["device_id"].unique()
    second_tests = statistics_cycle_second["test_id"].unique()
    second_parts = list(range(len(second_tests)))
    second_df = pd.DataFrame({
        "device_id": second_devices,
        "test_id": second_tests,
        "part_id": second_parts,
    })
    device_part_tests = pd.concat([first_df, second_df], ignore_index=True)
    # device-metadata
    devices = device_part_tests["device_id"].unique()[:-1]  # incomplete metadata
    device_values = list(range(len(devices)))
    device_metadata = pd.DataFrame({"device_id": devices, "device_value": device_values})
    # part-metadata
    parts = device_part_tests["part_id"].unique()[:-1]  # incomplete metadata
    part_values = list(range(len(parts)))
    part_metadata = pd.DataFrame({"part_id": parts, "part_value": part_values})
    return device_part_tests, device_metadata, part_metadata


def seed_duckdb():
    conn = duckdb.connect(database=os.environ["DUCKDB_PATH"])
    cursor = conn.cursor()
    # First set of tests (five parts put onto test)
    telemetry_df, statistics_step_df, statistics_cycle_df = telemetry_sources(num_channels=5)
    cursor.execute("CREATE SCHEMA IF NOT EXISTS telemetry")
    cursor.execute("CREATE TABLE telemetry.telemetry AS SELECT * FROM telemetry_df")
    cursor.execute("CREATE TABLE telemetry.statistics_step AS SELECT * FROM statistics_step_df")
    cursor.execute("CREATE TABLE telemetry.statistics_cycle AS SELECT * FROM statistics_cycle_df")
    # Second set of tests (one part is not put back on test)
    telemetry_df_second, statistics_step_df_second, statistics_cycle_df_second = telemetry_sources(num_channels=4)
    cursor.execute("INSERT INTO telemetry.telemetry SELECT * FROM telemetry_df_second")
    cursor.execute("INSERT INTO telemetry.statistics_step SELECT * FROM statistics_step_df_second")
    cursor.execute("INSERT INTO telemetry.statistics_cycle SELECT * FROM statistics_cycle_df_second")
    # Metadata (incomplete metadata on parts and devices)
    device_part_tests, device_metadata, part_metadata = metadata_sources(statistics_cycle_df, statistics_cycle_df_second)
    cursor.execute("CREATE SCHEMA IF NOT EXISTS metadata")
    cursor.execute("CREATE TABLE metadata.device_part_tests AS SELECT * FROM device_part_tests")
    cursor.execute("CREATE TABLE metadata.device_metadata AS SELECT * FROM device_metadata")
    cursor.execute("CREATE TABLE metadata.part_metadata AS SELECT * FROM part_metadata")
    # Close connection before running DBT
    conn.commit()
    conn.close()


@pytest.fixture(scope="session")
def seed_database(dbt_target, setup_environment):
    match dbt_target:
        case "duckdb":
            seed_duckdb()
            yield
        case "trino":
            yield


# Launches DBT run (tests rely on side effects from this fixture)

@pytest.fixture(scope="session")
def launch_dbt(dbt_target, seed_database):
    match dbt_target:
        case "duckdb":
            launch_script_path = os.path.join(scripts_dir, "launch_dbt_local.sh")
            os.chmod(launch_script_path, os.stat(launch_script_path).st_mode | stat.S_IEXEC)
            subprocess.run([launch_script_path], cwd=dbt_dir, check=True)
        case "trino":
            launch_script_path = os.path.join(scripts_dir, "launch_dbt_kubernetes.sh")
            os.chmod(launch_script_path, os.stat(launch_script_path).st_mode | stat.S_IEXEC)
            subprocess.run([launch_script_path], cwd=manifests_dir, check=True)
    yield


# Sets up database connection for individual tests
# "execute" and "fetchall" are supported by both duckdb and trino

@pytest.fixture(scope="session")
def database_cursor(dbt_target, launch_dbt):
    match dbt_target:
        case "duckdb":
            conn = duckdb.connect(database=os.environ["DUCKDB_PATH"])
            yield conn.cursor()
            conn.close()
        case "trino":
            yield trino.dbapi.connect()
