import os
import stat
import subprocess

import duckdb
import pandas as pd
import pytest
import trino.dbapi
from pulse_telemetry.sparklib import iceberg, statistics_cycle, statistics_step, telemetry
from pulse_telemetry.utils import channel, telemetry_generator
from pyspark.sql import SparkSession
from sqlalchemy import create_engine, text

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
    os.environ["PULSE_ANALYTICS_DUCKDB_PATH"] = os.path.join(current_dir, "test.duckdb")  # 'test' is the catalog
    os.environ["PULSE_ANALYTICS_TARGET_SCHEMA"] = "analytics"
    # telemetry source
    os.environ["PULSE_ANALYTICS_TELEMETRY_CATALOG"] = "test"
    os.environ["PULSE_ANALYTICS_TELEMETRY_SCHEMA"] = "telemetry"
    os.environ["PULSE_ANALYTICS_TELEMETRY_TABLE"] = "telemetry"
    os.environ["PULSE_ANALYTICS_STATISTICS_STEP_TABLE"] = "statistics_step"
    os.environ["PULSE_ANALYTICS_STATISTICS_CYCLE_TABLE"] = "statistics_cycle"
    # metadata source
    os.environ["PULSE_ANALYTICS_METADATA_CATALOG"] = "test"
    os.environ["PULSE_ANALYTICS_METADATA_SCHEMA"] = "metadata"
    os.environ["PULSE_ANALYTICS_DEVICE_METADATA_TABLE"] = "device_metadata"
    os.environ["PULSE_ANALYTICS_PART_METADATA_TABLE"] = "part_metadata"
    os.environ["PULSE_ANALYTICS_RECIPE_METADATA_TABLE"] = "recipe_metadata"
    os.environ["PULSE_ANALYTICS_DEVICE_TEST_PART_TABLE"] = "device_test_part"
    os.environ["PULSE_ANALYTICS_DEVICE_TEST_RECIPE_TABLE"] = "device_test_recipe"


def trino_environment():
    setup_script_path = os.path.join(scripts_dir, "setup_kubernetes.sh")
    subprocess.run([setup_script_path], cwd=manifests_dir, check=True)
    subprocess.run(["docker", "build", "-t", "pulse-analytics:latest", "."], check=True)
    subprocess.run(["kind", "load", "docker-image", "pulse-analytics:latest", "--name", "kind"], check=True)


@pytest.fixture(scope="session")
def setup_environment(dbt_target):
    match dbt_target:
        case "duckdb":
            duckdb_environment()
            yield
            duckdb_path = os.environ["PULSE_ANALYTICS_DUCKDB_PATH"]
            if os.path.exists(duckdb_path):
                os.remove(duckdb_path)
        case "trino":
            trino_environment()
            trino_forward = subprocess.Popen(
                ["kubectl", "port-forward", "svc/trino-cluster-coordinator", "8443:8443"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            minio_forward = subprocess.Popen(
                ["kubectl", "port-forward", "svc/minio", "9000:9000"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            hive_forward = subprocess.Popen(
                ["kubectl", "port-forward", "svc/hive", "9083:9083"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            yield
            trino_forward.terminate()
            minio_forward.terminate()
            hive_forward.terminate()
            subprocess.run(["kind", "delete", "cluster"], check=True)


# Seeds database with sources


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.appName("E2ESeeding")
        # Iceberg and S3 packages
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0,org.apache.hadoop:hadoop-aws:3.3.4",
        )
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # Hive metastore configuration
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hive")
        .config("spark.sql.catalog.lakehouse.uri", "thrift://localhost:9083")  # Hive Metastore
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/")
        # S3 configuration
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")  # MinIO
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "adminadmin")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # Timezone
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def telemetry_sources(num_channels, spark):
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
    return telemetry_df, statistics_step_df, statistics_cycle_df


def metadata_sources(statistics_cycle_first, statistics_cycle_second):
    # device-test-part
    first_devices = statistics_cycle_first["device_id"].unique()
    first_tests = statistics_cycle_first["test_id"].unique()
    first_parts = list(range(len(first_tests)))
    first_df = pd.DataFrame(
        {
            "device_id": first_devices,
            "test_id": first_tests,
            "part_id": first_parts,
        }
    )
    second_devices = statistics_cycle_second["device_id"].unique()
    second_tests = statistics_cycle_second["test_id"].unique()
    second_parts = list(range(len(second_tests)))
    second_df = pd.DataFrame(
        {
            "device_id": second_devices,
            "test_id": second_tests,
            "part_id": second_parts,
        }
    )
    device_test_part = pd.concat([first_df, second_df], ignore_index=True)
    # device-test-recipe
    first_recipes = list(range(len(first_tests)))
    first_df = pd.DataFrame(
        {
            "device_id": first_devices,
            "test_id": first_tests,
            "recipe_id": first_recipes,
        }
    )
    second_recipes = list(range(len(second_tests)))
    second_df = pd.DataFrame(
        {
            "device_id": second_devices,
            "test_id": second_tests,
            "recipe_id": second_recipes,
        }
    )
    device_test_recipe = pd.concat([first_df, second_df], ignore_index=True)
    # device-metadata
    devices = device_test_part["device_id"].unique()[:-1]  # incomplete metadata
    device_values = list(range(len(devices)))
    device_metadata = pd.DataFrame({"device_id": devices, "device_value": device_values})
    # part-metadata
    parts = device_test_part["part_id"].unique()[:-1]  # incomplete metadata
    part_values = list(range(len(parts)))
    part_metadata = pd.DataFrame({"part_id": parts, "part_value": part_values})
    # recipe-metadata
    recipes = device_test_recipe["recipe_id"].unique()[:-1]  # incomplete metadata
    recipe_values = list(range(len(recipes)))
    recipe_metadata = pd.DataFrame({"recipe_id": recipes, "recipe_value": recipe_values})
    return device_test_part, device_test_recipe, device_metadata, part_metadata, recipe_metadata


def seed_duckdb(spark):
    conn = duckdb.connect(database=os.environ["PULSE_ANALYTICS_DUCKDB_PATH"])
    cursor = conn.cursor()
    # First set of tests (five parts put onto test)
    telemetry_df, statistics_step_df, statistics_cycle_df = telemetry_sources(num_channels=5, spark=spark)
    telemetry_df = telemetry_df.toPandas()
    statistics_step_df = statistics_step_df.toPandas()
    statistics_cycle_df = statistics_cycle_df.toPandas()
    cursor.execute("CREATE SCHEMA IF NOT EXISTS telemetry")
    cursor.execute("CREATE TABLE telemetry.telemetry AS SELECT * FROM telemetry_df")
    cursor.execute("CREATE TABLE telemetry.statistics_step AS SELECT * FROM statistics_step_df")
    cursor.execute("CREATE TABLE telemetry.statistics_cycle AS SELECT * FROM statistics_cycle_df")
    # Second set of tests (one part is not put back on test)
    telemetry_df_second, statistics_step_df_second, statistics_cycle_df_second = telemetry_sources(
        num_channels=4, spark=spark
    )
    telemetry_df_second = telemetry_df_second.toPandas()
    statistics_step_df_second = statistics_step_df_second.toPandas()
    statistics_cycle_df_second = statistics_cycle_df_second.toPandas()
    cursor.execute("INSERT INTO telemetry.telemetry SELECT * FROM telemetry_df_second")
    cursor.execute("INSERT INTO telemetry.statistics_step SELECT * FROM statistics_step_df_second")
    cursor.execute("INSERT INTO telemetry.statistics_cycle SELECT * FROM statistics_cycle_df_second")
    # Metadata (incomplete metadata on parts and devices)
    device_test_part, device_test_recipe, device_metadata, part_metadata, recipe_metadata = metadata_sources(
        statistics_cycle_df, statistics_cycle_df_second
    )
    cursor.execute("CREATE SCHEMA IF NOT EXISTS metadata")
    cursor.execute("CREATE TABLE metadata.device_test_part AS SELECT * FROM device_test_part")
    cursor.execute("CREATE TABLE metadata.device_test_recipe AS SELECT * FROM device_test_recipe")
    cursor.execute("CREATE TABLE metadata.device_metadata AS SELECT * FROM device_metadata")
    cursor.execute("CREATE TABLE metadata.part_metadata AS SELECT * FROM part_metadata")
    cursor.execute("CREATE TABLE metadata.recipe_metadata AS SELECT * FROM recipe_metadata")
    # Close connection before running DBT
    conn.commit()
    conn.close()


def seed_trino(spark):
    # Generate the test data (2 sets of tests)
    telemetry_df, statistics_step_df, statistics_cycle_df = telemetry_sources(num_channels=5, spark=spark)
    telemetry_df_second, statistics_step_df_second, statistics_cycle_df_second = telemetry_sources(
        num_channels=4, spark=spark
    )

    # Load telemetry data
    iceberg.create_table_if_not_exists(
        spark, "lakehouse", "telemetry", "telemetry", telemetry.telemetry_comment, telemetry.telemetry_schema
    )
    iceberg.merge_into_table(
        spark, telemetry_df, "lakehouse", "telemetry", "telemetry", telemetry.telemetry_composite_key
    )
    iceberg.merge_into_table(
        spark, telemetry_df_second, "lakehouse", "telemetry", "telemetry", telemetry.telemetry_composite_key
    )
    iceberg.create_table_if_not_exists(
        spark,
        "lakehouse",
        "telemetry",
        "statistics_step",
        statistics_step.statistics_step_comment,
        statistics_step.statistics_step_schema,
    )
    iceberg.merge_into_table(
        spark,
        statistics_step_df,
        "lakehouse",
        "telemetry",
        "statistics_step",
        statistics_step.statistics_step_composite_key,
    )
    iceberg.merge_into_table(
        spark,
        statistics_step_df_second,
        "lakehouse",
        "telemetry",
        "statistics_step",
        statistics_step.statistics_step_composite_key,
    )
    iceberg.create_table_if_not_exists(
        spark,
        "lakehouse",
        "telemetry",
        "statistics_cycle",
        statistics_cycle.statistics_cycle_comment,
        statistics_cycle.statistics_cycle_schema,
    )
    iceberg.merge_into_table(
        spark,
        statistics_cycle_df,
        "lakehouse",
        "telemetry",
        "statistics_cycle",
        statistics_cycle.statistics_cycle_composite_key,
    )
    iceberg.merge_into_table(
        spark,
        statistics_cycle_df_second,
        "lakehouse",
        "telemetry",
        "statistics_cycle",
        statistics_cycle.statistics_cycle_composite_key,
    )

    # Generate metadata (based on first and second tests)
    device_test_part, device_test_recipe, device_metadata, part_metadata, recipe_metadata = metadata_sources(
        statistics_cycle_df.toPandas(), statistics_cycle_df_second.toPandas()
    )

    # Load metadata tables
    engine = create_engine(
        "trino://admin:admin@localhost:8443/lakehouse",
        connect_args={
            "http_scheme": "https",  # Use "http" if Trino doesn't use SSL
            "verify": False,  # For production, provide CA path: "path/to/ca.crt"
        },
    )
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS metadata"))
        device_test_part.to_sql("device_test_part", con=engine, schema="metadata", if_exists="replace", index=False)
        device_test_recipe.to_sql("device_test_recipe", con=engine, schema="metadata", if_exists="replace", index=False)
        device_metadata.to_sql("device_metadata", con=engine, schema="metadata", if_exists="replace", index=False)
        part_metadata.to_sql("part_metadata", con=engine, schema="metadata", if_exists="replace", index=False)
        recipe_metadata.to_sql("recipe_metadata", con=engine, schema="metadata", if_exists="replace", index=False)


@pytest.fixture(scope="session")
def seed_database(dbt_target, setup_environment, spark):
    match dbt_target:
        case "duckdb":
            seed_duckdb(spark)
            yield
        case "trino":
            seed_trino(spark)
            yield


# Launches DBT run (tests rely on side effects from this fixture)


@pytest.fixture(scope="session")
def launch_dbt(dbt_target, seed_database):
    match dbt_target:
        case "duckdb":
            launch_script_path = os.path.join(scripts_dir, "launch_dbt_local.sh")
            os.chmod(launch_script_path, os.stat(launch_script_path).st_mode | stat.S_IEXEC)
            try:
                subprocess.run([launch_script_path], cwd=dbt_dir, check=True)
            except subprocess.CalledProcessError:
                pytest.fail("DBT run failure", pytrace=False)
        case "trino":
            launch_script_path = os.path.join(scripts_dir, "launch_dbt_kubernetes.sh")
            os.chmod(launch_script_path, os.stat(launch_script_path).st_mode | stat.S_IEXEC)
            try:
                subprocess.run([launch_script_path], cwd=manifests_dir, check=True)
            except subprocess.CalledProcessError:
                pytest.fail("DBT run failure", pytrace=False)
    yield


# Sets up database connection for individual tests
# "execute" and "fetchall" are supported by both duckdb and trino


@pytest.fixture(scope="session")
def database_cursor(dbt_target, launch_dbt):
    match dbt_target:
        case "duckdb":
            conn = duckdb.connect(database=os.environ["PULSE_ANALYTICS_DUCKDB_PATH"])
            yield conn.cursor()
            conn.close()
        case "trino":
            conn = trino.dbapi.connect(
                host="localhost",
                port=8443,
                http_scheme="https",
                verify=False,
                user="admin",
                catalog="lakehouse",
                auth=trino.auth.BasicAuthentication("admin", "admin"),
            )
            yield conn.cursor()
            conn.close()
