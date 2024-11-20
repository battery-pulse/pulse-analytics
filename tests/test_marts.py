import pandas as pd
import pytest


def get_dataframe(cursor, query):
    cursor.execute(query)
    data = cursor.fetchall()
    columns = [i[0] for i in cursor.description]
    return pd.DataFrame(data=data, columns=columns)


mart_triplets = [  # source, metadata, mart
    # device marts
    ("analytics.telemetry", ("analytics.device_metadata", "analytics.recipe_metadata"), "analytics.test_telemetry"),
    (
        "analytics.statistics_step",
        ("analytics.device_metadata", "analytics.recipe_metadata"),
        "analytics.test_statistics_step",
    ),
    (
        "analytics.statistics_cycle",
        ("analytics.device_metadata", "analytics.recipe_metadata"),
        "analytics.test_statistics_cycle",
    ),
    # part marts
    (
        "analytics.telemetry",
        ("analytics.device_metadata", "analytics.recipe_metadata", "analytics.part_metadata"),
        "analytics.part_telemetry",
    ),
    (
        "analytics.statistics_step",
        ("analytics.device_metadata", "analytics.recipe_metadata", "analytics.part_metadata"),
        "analytics.part_statistics_step",
    ),
    (
        "analytics.statistics_cycle",
        ("analytics.device_metadata", "analytics.recipe_metadata", "analytics.part_metadata"),
        "analytics.part_statistics_cycle",
    ),
]


@pytest.mark.parametrize("source_table, metadata_tables, mart_table", mart_triplets)
def test_mart_columns(database_cursor, source_table, metadata_tables, mart_table):
    source = get_dataframe(database_cursor, f"SELECT * FROM {source_table}")
    mart = get_dataframe(database_cursor, f"SELECT * FROM {mart_table}")

    # Verifies that all source columns are present in the mart
    missing_source_columns = set(source.columns) - set(mart.columns)
    assert (
        not missing_source_columns
    ), f"Mart table {mart_table} is missing columns from source: {missing_source_columns}"

    # Verifies that all metadata columns are present in the mart
    for i in metadata_tables:
        metadata = get_dataframe(database_cursor, f"SELECT * FROM {i}")
        missing_metadata_columns = set(metadata.columns) - set(mart.columns)
        assert (
            not missing_metadata_columns
        ), f"Mart table {mart_table} is missing columns from metadata: {missing_metadata_columns}"


@pytest.mark.parametrize("source_table, metadata_tables, mart_table", mart_triplets)
def test_mart_metadata_behavior(database_cursor, source_table, metadata_tables, mart_table):
    source = get_dataframe(database_cursor, f"SELECT * FROM {source_table}")
    mart = get_dataframe(database_cursor, f"SELECT * FROM {mart_table}")

    # Checks that source rows are not dropped if there are no corresponding metadata rows (left-join behavior)
    for i in metadata_tables:
        metadata = get_dataframe(database_cursor, f"SELECT * FROM {i}")
        if "device" in i:
            missing_metadata_rows = set(mart["device_id"]) - set(metadata["device_id"])
        elif "recipe" in i:
            missing_metadata_rows = set(mart["recipe_id"]) - set(metadata["recipe_id"])
        elif "part" in i:
            missing_metadata_rows = set(mart["part_id"]) - set(metadata["part_id"])
        else:
            raise ValueError("Tests support device and part marts.")
        assert missing_metadata_rows, f"Expecting missing metadata rows for {mart_table}."
    assert len(mart) == len(source), f"Row count in mart {mart_table} should match source table row count."


def test_part_telemetry_renumbering(database_cursor):
    mart = get_dataframe(database_cursor, "SELECT * FROM analytics.part_telemetry")
    for _, i in mart.groupby("part_id"):
        # Case with two tests on a part
        if len(i) == 60:
            assert min(i["part_cycle_number"]) == 1
            assert min(i["part_step_number"]) == 1
            assert min(i["part_record_number"]) == 1
            assert max(i["part_cycle_number"]) == 4
            assert max(i["part_step_number"]) == 12
            assert max(i["part_record_number"]) == 60
        # Case with one test on a part
        elif len(i) == 30:
            assert min(i["part_cycle_number"]) == 1
            assert min(i["part_step_number"]) == 1
            assert min(i["part_record_number"]) == 1
            assert max(i["part_cycle_number"]) == 2
            assert max(i["part_step_number"]) == 6
            assert max(i["part_record_number"]) == 30
        else:
            raise Exception("Unexpected length of records.")


def test_part_statistics_step_renumbering(database_cursor):
    mart = get_dataframe(database_cursor, "SELECT * FROM analytics.part_statistics_step")
    for _, i in mart.groupby("part_id"):
        # Case with two tests on a part
        if len(i) == 12:
            assert min(i["part_cycle_number"]) == 1
            assert min(i["part_step_number"]) == 1
            assert max(i["part_cycle_number"]) == 4
            assert max(i["part_step_number"]) == 12
        # Case with one test on a part
        elif len(i) == 6:
            assert min(i["part_cycle_number"]) == 1
            assert min(i["part_step_number"]) == 1
            assert max(i["part_cycle_number"]) == 2
            assert max(i["part_step_number"]) == 6
        else:
            raise Exception("Unexpected length of records.")


def test_part_statistics_cycle_renumbering(database_cursor):
    mart = get_dataframe(database_cursor, "SELECT * FROM analytics.part_statistics_cycle")
    for _, i in mart.groupby("part_id"):
        # Case with two tests on a part
        if len(i) == 4:
            assert min(i["part_cycle_number"]) == 1
            assert max(i["part_cycle_number"]) == 4
        # Case with one test on a part
        elif len(i) == 2:
            assert min(i["part_cycle_number"]) == 1
            assert max(i["part_cycle_number"]) == 2
        else:
            raise Exception("Unexpected length of records.")
