import pandas as pd
import pytest


def get_dataframe(cursor, query):
    cursor.execute(query)
    data = cursor.fetchall()
    columns = [i[0] for i in cursor.description]
    return pd.DataFrame(data=data, columns=columns)


device_marts = [  # source, metadata, mart
    ("analytics.telemetry", "analytics.device_metadata", "analytics.device_telemetry"),
    ("analytics.statistics_step", "analytics.device_metadata", "analytics.device_statistics_step"),
    ("analytics.statistics_cycle", "analytics.device_metadata", "analytics.device_statistics_cycle"),
]


@pytest.mark.parametrize("source_table, metadata_table, mart_table", device_marts)
def test_device_marts(database_cursor, source_table, metadata_table, mart_table):
    # Fetch data from the source, metadata, and mart
    source = get_dataframe(database_cursor, f"SELECT * FROM {source_table}")
    metadata = get_dataframe(database_cursor, f"SELECT * FROM {metadata_table}")
    mart = get_dataframe(database_cursor, f"SELECT * FROM {mart_table}")

    # Verifies that all source columns are present in the mart
    missing_source_columns = set(source.columns) - set(mart.columns)
    assert (
        not missing_source_columns
    ), f"Mart table {mart_table} is missing columns from source: {missing_source_columns}"

    # Verifies that all metadata columns are present in the mart
    missing_metadata_columns = set(metadata.columns) - set(mart.columns)
    assert (
        not missing_metadata_columns
    ), f"Mart table {mart_table} is missing columns from metadata: {missing_metadata_columns}"

    # Checks that source rows are not dropped if there are no corresponding metadata rows (left-join behavior)
    missing_metadata_rows = set(source["device_id"]) - set(metadata["device_id"])
    assert missing_metadata_rows, f"Expecting missing metadata rows for {mart_table}."
    assert len(mart) == len(source), f"Row count in mart {mart_table} should match source table row count."


def test_part_telemetry(database_cursor):
    # Fetch data from the source, metadata, and mart
    source = get_dataframe(database_cursor, "SELECT * FROM analytics.telemetry")
    metadata = get_dataframe(database_cursor, "SELECT * FROM analytics.part_metadata")
    mart = get_dataframe(database_cursor, "SELECT * FROM analytics.part_telemetry")

    # Verifies that all source columns are present in the mart
    missing_source_columns = set(source.columns) - set(mart.columns)
    assert not missing_source_columns, f"Mart table is missing columns from source: {missing_source_columns}"

    # Verifies that all metadata columns are present in the mart
    missing_metadata_columns = set(metadata.columns) - set(mart.columns)
    assert not missing_metadata_columns, f"Mart table is missing columns from metadata: {missing_metadata_columns}"

    # Checks that source rows are not dropped if there are no corresponding metadata rows (left-join behavior)
    missing_metadata_rows = set(mart["part_id"]) - set(metadata["part_id"])
    assert missing_metadata_rows, "Expecting missing metadata rows for."
    assert len(mart) == len(source), "Row count in mart should match source table row count."

    # Check part-based cycle, step, and record numbers
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


def test_part_statistics_step(database_cursor):
    # Fetch data from the source, metadata, and mart
    source = get_dataframe(database_cursor, "SELECT * FROM analytics.statistics_step")
    metadata = get_dataframe(database_cursor, "SELECT * FROM analytics.part_metadata")
    mart = get_dataframe(database_cursor, "SELECT * FROM analytics.part_statistics_step")

    # Verifies that all source columns are present in the mart
    missing_source_columns = set(source.columns) - set(mart.columns)
    assert not missing_source_columns, f"Mart table is missing columns from source: {missing_source_columns}"

    # Verifies that all metadata columns are present in the mart
    missing_metadata_columns = set(metadata.columns) - set(mart.columns)
    assert not missing_metadata_columns, f"Mart table is missing columns from metadata: {missing_metadata_columns}"

    # Checks that source rows are not dropped if there are no corresponding metadata rows (left-join behavior)
    missing_metadata_rows = set(mart["part_id"]) - set(metadata["part_id"])
    assert missing_metadata_rows, "Expecting missing metadata rows for."
    assert len(mart) == len(source), "Row count in mart should match source table row count."

    # Check part-based cycle, step, and record numbers
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


def test_part_statistics_cycle(database_cursor):
    # Fetch data from the source, metadata, and mart
    source = get_dataframe(database_cursor, "SELECT * FROM analytics.statistics_cycle")
    metadata = get_dataframe(database_cursor, "SELECT * FROM analytics.part_metadata")
    mart = get_dataframe(database_cursor, "SELECT * FROM analytics.part_statistics_cycle")

    # Verifies that all source columns are present in the mart
    missing_source_columns = set(source.columns) - set(mart.columns)
    assert not missing_source_columns, f"Mart table is missing columns from source: {missing_source_columns}"

    # Verifies that all metadata columns are present in the mart
    missing_metadata_columns = set(metadata.columns) - set(mart.columns)
    assert not missing_metadata_columns, f"Mart table is missing columns from metadata: {missing_metadata_columns}"

    # Checks that source rows are not dropped if there are no corresponding metadata rows (left-join behavior)
    missing_metadata_rows = set(mart["part_id"]) - set(metadata["part_id"])
    assert missing_metadata_rows, "Expecting missing metadata rows for."
    assert len(mart) == len(source), "Row count in mart should match source table row count."

    # Check part-based cycle, step, and record numbers
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
