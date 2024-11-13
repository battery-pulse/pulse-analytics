import pytest


source_views = [  # source, view
    ("telemetry.telemetry", "analytics.telemetry"),
    ("telemetry.statistics_step", "analytics.statistics_step"),
    ("telemetry.statistics_cycle", "analytics.statistics_cycle"),
    ("metadata.device_part_tests", "analytics.device_part_tests"),
    ("metadata.device_metadata", "analytics.device_metadata"),
    ("metadata.part_metadata", "analytics.part_metadata"),
]

@pytest.mark.parametrize("table_name, view_name", source_views)
def test_sources(database_cursor, table_name, view_name):
    # Fetch data from the backing table and the view
    table_contents = database_cursor.execute(f"SELECT * FROM {table_name}").fetchall()
    view_contents = database_cursor.execute(f"SELECT * FROM {view_name}").fetchall()

    # View should have the same row count as the backing table
    assert len(table_contents) == len(view_contents), f"Row count should be equal for {table_name} and {view_name}"

    # View should have the same columns as the backing table
    assert len(table_contents[0]) == len(view_contents[0]), f"Columns should be the same for {table_name} and {view_name}"

    # View should have the same data as the backing table
    for row_table, row_view in zip(table_contents, view_contents):
        assert row_table == row_view, f"Data mismatch between {table_name} and {view_name}"
