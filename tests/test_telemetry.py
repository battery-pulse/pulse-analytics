import os
import time
import duckdb

def test_thing(database_cursor):
    # Create schema if it doesn't already exist
    conn = database_cursor

    # Query to get catalog (database) name and schema names
    result = conn.execute("SELECT * from telemetry.telemetry").fetchall()
    print(len(result))

    result = conn.execute("SELECT * from analytics.telemetry").fetchall()
    print(len(result))

    result = conn.execute("SELECT * from analytics.part_telemetry").fetchall()
    print(len(result))

    result = conn.execute("SELECT * from analytics.device_telemetry").fetchall()
    print(len(result))

    result = conn.execute("SELECT step_number, new_step_number from analytics.part_statistics_step").fetchall()
    print(result)

    # result = conn.execute("SELECT new_step_number from analytics.part_telemetry").fetchall()
    # print(result)

    res = conn.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'analytics' AND table_name = 'part_telemetry'
    """)
    device_part_tests_columns = [row[0] for row in res.fetchall()]
    print("Columns in device_part_tests:", device_part_tests_columns)

    # time.sleep(5)

    raise Exception

