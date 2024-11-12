{{ config(
    materialized='view'
) }}


WITH telemetry_with_part AS (
    -- Join telemetry records with device_part_tests to add part_id and part_metadata
    SELECT
        dpt.part_id,
        t.*,  -- Select all columns from telemetry
        pm.*  -- Select all columns from part_metadata
    FROM {{ ref('telemetry') }} AS t
    INNER JOIN {{ ref('device_part_tests') }} AS dpt
        ON t.device_id = dpt.device_id
        AND t.test_id = dpt.test_id
    LEFT JOIN {{ ref('part_metadata') }} AS pm  -- Don't drop rows with no part metadata
        ON dpt.part_id = pm.part_id
),

ordered_telemetry AS (
    -- Sort by timestamp and record_number, partitioning by part_id
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY part_id ORDER BY timestamp, record_number) AS new_record_number
    FROM telemetry_with_part
),

lagged_cycle AS (
    -- Calculate LAG for cycle_number without nesting
    SELECT
        *,
        LAG(cycle_number) OVER (PARTITION BY part_id ORDER BY new_record_number) AS prev_cycle_number
    FROM ordered_telemetry
),

cycle_numbered AS (
    -- Calculate the new_cycle_number based on changes in cycle_number
    SELECT
        *,
        SUM(CASE WHEN prev_cycle_number != cycle_number THEN 1 ELSE 0 END) 
        OVER (PARTITION BY part_id ORDER BY new_record_number) + 1 AS new_cycle_number
    FROM lagged_cycle
),

lagged_step AS (
    -- Calculate LAG for step_number without nesting
    SELECT
        *,
        LAG(step_number) OVER (PARTITION BY part_id ORDER BY new_record_number) AS prev_step_number
    FROM cycle_numbered
),

step_numbered AS (
    -- Calculate the new_step_number based on changes in step_number
    SELECT
        *,
        SUM(CASE WHEN prev_step_number != step_number THEN 1 ELSE 0 END) 
        OVER (PARTITION BY part_id ORDER BY new_record_number) + 1 AS new_step_number
    FROM lagged_step
)

-- Final output with explicit column selection
SELECT 
    *,  -- Select all columns from step_numbered
    new_cycle_number AS cycle_number,    -- Rename new_cycle_number to cycle_number
    new_step_number AS step_number,      -- Rename new_step_number to step_number
    new_record_number AS record_number   -- Rename new_record_number to record_number
FROM step_numbered
