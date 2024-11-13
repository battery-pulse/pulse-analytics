{{ config(
    materialized='view'
) }}


WITH statistics_step_with_part AS (
    -- Join statistics_step records with device_part_tests to add part_id and part_metadata
    SELECT
        dpt.part_id,
        t.*,  -- Select all columns from statistics_step
        pm.*  -- Select all columns from part_metadata
    FROM {{ ref('statistics_step') }} AS t
    INNER JOIN {{ ref('device_part_tests') }} AS dpt
        ON t.device_id = dpt.device_id
        AND t.test_id = dpt.test_id
    LEFT JOIN {{ ref('part_metadata') }} AS pm  -- Don't drop rows with no part metadata
        ON dpt.part_id = pm.part_id
),

ordered_statistics_step AS (
    -- Sort by timestamp and record_number, partitioning by part_id
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY part_id ORDER BY start_time, step_number) AS part_step_number
    FROM statistics_step_with_part
),

lagged_cycle AS (
    -- Calculate LAG for cycle_number without nesting
    SELECT
        *,
        LAG(cycle_number) OVER (PARTITION BY part_id ORDER BY part_step_number) AS prev_cycle_number
    FROM ordered_statistics_step
),

cycle_numbered AS (
    -- Calculate the part_cycle_number based on changes in cycle_number
    SELECT
        *,
        SUM(CASE WHEN prev_cycle_number != cycle_number THEN 1 ELSE 0 END) 
        OVER (PARTITION BY part_id ORDER BY part_step_number) + 1 AS part_cycle_number
    FROM lagged_cycle
)

-- Final output
SELECT * FROM cycle_numbered
