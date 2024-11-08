{{ config(
    materialized='view'
) }}


telemetry_with_part AS (
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

cycle_numbered AS (
    -- Apply change detection to reindex cycle_number based on changes, partitioning by part_id
    SELECT
        *,
        SUM(
            CASE WHEN LAG(cycle_number) OVER (PARTITION BY part_id ORDER BY new_record_number) != cycle_number THEN 1 ELSE 0 END
        ) OVER (PARTITION BY part_id ORDER BY new_record_number) + 1 AS new_cycle_number
    FROM ordered_telemetry
),

step_numbered AS (
    -- Apply change detection to reindex step_number based on changes, partitioning by part_id
    SELECT
        *,
        SUM(
            CASE WHEN LAG(step_number) OVER (PARTITION BY part_id ORDER BY new_record_number) != step_number THEN 1 ELSE 0 END
        ) OVER (PARTITION BY part_id ORDER BY new_record_number) + 1 AS new_step_number
    FROM cycle_numbered
)

-- Final output
SELECT 
    step_numbered.*,  -- Select all columns from step_numbered
    new_cycle_number AS cycle_number,    -- Overwrite cycle_number with new_cycle_number
    new_step_number AS step_number       -- Overwrite step_number with new_step_number
    new_record_number AS record_number,  -- Overwrite record_number with new_record_number
FROM step_numbered
