{{ config(
    materialized='view'
) }}


WITH statistics_cycle_with_part AS (
    -- Join statistics_cycle records with device_part_tests to add part_id and part_metadata
    SELECT
        dpt.part_id,
        t.*,  -- Select all columns from statistics_cycle
        pm.*  -- Select all columns from part_metadata
    FROM {{ ref('statistics_cycle') }} AS t
    INNER JOIN {{ ref('device_part_tests') }} AS dpt
        ON t.device_id = dpt.device_id
        AND t.test_id = dpt.test_id
    LEFT JOIN {{ ref('part_metadata') }} AS pm  -- Don't drop rows with no part metadata
        ON dpt.part_id = pm.part_id
),

ordered_statistics_cycle AS (
    -- Sort by start_time and cycle_number, partitioning by part_id
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY part_id ORDER BY start_time, cycle_number) AS new_cycle_number
    FROM statistics_cycle_with_part
)

-- Final output
SELECT 
    *,  -- Select all columns from cycle_numbered
    new_cycle_number AS cycle_number,    -- Overwrite cycle_number with new_cycle_number
FROM ordered_statistics_cycle
