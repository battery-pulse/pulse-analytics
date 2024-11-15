{{ config(
    materialized='view'
) }}


WITH records_with_part AS (
    -- Phase 1: Join records with device_part_tests and part_metadata
    SELECT
        dpt.part_id,
        t.*,  -- All columns from statistics_cycle
        pm.*  -- All columns from part_metadata
    FROM {{ ref('statistics_cycle') }} AS t
    INNER JOIN {{ ref('device_part_tests') }} AS dpt
        ON t.device_id = dpt.device_id
        AND t.test_id = dpt.test_id
    LEFT JOIN {{ ref('part_metadata') }} AS pm  -- Keep rows without part metadata
        ON dpt.part_id = pm.part_id
),

renumbered AS (
    -- Phase 3: Reindex cycle number
    SELECT
        *,
        ROW_NUMBER() OVER part_window AS part_cycle_number
    FROM records_with_part
    WINDOW part_window AS (
        PARTITION BY part_id
        ORDER BY start_time, cycle_number
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
)

-- Final output
SELECT * FROM renumbered
