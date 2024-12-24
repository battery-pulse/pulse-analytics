{{ config(
    materialized='view'
) }}


WITH test_with_part AS (
    -- Phase 1: Join records with device_part_tests and part_metadata
    SELECT
        dpt.part_id,
        t.*,  -- All columns from test
        {{ prefix_columns('part_metadata', 'pm') }} -- Select all columns from part_metadata
    FROM {{ ref('test_statistics_step') }} AS t
    INNER JOIN {{ ref('device_test_part') }} AS dpt
        ON t.device_id = dpt.device_id
        AND t.test_id = dpt.test_id
    LEFT JOIN {{ ref('part_metadata') }} AS pm  -- Keep rows without part metadata
        ON dpt.part_id = pm.part_id
),

lagged AS (
    -- Phase 2: Calculate lags for step_number and cycle_number
    SELECT
        *,
        LAG(cycle_number) OVER part_window AS prev_cycle_number
    FROM test_with_part
    WINDOW part_window AS (PARTITION BY part_id ORDER BY start_time, step_number)
),

renumbered AS (
    -- Phase 3: Reindex cycle and step numbers
    SELECT
        *,
        SUM(CASE WHEN prev_cycle_number IS DISTINCT FROM cycle_number THEN 1 ELSE 0 END) OVER part_window AS part_cycle_number,
        ROW_NUMBER() OVER part_window AS part_step_number
    FROM lagged
    WINDOW part_window AS (
        PARTITION BY part_id
        ORDER BY start_time, step_number
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
)

-- Final output
SELECT * FROM renumbered
