{{ config(
    materialized='view'
) }}


WITH records_with_part AS (
    -- Phase 1: Join records with device_part_tests and part_metadata
    SELECT
        dpt.part_id,
        t.*,  -- All columns from telemetry
        pm.*  -- All columns from part_metadata
    FROM {{ ref('telemetry') }} AS t
    INNER JOIN {{ ref('device_part_tests') }} AS dpt
        ON t.device_id = dpt.device_id
        AND t.test_id = dpt.test_id
    LEFT JOIN {{ ref('part_metadata') }} AS pm  -- Keep rows without part metadata
        ON dpt.part_id = pm.part_id
),

lagged AS (
    -- Phase 2: Calculate lags for step_number and cycle_number
    SELECT
        *,
        LAG(cycle_number) OVER part_window AS prev_cycle_number,
        LAG(step_number) OVER part_window AS prev_step_number
    FROM records_with_part
    WINDOW part_window AS (PARTITION BY part_id ORDER BY timestamp, record_number)
),

final_numbering AS (
    -- Phase 3: Reindex cycle, step, and record numbers
    SELECT
        *,
        SUM(CASE WHEN prev_cycle_number IS DISTINCT FROM cycle_number THEN 1 ELSE 0 END) OVER part_window AS part_cycle_number,
        SUM(CASE WHEN prev_step_number IS DISTINCT FROM step_number THEN 1 ELSE 0 END) OVER part_window AS part_step_number,
        ROW_NUMBER() OVER part_window AS part_record_number
    FROM lagged
    WINDOW part_window AS (PARTITION BY part_id ORDER BY timestamp, record_number)
)

-- Final output
SELECT * FROM final_numbering
