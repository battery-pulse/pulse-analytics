{{ config(
    materialized='view'
) }}


SELECT
    t.*,  -- Select all columns from telemetry
    dm.*  -- Select all columns from device_metadata
FROM {{ ref('telemetry') }} AS t
LEFT JOIN {{ ref('device_metadata') }} AS dm  -- Don't drop rows with no device metadata
    ON t.device_id = dm.device_id
