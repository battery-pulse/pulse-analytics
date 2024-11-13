{{ config(
    materialized='view'
) }}

SELECT * FROM {{ source('telemetry_source', 'statistics_cycle') }}
