{{ config(
    materialized='view'
) }}

SELECT * FROM {{ source('telemetry_source', 'telemetry') }}
