{{ config(
    materialized='view'
) }}

SELECT * FROM {{ source('metadata_source', 'device_metadata') }}
