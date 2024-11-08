{{ config(
    materialized='view'
) }}

SELECT * FROM {{ source('metadata_source', 'part_metadata') }}
