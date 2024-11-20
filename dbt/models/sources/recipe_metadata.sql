{{ config(
    materialized='view'
) }}

SELECT * FROM {{ source('metadata_source', 'recipe_metadata') }}
