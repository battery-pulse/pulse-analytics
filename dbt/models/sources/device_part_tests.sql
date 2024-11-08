{{ config(
    materialized='view'
) }}

SELECT
    device_id,
    test_id,
    part_id
FROM {{ source('metadata_source', 'device_part_tests') }}
