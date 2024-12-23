{{ config(
    materialized='view'
) }}


SELECT
    dtr.recipe_id,  -- Prefix table with test recipe
    t.*,  -- Select all columns from telemetry
    {{ prefix_columns('device_metadata', 'dm') }}, -- Select all columns from device_metadata
    {{ prefix_columns('recipe_metadata', 'rm') }}  -- Select all columns from recipe_metadata
FROM {{ ref('statistics_cycle') }} AS t
LEFT JOIN {{ ref('device_metadata') }} AS dm  -- Don't drop rows with no device metadata
    ON t.device_id = dm.device_id
LEFT JOIN {{ ref('device_test_recipe') }} AS dtr  -- Don't drop rows where the recipe is not labeled
    ON t.device_id = dtr.device_id
    AND t.test_id = dtr.test_id
LEFT JOIN {{ ref('recipe_metadata') }} AS rm  -- Don't drop rows with no recipe metadata
    ON dtr.recipe_id = rm.recipe_id
