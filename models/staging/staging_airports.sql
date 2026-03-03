{{ config(materialized='view') }}

WITH airports_regions_join AS (
    SELECT *
    FROM {{ source('static_data', 'airports') }}
    LEFT JOIN {{ source('static_data', 'regions') }}
    USING (country)
)
SELECT *
FROM airports_regions_join