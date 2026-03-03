WITH flights_jonas AS (
    SELECT *
    FROM {{ source('flights_data', 'flights_jonas') }}
)

SELECT *
FROM flights_jonas