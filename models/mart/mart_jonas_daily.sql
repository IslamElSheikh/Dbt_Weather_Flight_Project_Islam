{{ config(materialized='table') }}

WITH flights AS (
    SELECT
        flight_date,
        origin,
        cancelled,
        diverted,
        dep_delay,
        arr_delay
    FROM {{ ref('prep_flights_jonas') }}
),

flights_daily AS (
    SELECT
        flight_date,
        origin,
        COUNT(*) AS total_flights,
        SUM(cancelled) AS cancelled_flights,
        SUM(diverted) AS diverted_flights,
        AVG(dep_delay) AS avg_dep_delay_min,
        AVG(arr_delay) AS avg_arr_delay_min
    FROM flights
    GROUP BY 1,2
),

weather AS (
    SELECT
        date,
        airport_code AS origin,
        precipitation_mm,
        avg_wind_speed_kmh,
        max_snow_mm,
        max_temp_c,
        min_temp_c
    FROM {{ ref('prep_weather_daily') }}
),

joined AS (
    SELECT
        f.flight_date,
        f.origin,
        f.total_flights,
        f.cancelled_flights,
        f.diverted_flights,
        f.avg_dep_delay_min,
        f.avg_arr_delay_min,
        w.precipitation_mm,
        w.avg_wind_speed_kmh,
        w.max_snow_mm,
        w.max_temp_c,
        w.min_temp_c
    FROM flights_daily f
    LEFT JOIN weather w
        ON f.flight_date = w.date
       AND f.origin = w.origin
)

SELECT *
FROM joined
ORDER BY flight_date, origin