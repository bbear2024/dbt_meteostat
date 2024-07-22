WITH airports_regions_join AS (
    SELECT * 
    FROM {{source('flights', 'airports')}}
    LEFT JOIN {{source('flights', 'regions')}}
    USING (country)
)
SELECT * FROM airports_regions_join