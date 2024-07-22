WITH hourly_data AS (
    SELECT * 
    FROM {{ref('staging_weather_hourly')}}
),
add_features AS (
    SELECT *
		      , timestamp::DATE AS date -- only time (hours:minutes:seconds) as TIME data type
		      , TO_CHAR(timestamp, 'HH24:MI:SS')::TIME AS time -- only time (hours:minutes:seconds) as TIME data type
          , TO_CHAR(timestamp,'HH24:MI')::TEXT as hour -- time (hours:minutes) as TEXT data type
          , TO_CHAR(timestamp, 'FMmonth')::TEXT AS month_name -- month name as a text
          , TO_CHAR(timestamp::DATE, 'day')::TEXT AS weekday -- weekday name as text        
          , DATE_PART('day', timestamp)::INTEGER	 AS date_day
          , DATE_PART('month', timestamp)::INTEGER AS date_month
          , DATE_PART('year', timestamp)::INTEGER AS date_year
          , DATE_PART('week', timestamp)::INTEGER AS cw
    FROM hourly_data
),
add_more_features AS (
    SELECT *
          ,(CASE 
            WHEN (time BETWEEN '21:00:00' AND '24:00:00') OR (time BETWEEN '00:00:00' AND '06:00:00') THEN 'night'
            WHEN time BETWEEN '06:00:00' AND '17:00:00' THEN 'day'
            WHEN time BETWEEN '17:00:00' AND '21:00:00' THEN 'evening'
          END) AS day_part
    FROM add_features
)
SELECT *
FROM add_more_features;