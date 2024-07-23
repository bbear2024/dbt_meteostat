WITH route AS (
		SELECT origin,
				dest,
				COUNT(*) AS num_flights,
				COUNT(DISTINCT tail_number) AS num_unique_airplanes,
				COUNT(DISTINCT airline) AS num_unique_airlines,
				MAKE_INTERVAL(mins => ROUND(AVG(actual_elapsed_time))::INT) AS avg_actual_elapsed_time,
				ROUND(AVG(arr_delay),2) AS avg_arr_delay,
				MAX(arr_delay) AS max_delay,
				MIN(arr_delay) AS min_delay,
				SUM(cancelled) AS num_cancelled,
				SUM(diverted) AS num_diverted
		FROM {{ref('prep_flights')}}
		GROUP BY origin, dest
)
SELECT origin,
		pa.name AS origin_name,
		pa.city,
		pa.country,
		dest,
		pa2.name AS dest_name,
		pa2.city,
		pa2.country,
		num_flights,
		num_unique_airplanes,
		num_unique_airlines,
		avg_actual_elapsed_time,
		avg_arr_delay,
		max_delay,
		min_delay,
		num_cancelled,
		num_diverted
FROM route r
JOIN {{ref('prep_airports')}} pa
ON r.origin = pa.faa
JOIN {{ref('prep_airports')}} pa2 
ON r.dest = pa2.faa