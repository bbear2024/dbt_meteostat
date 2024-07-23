WITH departure AS (
		SELECT  flight_date,
				origin AS airport,
				count(*) AS num_dep,
				sum(cancelled) AS c_dep,
				sum(diverted) AS d_dep,
				count(air_time) AS o_dep,
				count(DISTINCT tail_number) AS num_unique_airplanes_dep,
				count(DISTINCT airline) AS num_unique_airline_dep
		FROM {{ref('prep_flights')}}
		GROUP BY origin, flight_date
),
arrival AS (
		SELECT  flight_date,
				dest AS airport,
				count(*) AS num_arr,
				sum(cancelled) AS c_arr,
				sum(diverted) AS d_arr,
				count(air_time) AS o_arr,
				count(DISTINCT tail_number) AS num_unique_airplanes_arr,
				count(DISTINCT airline) AS num_unique_airline_arr
		FROM {{ref('prep_flights')}}
		GROUP BY dest, flight_date
),
t AS (
		SELECT  flight_date,
				airport,
				num_dep,
				num_arr,
				(num_dep + num_arr) AS num_flights_planned,
				(c_dep + c_arr) AS num_cancelled,
				(d_dep + d_arr) AS num_diverted,
				(o_dep + o_arr) AS num_occured,
				round((num_unique_airplanes_dep + num_unique_airline_arr) / 2, 2) AS avg_num_unique_airplanes,
				round((num_unique_airline_dep + num_unique_airline_arr) / 2, 2) AS avg_num_unique_airlines
		FROM departure
		JOIN arrival
		USING (airport, flight_date)
),
t2 AS (
		SELECT  flight_date,
				a.name AS airport,
		        t.airport AS code,
		        a.city,
		        a.country,
		        num_dep,
		        num_arr,
		        num_flights_planned,
		        num_cancelled,
		        num_diverted,
		        num_occured,
		        avg_num_unique_airplanes,
		        avg_num_unique_airlines
		FROM t
		JOIN {{ref('prep_airports')}} a
		ON t.airport = a.faa
)
SELECT  flight_date,
		airport,
		code,
		city,
		country,
		num_dep,
		num_arr,
		num_flights_planned,
		num_cancelled,
		num_diverted,
		num_occured,
		avg_num_unique_airplanes,
		avg_num_unique_airlines
FROM t2
JOIN {{ref('prep_weather_daily')}} pwd
ON t2.code = pwd.airport_code AND t2.flight_date = pwd.date
ORDER BY flight_date