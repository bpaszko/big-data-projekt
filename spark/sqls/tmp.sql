SELECT a.dt, a.station as city, AVG(main_temp_max) as temp_max, AVG(main_temp_min) as temp_min, AVG(main_pressure) as pressure, AVG(main_humidity) as humidity, AVG(wind_speed) as wind_speed FROM 
        weather_forecast_text AS a
        JOIN
        (SELECT station AS city, MAX(update_date) AS recent_time FROM weather_forecast_text GROUP BY station) AS b
        ON a.station = b.city
        WHERE a.station = b.city AND  a.dt > "{}" GROUP BY a.dt, a.station
