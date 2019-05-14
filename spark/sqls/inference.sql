SELECT
  weather.dt,
  weather.time,
  weather.city,
  pollution.current_value,
  weather.temp_max,
  weather.temp_min,
  weather.pressure,
  weather.humidity,
  weather.wind_speed
FROM
  (
    SELECT
      dt,
      time,
      station as city,
      AVG(main_temp_max) as temp_max,
      AVG(main_temp_min) as temp_min,
      AVG(main_pressure) as pressure,
      AVG(main_humidity) as humidity,
      AVG(wind_speed) as wind_speed
    FROM
      meteo_predykcje
    WHERE
      from_unixtime(
        UNIX_TIMESTAMP(dt, "yyyy-MM-dd HH:mm:ss"),
        "yyyy-MM-dd"
      ) == add_date({ }, 1)
    GROUP BY
      station
  ) as weather
  JOIN (
    SELECT
      city,
      AVG(wartosc) as wartosc
    FROM
      PM10_history AS pm
      JOIN (
        SELECT
          b.miejscowosc AS city,
          MAX(a.time) AS recent_time
        FROM
          PM10_history AS a
          JOIN all_stations AS b ON a.id = b.id
        WHERE
          TO_DATE(
            from_unixtime(
              UNIX_TIMESTAMP(dt, "yyyy-MM-dd HH:mm:ss"),
              "yyyy-MM-dd"
            )
          ) = { }
        GROUP BY
          b.miejscowosc
      ) AS last_times ON pm.city = last_times.city
    WHERE
      pm.dt = { }
      AND pm.time = last_time.recent_time
    GROUP BY
      city
  ) as polluttion ON weather.city = pollution.city

