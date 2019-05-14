SELECT
  weather.dt,
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
      a.dt,
      LOWER(a.station) as city,
      AVG(main_temp_max) as temp_max,
      AVG(main_temp_min) as temp_min,
      AVG(main_pressure) as pressure,
      AVG(main_humidity) as humidity,
      AVG(wind_speed) as wind_speed
    FROM
      weather_forecast_text AS a
      JOIN (
        SELECT
          station AS city,
          MAX(update_date) AS recent_time
        FROM
          weather_forecast_text
        GROUP BY
          station
      ) AS b ON a.station = b.city
    WHERE
      a.station = b.city
      AND a.dt > "{}"
    GROUP BY
      a.dt,
      a.station
  ) as weather
  JOIN (
    SELECT
      LOWER(pm.city) as city,
      AVG(wartosc) as current_value
    FROM
      (
        SELECT
          x.dt,
          x.wartosc,
          y.miejscowosc as city
        FROM
          smog_history AS x
          JOIN all_stations AS y ON x.id = y.id
        WHERE
          x.pomiar = "{}"
      ) AS pm
      JOIN (
        SELECT
          b.miejscowosc AS city,
          MAX(a.dt) AS recent_time
        FROM
          smog_history AS a
          JOIN all_stations AS b ON a.id = b.id
        WHERE
          pomiar = "{}"
        GROUP BY
          b.miejscowosc
      ) AS last_times ON pm.city = last_times.city
    WHERE
      pm.dt = last_times.recent_time
    GROUP BY
      pm.city
  ) as pollution ON weather.city = pollution.city

