SELECT
  sm.current_date,
  sm.future_date,
  sm.city,
  mt.temp_max,
  mt.temp_min,
  mt.pressure,
  mt.humidity,
  mt.wind_speed,
  sm.current_value,
  sm.future_value
FROM
  (
    SELECT
      LOWER(station) as city,
      dt as future_date,
      AVG(main_temp_max) as temp_max,
      AVG(main_temp_min) as temp_min,
      AVG(main_pressure) as pressure,
      AVG(main_humidity) as humidity,
      AVG(wind_speed) as wind_speed
    FROM
      meteo_final
    GROUP BY
      dt,
      station
  ) as mt
  JOIN (
    SELECT
      cs.date as current_date,
      fs.date as future_date,
      cs.city as city,
      current_value,
      future_value
    FROM
      (
        SELECT
          date,
          LOWER(miejscowosc) as city,
          AVG(wartosc) as current_value
        FROM
          (
            SELECT
              TO_DATE(
                from_unixtime(
                  UNIX_TIMESTAMP(dt, "yyyy-MM-dd HH:mm:ss"),
                  "yyyy-MM-dd"
                )
              ) as date,
              id,
              wartosc
            FROM
	      smog_history
            WHERE
              wartosc IS NOT NULL
	      AND pomiar = "{}"  /* PM10 lub PM25 */
          ) as a
          JOIN all_stations as s ON a.id = s.id
        GROUP BY
          date,
          miejscowosc
      ) as cs
      JOIN (
        SELECT
          date,
          LOWER(miejscowosc) as city,
          AVG(wartosc) as future_value
        FROM
          (
            SELECT
              TO_DATE(
                from_unixtime(
                  UNIX_TIMESTAMP(dt, "yyyy-MM-dd HH:mm:ss"),
                  "yyyy-MM-dd"
                )
              ) as date,
              id,
              wartosc
            FROM 
              smog_history 
            WHERE
              wartosc IS NOT NULL
	      AND pomiar = "{}"  /* PM10 lub PM25 */
          ) as a
          JOIN all_stations as s ON a.id = s.id
        GROUP BY
          date,
          miejscowosc
      ) as fs ON (
        cs.date = date_sub(fs.date, 1)
        AND cs.city = fs.city
      )
  ) as sm ON (
    sm.city == mt.city
    AND sm.future_date = mt.future_date
  )

