DROP TABLE IF EXISTS weather_forecast;

CREATE EXTERNAL TABLE IF NOT EXISTS weather_forecast(
    update_date timestamp,
    station string,
    dt timestamp,
    main_temp_min double,
    main_temp_max double,
    main_pressure double,
    main_humidity double,
    wind_speed double)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/weather/weather_forecast'
TBLPROPERTIES ('skip.header.line.count'='1');