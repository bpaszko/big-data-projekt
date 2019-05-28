CREATE EXTERNAL TABLE IF NOT EXISTS weather_forecast_text(
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

CREATE TABLE IF NOT EXISTS weather_forecast(
    update_date timestamp,
    station string,
    dt timestamp,
    main_temp_min double,
    main_temp_max double,
    main_pressure double,
    main_humidity double,
    wind_speed double)
STORED AS ORC;

INSERT OVERWRITE TABLE weather_forecast SELECT * FROM weather_forecast_text;