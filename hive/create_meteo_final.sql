DROP TABLE IF EXISTS meteo_final;

CREATE EXTERNAL TABLE IF NOT EXISTS meteo_final(
    dt date ,
    station string,
    main_temp_max double,
    main_temp_min double,
    main_pressure double,
    main_humidity double,
    wind_speed double)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/weather/meteo_final'
TBLPROPERTIES ('skip.header.line.count'='1');