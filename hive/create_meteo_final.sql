CREATE EXTERNAL TABLE IF NOT EXISTS meteo_final_text(
    dt timestamp ,
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

CREATE TABLE IF NOT EXISTS meteo_final(
    dt timestamp ,
    station string,
    main_temp_max double,
    main_temp_min double,
    main_pressure double,
    main_humidity double,
    wind_speed double)
STORED AS ORC;

INSERT OVERWRITE TABLE meteo_final SELECT * FROM meteo_final_text;
