CREATE EXTERNAL TABLE IF NOT EXISTS all_stations_text(
  wojewodztwo string,
  kod_stary string,
  kod_nowy string,
  nazwa_stacji string,
  miejscowosc string,
  id int,
  dostepnosc boolean)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/smog/all_stations'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE TABLE IF NOT EXISTS all_stations(
  wojewodztwo string,
  kod_stary string,
  kod_nowy string,
  nazwa_stacji string,
  miejscowosc string,
  id int,
  dostepnosc boolean)
STORED AS ORC;

INSERT OVERWRITE TABLE all_stations SELECT * FROM all_stations_text;
