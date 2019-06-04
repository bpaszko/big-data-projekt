DROP TABLE IF EXISTS smog_history;

CREATE EXTERNAL TABLE IF NOT EXISTS smog_history(
  dt timestamp,
  id int,
  wartosc double,
  pomiar string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/smog/smog_history';