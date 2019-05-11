CREATE EXTERNAL TABLE IF NOT EXISTS pm10_history_text(
  dt timestamp,
  id int,
  wartosc double)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/smog/pm10_history';

CREATE TABLE IF NOT EXISTS pm10_history(
  dt timestamp,
  id int,
  wartosc double)
STORED AS ORC;

INSERT OVERWRITE TABLE pm10_history SELECT * FROM pm10_history_text;