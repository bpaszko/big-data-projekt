CREATE EXTERNAL TABLE IF NOT EXISTS pm25_history_text(
  dt timestamp,
  id int,
  wartosc double)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/smog/pm25_history';

CREATE TABLE IF NOT EXISTS pm25_history(
  dt timestamp,
  id int,
  wartosc double)
STORED AS ORC;

INSERT OVERWRITE TABLE pm25_history SELECT * FROM pm25_history_text;