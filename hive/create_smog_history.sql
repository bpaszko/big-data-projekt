CREATE EXTERNAL TABLE IF NOT EXISTS smog_history_text(
  dt timestamp,
  id int,
  wartosc double,
  pomiar string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/smog/smog_history';

CREATE TABLE IF NOT EXISTS smog_history(
  dt timestamp,
  id int,
  wartosc double,
  pomiar string)
STORED AS ORC;

INSERT OVERWRITE TABLE smog_history SELECT * FROM smog_history_text;