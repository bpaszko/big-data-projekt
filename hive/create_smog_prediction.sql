CREATE TABLE IF NOT EXISTS smog_prediction(
  dt timestamp,
  id int,
  wartosc double,
  pomiar string,
  update_date timestamp)
STORED AS ORC;