CREATE TABLE meteo_final(
  data timestamp,
  stacja string,
  sr_temp_powietrza double,
  maks_temp_powietrza double,
  min_temp_powietrza double,
  min_temp_powietrza_grunt double,
  sr_wiglgotnosc_powietrza double,
  sr_preznosc_pary_wodnej double,
  sr_zachmurzenie double,
  sr_predkosc_wiatru double,
  czas_trwania_wiatru_pow_10 double,
  czas_trwania_wiatru_pow_15 double,
  cisnienie_na_poziomie_morza double,
  cisnienie_na_poziomie_stacji double,
  dobowa_suma_opadów double,
  opad_dzien double,
  opad_noc double,
  wysokosc_pokrywy_snieznej double,
  rownowaznik_wodny_sniegu double,
  uslonecznienie double,
  czas_trwania_opadu_deszczu double,
  czas_trwania_opadu_sniegu double,
  czas_trwania_opadu_deszczu_ze_sniegiem double,
  czas_trwania_mgly double,
  czas_trwania_rosy double,
  czas_trwania_szronu double,
  czas_trwania_burzy double)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/weather/meteo_final'
TBLPROPERTIES ('skip.header.line.count'='1');