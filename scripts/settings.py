import collections
import os

weather_current_filename = "weather_current"
weather_forecast_filename = "weather_forecast"

# local
# files_path = os.path.join("..", "data", "weather")

# cluster
files_path = "/big-data-projekt/data/weather"

weather_current_filename = os.path.join(files_path, "meteo_final", weather_current_filename)
weather_forecast_filename = os.path.join(files_path, "weather_forecast", weather_forecast_filename)

encoding_utf = "utf-8"

cities_dict = {
    'Bielsko-Biala': 'BIELSKO-BIAŁA',
    'Zakopane': 'ZAKOPANE',
    'Krosno': 'KROSNO',
    'Jelenia Gora': 'JELENIA GÓRA',
    'Klodzko': 'KŁODZKO',
    'Opole': 'OPOLE',
    'Czestochowa': 'CZĘSTOCHOWA',
    'Katowice': 'KATOWICE',
    'Tarnow': 'TARNÓW',
    'Zamosc': 'ZAMOŚĆ',
    'Zielona Gora': 'ZIELONA GÓRA',
    'Legnica': 'LEGNICA',
    'Leszno': 'LESZNO',
    'Wroclaw': 'WROCŁAW',
    'Kalisz': 'KALISZ',
    'Wielun': 'WIELUŃ',
    'Lodz': 'ŁÓDŹ',
    'Poznan': 'POZNAŃ',
    'Plock': 'PŁOCK',
    'Warszawa': 'WARSZAWA',
    'Siedlce': 'SIEDLCE',
    'Szczecin': 'SZCZECIN',
    'Pila': 'PIŁA',
    'Torun': 'TORUŃ',
    'Mlawa': 'MŁAWA',
    'Olsztyn': 'OLSZTYN',
    'Bialystok': 'BIAŁYSTOK',
    'Koszalin': 'KOSZALIN',
    'Leba': 'ŁEBA',
    'Lebork': 'LĘBORK',
    'Elblag': 'ELBLĄG',
    'Suwalki': 'SUWAŁKI',
    'Nowy Sacz': 'NOWY SĄCZ',
    'Zgorzelec': 'ZGORZELEC'
}

weather_current_columns = [
    'dt',
    'city_name',
    'main_temp_max',
    'main_temp_min',
    'main_pressure',
    'main_humidity',
    'wind_speed'
]

weather_forecast_columns = [
    'update_date',
    'city_name',
    'dt',
    'main_temp_min',
    'main_temp_max',
    'main_pressure',
    'main_humidity',
    'wind_speed'
]


def flatten(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)
