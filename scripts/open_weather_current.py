import requests
import csv
from datetime import datetime
from settings import *
import os


def get_weather():
    cities_rows = []
    for city_ascii, city_utf in cities_dict.items():
        print("{} requested".format(city_ascii))
        api_key = '82263ce8ab1dc5df22e5914cecb17102'
        url = "https://api.openweathermap.org/data/2.5/weather?q={}&units=metric&appid={}".format(city_ascii, api_key)
        r = requests.get(url)
        test_data = r.json()
        a = (flatten(test_data))
        del a['weather']

        a['dt']=datetime.fromtimestamp((a['dt'])).strftime('%Y-%m-%d')
        a['name'] = city_utf
        a2 = {
            'dt': a.get('dt'),
            'city_name': a.get('name'),
            'main_temp_max': a.get('main_temp_max'),
            'main_temp_min': a.get('main_temp_min'),
            'main_pressure': a.get('main_pressure'),
            'main_humidity': a.get('main_humidity'),
            'wind_speed': a.get('wind_speed')
        }
        cities_rows.append(a2)

    time_stamp = datetime.now().strftime('%Y%m%d%H')
    file = "{}_{}.csv".format(weather_current_filename, time_stamp)
    with open(file, 'w', newline='', encoding=encoding_utf) as f:
        w = csv.DictWriter(f, weather_current_columns)
        w.writeheader()
        for row in cities_rows:
            w.writerow(row)

    return file


if __name__ == '__main__':
    save_path = get_weather()
    hdfs_command = "hdfs dfs -put {} /data/weather/meteo_final".format(save_path)
    os.system(hdfs_command)
    rm_command = "rm {}".format(save_path)
    os.system(rm_command)
