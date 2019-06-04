import requests
import csv
from datetime import datetime
from settings import *
import os


def get_weather():
    cities_rows = {}
    for city_ascii, city_utf in cities_dict.items():
        print("{} requested".format(city_ascii))
        api_key = '82263ce8ab1dc5df22e5914cecb17102'
        url = "https://api.openweathermap.org/data/2.5/forecast?q={}&units=metric&appid={}".format(city_ascii, api_key)
        r = requests.get(url)
        test_data = r.json()
        u = test_data['list'][1]

        a1 = flatten(u)
        a = flatten(test_data)
        del a['list']
        a.pop('cod', None)
        a.pop('message', None)
        a.pop('city_id', None)
        a.pop('city_coord_lat', None)
        a.pop('city_coord_lon', None)
        a.pop('city_country', None)
        a.pop('cnt', None)
        a.pop('city_population', None)
        a.pop('city_timezone', None)
        a2 = {'update_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        a2.update(a)
        a2.update(a1)
        a2.pop('main_temp', None)
        a2.pop('main_sea_level', None)
        a2.pop('main_grnd_level', None)
        a2.pop('main_temp_kf', None)
        a2.pop('weather', None)
        a2.pop('wind_deg', None)
        a2.pop('sys_pod', None)
        a2.pop('clouds_all', None)
        a2.pop('dt_txt', None)
        cities_rows[city_utf] = test_data['list']

    file = "{}.csv".format(weather_forecast_filename)
    with open(file, 'w', newline='', encoding=encoding_utf) as f:
        w = csv.DictWriter(f, weather_forecast_columns)
        w.writeheader()
        for city, row in cities_rows.items():
            print("{} saved".format(city))
            for r in row:
                rr = flatten(r)
                rr.pop('rain_3h', None)
                rr.pop('dt_txt', None)
                rr.pop('snow_3h', None)
                rr.pop('main_temp', None)
                rr.pop('main_sea_level', None)
                rr.pop('main_grnd_level', None)
                rr.pop('main_temp_kf', None)
                rr.pop('weather', None)
                rr.pop('wind_deg', None)
                rr.pop('sys_pod', None)
                rr.pop('clouds_all', None)
                at = {'update_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                at.update(a)
                at['city_name'] = city
                rr['dt'] = datetime.fromtimestamp((rr['dt'])).strftime('%Y-%m-%d %H:%M:%S')
                at.update(rr)
                w.writerow(at)
    return file


if __name__ == '__main__':
    save_path = get_weather()
    hdfs_command = "hdfs dfs -put -f {} /data/weather/weather_forecast".format(save_path)
    os.system(hdfs_command)
    rm_command = "rm {}".format(save_path)
    os.system(rm_command)
