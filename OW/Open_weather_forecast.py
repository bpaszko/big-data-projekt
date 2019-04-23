import sys
import requests
import csv
import collections
from datetime import datetime


def get_weather(location):
    api_key = '82263ce8ab1dc5df22e5914cecb17102'
    url = "https://api.openweathermap.org/data/2.5/forecast?q={}&units=metric&appid={}".format(location, api_key)
    r = requests.get(url)
    test_data=r.json()
    # print(type(test_data))
    # test_data
    # test_data.items()
    u=test_data['list'][1]

    def flatten(d, parent_key='', sep='_'):
        items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, collections.MutableMapping):
                items.extend(flatten(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)
    a1=flatten(u)
    a=(flatten(test_data))
    del a['list']
    a.pop('cod', None)
    a.pop('message', None)
    a.pop('city_id', None)
    a.pop('city_coord_lat', None)
    a.pop('city_coord_lon', None)
    a.pop('city_country', None)
    a.pop('cnt', None)
    a.pop('city_population', None)
    a2={'update_date':datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
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
    with open('forecast.csv', 'w',newline='') as f:  # Just use 'w' mode in 3.x
        w = csv.DictWriter(f, a2.keys())
        w.writeheader()
        for r in test_data['list']:
            rr=flatten(r)
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
            at={'update_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            at.update(a)
            at['city_name']=location
            rr['dt']=datetime.fromtimestamp((rr['dt'])).strftime('%Y-%m-%d %H:%M:%S')
            at.update(rr)


            w.writerow(at)
        f.close()


def main():
    location = sys.argv[1]
    weather = get_weather(location)


if __name__ == '__main__':
    main()


