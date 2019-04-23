import json
import pandas as pd
import requests
import sys
import csv
import collections
from datetime import datetime

def get_weather(location):
    api_key = '82263ce8ab1dc5df22e5914cecb17102'
    #url = "https://api.openweathermap.org/data/2.5/weather?q={}&units=metric&appid={}".format(location, api_key)
    url = "https://api.openweathermap.org/data/2.5/weather?q={}&units=metric&appid={}".format('warsaw', api_key)
    r = requests.get(url)
    test_data=r.json()
    print(type(test_data['coord']))

    def flatten(d, parent_key='', sep='_'):
        items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, collections.MutableMapping):
                items.extend(flatten(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    a=(flatten(test_data))
    del a['weather']

    a['dt']=datetime.fromtimestamp((a['dt'])).strftime('%Y-%m-%d %H:%M')
    a['name'] = 'Warszawa'
    #a2={a['name'],a['dt'],a['main_temp_min'],a['main_temp_max'],a['main_pressure'],a['main_humidity'],a['wind_speed']}
    a2= {'city_name':a.get('name'),'dt':a.get('dt'),'main_temp_min':a.get('main_temp_min'),'main_temp_max':a.get('main_temp_max'),'main_pressure':a.get('main_pressure'),'main_humidity':a.get('main_humidity'),'wind_speed':a.get('wind_speed')}
    a2
    with open('odczyt.csv', 'w', newline='') as f:  # Just use 'w' mode in 3.x
        w = csv.DictWriter(f, a2.keys())
        w.writeheader()

        w.writerow(a2)
    f.close()

def main():
    location = "Warsaw"
    get_weather(location)

if __name__ == '__main__':
    main()