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
    print(type(test_data))
    test_data
    test_data.items()
    keys=[]
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
    a['sys_sunrise']=datetime.fromtimestamp((a['sys_sunrise'])).strftime('%Y-%m-%d %H:%M')
    a['sys_sunset']=datetime.fromtimestamp((a['sys_sunset'])).strftime('%Y-%m-%d %H:%M')
    with open('mycsvfile.csv', 'w') as f:  # Just use 'w' mode in 3.x
        w = csv.DictWriter(f, a.keys())
        w.writeheader()
        w.writerow(a)


        test_data.close()
    return w.writerow(a)


def main():
    location = sys.argv[1]
    weather = get_weather(location)

    print(weather['main']['temp'])
    print(weather)


if __name__ == '__main__':
    main()