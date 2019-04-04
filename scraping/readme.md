Scraper to get historical meteo data.
To use it you have to install:

* scrapy
* selenium
* Firefox
* Mozilla GeckoDriver

```shell
scrapy startproject meteo
cp meteo_spider.py meteo/meteo/spiders/
cp settings.py meteo/meteo/
cd meteo/
scrapy crawl meteo -o meteo.csv
```

[Scaping result file](https://drive.google.com/open?id=1m0yoBj_UgnXJn5RuYjXRK1sYk8hzNxdg)
