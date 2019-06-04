#!/bin/sh

hive -f /big-data-projekt/hive/create_smog_prediction.sql
python3 /big-data-projekt/spark/predict.py -p PM2.5 -d /big-data-projekt/spark/models/pm25/
python3 /big-data-projekt/spark/predict.py -p PM10 -d /big-data-projekt/spark/models/pm10/
