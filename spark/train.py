from .pollution_model import PollutionModel

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession

import argparse
import os


def get_args():
    parser = argparse.ArgumentParser(description='Training pollution model.')
    parser.add_argument('-p', '--pollution', dest='pollution', default='all', choices=['PM10', 'PM2.5', 'all'], help='which models to train ("PM10", "PM2.5", "all")')
    parser.add_argument('-d', '--save_dir', dest='save_dir', help='Path where to save model')
    args = parser.parse_args()
    return args.pollution, args.save_dir


def train_model(spark_session, pollution, save_dir):
    if pollution == 'all':
        train_pm10(spark_session, save_dir)
        train_pm25(spark_session, save_dir)
    elif pollution == 'PM2.5':
        train_pm25(spark_session, save_dir)
    elif pollution == 'PM10':
        train_pm10(spark_session, save_dir)
    else:
        raise RuntimeError(f'Unknown pollution type: {pollution}')


def train_pm10(spark_session, save_dir):
    features = ['temp_max', 'temp_min', 'pressure', 'humidity', 'wind_speed', 'current_value']
    train_kwargs = {'maxIter':100, 'regParam':0.3, 'elasticNetParam':0.8}
    pm10_model = PollutionModel(spark_session, 'PM10', features=features)
    pm10_model.fit_sql('./sqls/train.sql', validate=False, **train_kwargs)
    pm10_model.save(os.path.join(save_dir, 'pm10'))


def train_pm25(spark_session, save_dir):
    features = ['temp_max', 'temp_min', 'pressure', 'humidity', 'wind_speed', 'current_value']
    train_kwargs = {}
    pm25_model = PollutionModel(spark_session, 'PM2.5', features=features)
    pm25_model.fit_sql('./sqls/train.sql', validate=False, **train_kwargs)
    pm25_model.save(os.path.join(save_dir, 'pm25'))


if __name__ == '__main__':
    pollution, save_dir = get_args()
    SparkContext.setSystemProperty("hive.metastore.uris", "0.0.0.0:9083")
    spark_session = (SparkSession
                    .builder
                    .appName('example-pyspark-read-and-write-from-hive')
                    .enableHiveSupport()
                    .getOrCreate())
    train_model(spark_session, pollution, save_dir)