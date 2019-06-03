from .pollution_model import PollutionModel

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession

import argparse


def get_args():
    parser = argparse.ArgumentParser(description='Training pollution model.')
    parser.add_argument('-p', '--pollution', dest='pollution', required=True, choices=['PM10', 'PM2.5'], help='which model use for prediction ("PM10", "PM2.5")')
    parser.add_argument('-d', '--load_dir', dest='load_dir', help='directory with model')
    args = parser.parse_args()
    return args.pollution, args.load_dir


def predict(spark_session, pollution, load_dir):
    features = ['temp_max', 'temp_min', 'pressure', 'humidity', 'wind_speed', 'current_value']
    model = PollutionModel(spark_session, pollution, features=features)
    model.load(load_dir)
    model.predict_sql('./sqls/inference.sql')


if __name__ == '__main__':
    pollution, load_dir = get_args()
    SparkContext.setSystemProperty("hive.metastore.uris", "0.0.0.0:9083")
    spark_session = (SparkSession
                    .builder
                    .appName('example-pyspark-read-and-write-from-hive')
                    .enableHiveSupport()
                    .getOrCreate())
    predict(spark_session, pollution, load_dir)