from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.ml.regression import LinearRegression, LinearRegressionModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler, Imputer, ImputerModel
import pyspark.sql.functions as sf

import os
import datetime


class PollutionModel:
    available_measurements = ['PM10', 'PM2.5']

    def __init__(self, spark_session, pm, features, label='future_value'):
        assert pm in self.available_measurements
        self.session = spark_session
        self.pm = pm
        self.features = features
        self.label = label
        self.model = None
        self.imputer = None
        self.assembler = None

    def load(self, load_dir):
        if os.path.isdir(load_dir):
            self.model = LinearRegressionModel.load(os.path.join(load_dir, 'model'))
            self.imputer= ImputerModel.load(os.path.join(load_dir, 'imputer'))
            self.assembler = VectorAssembler.load(os.path.join(load_dir, 'assembler'))
        else:
            raise RuntimeError('Save path: {}, does not exist or is not a directory'.format(load_dir)) 

    def save(self, save_dir):
        assert not self.model is None
        if os.path.isdir(save_dir):
            self.model.write().overwrite().save(os.path.join(save_dir, 'model'))
            self.imputer.write().overwrite().save(os.path.join(save_dir, 'imputer'))
            self.assembler.write().overwrite().save(os.path.join(save_dir, 'assembler'))
        else:
            raise RuntimeError('Save path: {}, does not exist or is not a directory'.format(save_dir)) 

    def fit_sql(self, sql_path='./sqls/train.sql', validate=False, **kwargs):
        query = read_sql(sql_path, *[self.pm]*2)
        df = self.session.sql(query)
        self.fit_data(df)
        df = self.transform_data(df)
        train_df = df.select(['features', self.label])
        test_df = None
        if validate:
            train_df, test_df = train_test_spark_split(df, test_size=0.3)
        lr = LinearRegression(featuresCol = 'features', labelCol=self.label, **kwargs)
        self.model = lr.fit(train_df)
        self.model_summary(test_df)

    def predict_sql(self, sql_path='./sqls/inference.sql'):
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
        query = read_sql(sql_path, *[current_time, self.pm, self.pm])
        df = self.session.sql(query)
        df = self.transform_data(df)
        df = df.select(['dt', 'city', 'features'])
        predictions = self.model.transform(df)
        self.store(predictions)
    
    def store(self, df):
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
        df = df.withColumn('pomiar', sf.lit(self.pm))
        df = df.withColumn('update_date', sf.lit(current_time))
        df = df.selectExpr('dt', 'city as miejscowosc', 'prediction as wartosc', 'pomiar', 'update_date')
        # df.show(10)
        df.registerTempTable("temptable") 
        self.session.sql("INSERT INTO TABLE smog_prediction SELECT * FROM temptable")

    def model_summary(self, test_df=None):
        assert self.model is not None
        trainingSummary = self.model.summary
        print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
        print("r2: %f" % trainingSummary.r2)

        if not test_df is None:
            predictions = self.model.transform(test_df)
            evaluator = RegressionEvaluator(predictionCol="prediction", labelCol=self.label, metricName="r2")
            print("R Squared (R2) on test data = %g" % evaluator.evaluate(predictions))

            test_result = self.model.evaluate(test_df)
            print("Root Mean Squared Error (RMSE) on test data = %g" % test_result.rootMeanSquaredError)

    def transform_data(self, df):
        assert not (self.imputer is None or self.assembler is None)
        df = self.imputer.transform(df)
        df = self.assembler.transform(df)
        return df

    def fit_data(self, df):
        imputed_features = ['{}_i'.format(c) for c in self.features]
        self.imputer = Imputer(inputCols=self.features, outputCols=imputed_features).fit(df)
        self.assembler = VectorAssembler(inputCols=imputed_features, outputCol='features')


def read_sql(sql_path, *args):
    with open(sql_path) as f:
        sql_query = f.read()
    return sql_query.format(*args)


def train_test_spark_split(df, test_size):
    assert 0 < test_size < 1
    splits = df.randomSplit([1-test_size, test_size])
    train_df = splits[0]
    test_df = splits[1]
    return train_df, test_df


if __name__ == '__main__':
    SparkContext.setSystemProperty("hive.metastore.uris", "0.0.0.0:9083")
    sparkSession = (SparkSession
                    .builder
                    .appName('example-pyspark-read-and-write-from-hive')
                    .enableHiveSupport()
                    .getOrCreate())

    features = ['temp_max', 'temp_min', 'pressure', 'humidity', 'wind_speed', 'current_value']
    train_kwargs = {'maxIter':100, 'regParam':0.3, 'elasticNetParam':0.8}
    
    # pm10_model = PollutionModel(sparkSession, 'PM10', features=features)
    # pm10_model.load('./models/pm10')
    # pm10_model.predict_sql('./sqls/inference.sql')
    
    # pm10_model.fit_sql('./sqls/train.sql', validate=False, **train_kwargs)
    #  pm10_model.save('./models/pm10')
    
    
    pm25_model = PollutionModel(sparkSession, 'PM2.5', features=features)
    pm25_model.fit_sql('./sqls/train.sql', validate=True, **train_kwargs)
    pm25_model.save('./models/pm25')
