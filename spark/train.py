from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import Imputer


def fill_with_mean(df, exclude=set()): 
    stats = df.agg(*(
        avg(c).alias(c) for c in df.columns if c not in exclude
    ))
    return df.na.fill(stats.first().asDict())


def test_model(train_df, test_df):
    lr = LinearRegression(featuresCol = 'features', labelCol='future_value', maxIter=100, regParam=0.3, elasticNetParam=0.8)
    lr_model = lr.fit(train_df)

    trainingSummary = lr_model.summary
    print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
    print("r2: %f" % trainingSummary.r2)

    lr_predictions = lr_model.transform(test_df)
    lr_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="future_value",metricName="r2")
    print("R Squared (R2) on test data = %g" % lr_evaluator.evaluate(lr_predictions))

    test_result = lr_model.evaluate(test_df)
    print("Root Mean Squared Error (RMSE) on test data = %g" % test_result.rootMeanSquaredError)


def train_test_model(sql_query, sparkSession):
    df = sparkSession.sql(sql_query)
    pre_input_cols = ['temp_max', 'temp_min', 'pressure', 'humidity', 'wind_speed', 'current_value']
    post_input_cols = ['{}_i'.format(c) for c in pre_input_cols]
    imputer = Imputer(inputCols=pre_input_cols, outputCols=post_input_cols)
    df = imputer.fit(df).transform(df)

    vectorAssembler = VectorAssembler(inputCols=post_input_cols, outputCol='features')
    t_df = vectorAssembler.transform(df)
    t_df = t_df.select(['features', 'future_value'])
    splits = t_df.randomSplit([0.7, 0.3])
    train_df = splits[0]
    test_df = splits[1]
    test_model(train_df, test_df)


if __name__ == '__main__':
    SparkContext.setSystemProperty("hive.metastore.uris", "0.0.0.0:9083")
    sparkSession = (SparkSession
                    .builder
                    .appName('example-pyspark-read-and-write-from-hive')
                    .enableHiveSupport()
                    .getOrCreate())

    sql_query_10 = \
        'SELECT sm.current_date, sm.future_date, sm.city, mt.temp_max, mt.temp_min, mt.pressure, mt.humidity, mt.wind_speed, sm.current_value, sm.future_value FROM \
            (SELECT LOWER(station) as city, dt as future_date, AVG(main_temp_max) as temp_max, AVG(main_temp_min) as temp_min, AVG(main_pressure) as pressure, AVG(main_humidity) as humidity, AVG(wind_speed) as wind_speed \
                 FROM meteo_final GROUP BY dt, station) as mt \
            JOIN \
            (SELECT cs.date as current_date, fs.date as future_date, cs.city as city, current_value, future_value FROM \
                (SELECT date, LOWER(miejscowosc) as city, AVG(wartosc) as current_value FROM \
                    (SELECT TO_DATE(from_unixtime(UNIX_TIMESTAMP(dt, "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd")) as date, id, wartosc FROM PM10_history WHERE wartosc IS NOT NULL) as a \
                    JOIN all_stations as s ON a.id = s.id \
                GROUP BY date, miejscowosc) as cs \
                JOIN \
                (SELECT date, LOWER(miejscowosc) as city, AVG(wartosc) as future_value FROM \
                    (SELECT TO_DATE(from_unixtime(UNIX_TIMESTAMP(dt, "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd")) as date, id, wartosc FROM PM10_history WHERE wartosc IS NOT NULL) as a \
                    JOIN all_stations as s ON a.id = s.id \
                GROUP BY date, miejscowosc) as fs \
                ON (cs.date = date_sub(fs.date, 1) AND cs.city = fs.city)) as sm \
            ON (sm.city == mt.city AND sm.future_date = mt.future_date)'

    sql_query_25 = \
        'SELECT sm.current_date, sm.future_date, sm.city, mt.temp_max, mt.temp_min, mt.pressure, mt.humidity, mt.wind_speed, sm.current_value, sm.future_value FROM \
            (SELECT LOWER(station) as city, dt as future_date, AVG(main_temp_max) as temp_max, AVG(main_temp_min) as temp_min, AVG(main_pressure) as pressure, AVG(main_humidity) as humidity, AVG(wind_speed) as wind_speed \
                 FROM meteo_final GROUP BY dt, station) as mt \
            JOIN \
            (SELECT cs.date as current_date, fs.date as future_date, cs.city as city, current_value, future_value FROM \
                (SELECT date, LOWER(miejscowosc) as city, AVG(wartosc) as current_value FROM \
                    (SELECT TO_DATE(from_unixtime(UNIX_TIMESTAMP(dt, "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd")) as date, id, wartosc FROM PM25_history WHERE wartosc IS NOT NULL) as a \
                    JOIN all_stations as s ON a.id = s.id \
                GROUP BY date, miejscowosc) as cs \
                JOIN \
                (SELECT date, LOWER(miejscowosc) as city, AVG(wartosc) as future_value FROM \
                    (SELECT TO_DATE(from_unixtime(UNIX_TIMESTAMP(dt, "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd")) as date, id, wartosc FROM PM25_history WHERE wartosc IS NOT NULL) as a \
                    JOIN all_stations as s ON a.id = s.id \
                GROUP BY date, miejscowosc) as fs \
                ON (cs.date = date_sub(fs.date, 1) AND cs.city = fs.city)) as sm \
            ON (sm.city == mt.city AND sm.future_date = mt.future_date)'


    train_test_model(sql_query_10, sparkSession)
    # train_test_model(sql_query_25, sparkSession)

    
