from pyspark.sql import SparkSession

def create_spark_session(app_name: str, spark_conf: dict = None) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)

    if spark_conf:
        for key, value in spark_conf.items():
            builder = builder.config(key, value)

    spark = builder.enableHiveSupport().getOrCreate()
    return spark