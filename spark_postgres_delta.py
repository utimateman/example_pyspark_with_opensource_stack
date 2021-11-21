from pyspark import SparkContext, SparkConf
import os
from pyspark.sql.session import SparkSession
import numpy as np
import pandas as pd
from delta import *

builder = SparkSession.builder.appName("MyApp") \
    .appName('Python Spark Postgresql') \
    .config("spark.jars", "./postgresql-42.2.5.jar") \
    .config('spark.driver.extraClassPath', './postgresql-42.2.5.jar') \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") 

spark = spark = configure_spark_with_delta_pip(builder).getOrCreate()



df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/mockup_sales_data_v1") \
    .option("dbtable", 'mock_data_v1') \
    .option("user", "postgres") \
    .option("password", "pass1234") \
    .option("driver", "org.postgresql.Driver") \
    .load()

output = df.select('*').toPandas()
print(output)

df = df.write.format("delta").save("C:/Users/kraifirstspark/spark-3.1.2-bin-hadoop2.7/research_and_development/delta/poc_delta_v3")
