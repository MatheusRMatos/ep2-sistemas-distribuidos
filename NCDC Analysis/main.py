import findspark
findspark.init() 

from pyspark.sql import SparkSession
from ETL.Extract import *
from ETL.Transform import *


spark = SparkSession \
    .builder \
    .appName("Ep 2") \
    .getOrCreate()

print("Script inicializado... \n")

df = readSampleDate(spark)
df = transformColumns(df)

df = transformData(df)

def mean(df, column_name, start_date, end_date):
    return df.filter( (col("DATE") >= start_date) & \
               (col("DATE") <= end_date)) \
       .select(_mean(column_name).alias(f'{column_name}_MEAN'))