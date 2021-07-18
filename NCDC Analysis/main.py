import findspark
findspark.init() 

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from ETL.Extract import *
from ETL.Transform import *
from Statistic.Descritive import *


spark = SparkSession \
    .builder \
    .appName("Ep 2") \
    .getOrCreate()

print("Script inicializado... \n")

df = readSampleDate(spark)
df = transformColumns(df)

df = transformData(df)
# df = df.withColumn("TEMP", when(df.TEMP == 17.89, 231546).otherwise(df.TEMP))
df.show(5)
'''
print("Digite a coluna que deseja agrupar: ")
campo = input()
'''
# print(df.filter(df["SLP"] >= 9999.9).show())
# print(df.groupBy(campo).count().orderBy("count", ascending=True).show())
'''
df_stats = df.select(
    F.mean(F.col('TEMP')).alias('mean'),
    F.stddev(F.col('TEMP')).alias('std')
).collect()

mean = df_stats[0]['mean']
std = df_stats[0]['std']
'''

print("A média é: ", media(df,'TEMP'))
print("O desvio é: ", desvioPadrao(df, 'TEMP'))
print("A mediana é: ", mediana(df, 'TEMP'))
