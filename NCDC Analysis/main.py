from pyspark.sql import SparkSession
from ETL.Extract import *
from ETL.Transform import *

spark = SparkSession \
    .builder \
    .appName("Ep 2") \
    .master("local[*]") \
    .getOrCreate()

print("Script inicializado... \n")

df = readData(spark)
df = transformColumns(df)

# df.printSchema()
df.show(5)
# print("O dataframe agora tem ",df.count()," linhas e ", len(df.columns), "colunas. \n")
df = transformData(df)
df.show(5)
'''
print("Digite a coluna que deseja agrupar: ")
campo = input()
'''
# print(df.filter(df["TEMP_ATTRIBUTES"] == "24").show())
# print(df.groupBy(campo).count().orderBy("count", ascending=True).show())
