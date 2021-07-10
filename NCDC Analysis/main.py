from pyspark.sql import SparkSession
from pyspark import SparkFiles
from pyspark.context import SparkContext

spark = SparkSession \
    .builder \
    .appName("Ep 2") \
    .master("local[*]") \
    .getOrCreate()

print("Script inicializado... \n")

# Read all csv files from a directory
df = spark.read.option("header",True).csv("assets/samples/import/")
df.show()

print("O dataframe agora tem ",df.count()," linhas e ", len(df.columns), "colunas. \n")

# Export dataframe to csv file
# Para escrever em um arquivo: coalesce(1), (não tem paralelismo)
df.write.csv("assets/samples/export/data.csv")