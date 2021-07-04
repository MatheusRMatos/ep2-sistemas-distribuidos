from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Ep 2") \
    .getOrCreate()

print("Script inicializado... \n")

df = spark.read.csv("assets/sample.csv", header=True)
df.printSchema()
df.show()