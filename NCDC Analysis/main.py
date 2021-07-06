from pyspark.sql import SparkSession
from pyspark import SparkFiles

spark = SparkSession \
    .builder \
    .appName("Ep 2") \
    .master("local[*]") \
    .getOrCreate()

print("Script inicializado... \n")

'''
    df = spark.read.csv("assets/sample.csv", header=True)
    df.printSchema()
    df.show()
'''

'''
spark.sparkContext.addFile("https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/1929/99006199999.csv")
df_data_path = SparkFiles.get("99006199999.csv")
df = spark.read.csv(df_data_path)
print("O dataframe tem ",df.count()," linhas e ", len(df.columns), "colunas. \n")
'''

i = 1929
for i in range(1929,1931): #92
    for n in range(33000,34000): #99999999999
        try:
            string_path = 'https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/' + str(i) + '/' + str(n) + '99999.csv'
            spark.sparkContext.addFile(string_path)
            spark_data_path = str(n) + '99999.csv'
            df = df.union(spark.read.csv(SparkFiles.get(spark_data_path)))
            print("Dataframe incorporado.")
        except:
            print("Tentativa ", n, " de 999.999")
'''
string_path = "https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/",i,"/",n,".csv"
spark.sparkContext.addFile(string_path)
spark_data_path = n,".csv"
df = df.union(spark.read.csv(SparkFiles.get(spark_data_path)))


spark.sparkContext.addFile("https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/1929/03980099999.csv")
df_data_path = SparkFiles.get("03980099999.csv")
df2 = spark.read.csv(df_data_path)
result = df.union(df2)
'''

print("O dataframe agora tem ",df.count()," linhas e ", len(df.columns), "colunas. \n")
