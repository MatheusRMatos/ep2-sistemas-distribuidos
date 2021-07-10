from pyspark.sql import SparkSession
from pyspark import SparkFiles
import xml.etree.ElementTree as ET

'''

Arquivo responsável por Extração da base de dados
Criação do DataFrame que será utilizado com todos os dados consolidados

'''

print("Script inicializado... \n")

spark = SparkSession \
    .builder \
    .appName("Ep 2") \
    .master("local[*]") \
    .getOrCreate()

'''
Extração Gz files
tf = tarfile.open("samples.tar.gz")
tf.extractall()
'''


file_rdd = spark.read.text("./assets/data.xml", wholetext=True).rdd
print(file_rdd.take(1))
print("CONTEUDO DA PRIMEIRA LINHA")

root = ET.fromstring(file_rdd.take(1)[0][0])

for i,child in enumerate(root[0]):
    print("------------------------")
    print("RECORD", i)
    print("          ")
    for value in child:
        if len(value) > 0:
            for val in value:
                print(val.tag,"  :",value.find(val.tag).text)
        else:
            print(value.tag, "   :",child.find(value.tag).text)



