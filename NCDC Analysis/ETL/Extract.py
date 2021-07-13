'''
Arquivo responsável por Extração da base de dados
Criação do DataFrame que será utilizado com todos os dados consolidados
'''

# Read all csv files from a directory
def readData(spark):
    return spark.read.option("header", True).csv("assets/samples/import/")
