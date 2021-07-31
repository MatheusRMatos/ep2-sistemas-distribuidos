import findspark
findspark.init() 

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from ETL.Extract import *
from ETL.Transform import *
from Statistic.Statistic import *
from Statistic.Descritive import *
import os
from datetime import datetime
from matplotlib import pyplot as plt

spark = SparkSession \
    .builder \
    .appName("Ep 2") \
    .getOrCreate()

def print_menu():
    print("Selecione um item do menu: \n")
    print("0. Reler base de dados")
    print("1. Calcular Média")
    print("2. Calcular Mediana")
    print("3. Calcular Desvio Padrão")
    print("4. Calcular Variância")
    print("5. Gerar gráficos")
    print("6. Aplicar Método dos Quadrados Mínimos para predição")
    print("7. Exibir tabela")
    print("9. Sair")
    return int(input())

def print_graph_type_menu():
    print("Selecione o tipo de gráfico: \n")
    print("1. Gráfico de Barras")
    print("2. Gráfico de Linha")
    print("3. Gráfico de Dispersão")
    return int(input())


def print_graph_group_menu():
    print("Selecione o tipo de agrupamento: \n")
    print("1. Agrupar por mes")
    print("2. Agrupar por ano")
    return int(input())

def handle_graph_generator():
    global data_frame

    graph_type = print_graph_type_menu()
    fig, ax = plt.subplots()

    if graph_type == 1:
        #Gráfico de linha
        column_name = input("Informe a coluna que deseja analisar: ")
        
        graph_group_type = print_graph_group_menu()
        
        if graph_group_type == 1:
            #Agrupar por mês
            graph_result = df.groupBy(F.month(col("Date")).alias("Date") ).agg(_mean(column_name).alias(column_name)).orderBy("Date").toPandas()

            ax.plot(graph_result["Date"], graph_result[column_name])
            stringDate = datetime.today().strftime("%d-%m-%Y-%H-%M-%S")
            fig.imsave(f"{stringDate}_line_month_mean.png")
            pass
        elif graph_group_type == 2:
            #Agrupar por ano
            graph_result = df.groupBy(F.year(col("Date")).alias("Date") ).agg(_mean(column_name).alias(column_name)).orderBy("Date").toPandas()

            ax.plot(graph_result["Date"], graph_result[column_name])
            stringDate = datetime.today().strftime("%d-%m-%Y-%H-%M-%S")
            fig.imsave(f"{stringDate}_line_year_mean.png")
            pass
        pass
    elif graph_type == 2:
        #Gráfico de barras
        column_name = input("Informe a coluna que deseja analisar: ")
        
        graph_group_type = print_graph_group_menu()
        
        if graph_group_type == 1:
            #Agrupar por mês
            graph_result = df.groupBy(F.month(col("Date")).alias("Date") ).agg(_mean(column_name).alias(column_name)).orderBy("Date").toPandas()

            ax.bar(graph_result["Date"], graph_result[column_name])
            stringDate = datetime.today().strftime("%d-%m-%Y-%H-%M-%S")
            fig.imsave(f"{stringDate}_bar_month_mean.png")
            pass
        elif graph_group_type == 2:
            #Agrupar por ano
            graph_result = df.groupBy(F.year(col("Date")).alias("Date") ).agg(_mean(column_name).alias(column_name)).orderBy("Date").toPandas()

            ax.bar(graph_result["Date"], graph_result[column_name])
            stringDate = datetime.today().strftime("%d-%m-%Y-%H-%M-%S")
            fig.imsave(f"{stringDate}_bar_year_mean.png")
            pass
        pass                
    elif graph_type == 3:
        #Gráfico de dispersão
        x_column_name = input("Informe a coluna que deseja analisar no eixo X: ")
        y_column_name = input("Informe a coluna que deseja analisar no eixo Y: ")
        
        graph_group_type = print_graph_group_menu()
        if graph_group_type == 1:
            #Agrupar por mês
            x_graph_result = df.groupBy(F.month(col("Date")).alias("Date") ).agg(_mean(x_column_name).alias(x_column_name)).orderBy("Date").toPandas()
            y_graph_result = df.groupBy(F.month(col("Date")).alias("Date") ).agg(_mean(y_column_name).alias(y_column_name)).orderBy("Date").toPandas()
            
            ax.scatter(x_graph_result[x_column_name], y_graph_result[x_column_name])
            stringDate = datetime.today().strftime("%d-%m-%Y-%H-%M-%S")
            fig.imsave(f"{stringDate}_scatter_month_mean.png")
            pass
        elif graph_group_type == 2:
            #Agrupar por ano
            x_graph_result = df.groupBy(F.month(col("Date")).alias("Date") ).agg(_mean(x_column_name).alias(x_column_name)).orderBy("Date").toPandas()
            y_graph_result = df.groupBy(F.month(col("Date")).alias("Date") ).agg(_mean(y_column_name).alias(y_column_name)).orderBy("Date").toPandas()
            
            ax.scatter(x_graph_result[x_column_name], y_graph_result[x_column_name])
            stringDate = datetime.today().strftime("%d-%m-%Y-%H-%M-%S")
            fig.imsave(f"{stringDate}_scatter_month_mean.png")
            pass
        pass    

def request_start_end_dates():
    start_date_str = input("Informe a data de início no formato dd/mm/yyyy: ")
    start_date = datetime.strptime(start_date_str, '%d/%m/%Y')
    
    end_date_str = input("Informe a data de término no formato dd/mm/yyyy: ")
    end_date = datetime.strptime(end_date_str, '%d/%m/%Y')

    return start_date, end_date


def handle_mean_calc():
    global data_frame
    
    column_name = input("Informe a coluna que deseja analisar: ")

    start_date, end_date = request_start_end_dates()

    mean(data_frame, column_name, start_date, end_date).show()

def handle_stddev_calc():
    global data_frame
    
    column_name = input("Informe a coluna que deseja analisar: ")

    start_date, end_date = request_start_end_dates()

    std(data_frame, column_name, start_date, end_date).show()

def handle_variance_calc():
    global data_frame
    
    column_name = input("Informe a coluna que deseja analisar: ")

    start_date, end_date = request_start_end_dates()

    variance(data_frame, column_name, start_date, end_date).show()

def handle_median_calc():
    global data_frame
    
    column_name = input("Informe a coluna que deseja analisar: ")

    start_date, end_date = request_start_end_dates()

    print(f"Mediana: {median(data_frame, column_name, start_date, end_date)}") 





print("Bem vindo! ")

print("Informe os anos em que gostaria de fazer a análise")
start_year = int(input("Ano inicial: "))
end_year = int(input("Ano Final: "))

base_dir = "assets/raw"

years_list = list(filter(lambda x: int(x) in range(start_year, end_year), os.listdir(base_dir)))

print("CARREGANDO DADOS")
data_frame = spark.read.option("header", True).csv([ f"assets/raw/{x}/*.csv" for x in years_list ])

menu_item = print_menu()

while(menu_item != 9):
    if menu_item == 0:
        # Reler base de dados
        pass        
    elif menu_item == 1:
        #Calcular Média
        handle_mean_calc()
        menu_item = print_menu()
    elif menu_item == 2:
        #Calcular Mediana
        handle_median_calc()
        menu_item = print_menu()
    elif menu_item == 3:
        #Calcular Desvio Padrão
        handle_stddev_calc()
        menu_item = print_menu()
    elif menu_item == 4:
        #Calcular Variância
        handle_variance_calc()
        menu_item = print_menu()
    elif menu_item == 5:
        #Gerar Gráficos....       
        handle_graph_generator()
        menu_item = print_menu()
    elif menu_item == 6:
        #Aplicar Método dos Quadrados Mínimos para predição            
        menu_item = print_menu()
    elif menu_item == 7:
        #Exibir dataframe
        menu_item = print_menu()
    elif menu_item == 8:
        #Nada aqui por enquanto
        menu_item = print_menu()
    elif menu_item == 9:
        pass
    else: 
        pass

print("Saindo do sistema...")

# print("Script inicializado... \n")

# df = readSampleDate(spark)
# df = transformColumns(df)

# df = transformData(df)
# # df = df.withColumn("TEMP", when(df.TEMP == 17.89, 231546).otherwise(df.TEMP))
# df.show(5)
# '''
# print("Digite a coluna que deseja agrupar: ")
# campo = input()
# '''
# # print(df.filter(df["SLP"] >= 9999.9).show())
# # print(df.groupBy(campo).count().orderBy("count", ascending=True).show())
# '''
# df_stats = df.select(
#     F.mean(F.col('TEMP')).alias('mean'),
#     F.stddev(F.col('TEMP')).alias('std')
# ).collect()

# mean = df_stats[0]['mean']
# std = df_stats[0]['std']
# '''

# print("A média é: ", media(df,'TEMP'))
# print("O desvio é: ", desvioPadrao(df, 'TEMP'))
# print("A mediana é: ", mediana(df, 'TEMP'))
