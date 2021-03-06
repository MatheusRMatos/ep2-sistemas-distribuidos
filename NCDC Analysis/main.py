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
import matplotlib as mpl
from matplotlib import pyplot as plt
from sklearn.linear_model import LinearRegression
from joblib import dump as model_dump, load as model_load
from pyspark.sql.functions import mean as _mean, stddev as _stddev,count as _count, col, variance as _variance
import numpy as np

spark = SparkSession \
    .builder \
    .appName("Ep 2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

def print_menu():
    print("\n\nSelecione um item do menu: \n")
    print("0. Reler base de dados")
    print("1. Calcular Média")
    print("2. Calcular Mediana")
    print("3. Calcular Desvio Padrão")
    print("4. Calcular Variância")
    print("5. Gerar gráficos")
    print("6. Aplicar Método dos Quadrados Mínimos para predição")
    print("7. Carregar modelo de predição e realizar previsões")
    print("8. Listar colunas e tipos de dados")
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
            graph_result = data_frame.groupBy(F.month(col("Date")).alias("Date") ).agg(_mean(column_name).alias(column_name)).orderBy("Date").toPandas()

            ax.plot(graph_result["Date"], graph_result[column_name])
            stringDate = datetime.today().strftime("%d-%m-%Y-%H-%M-%S")
            fig.savefig(f"{stringDate}_line_month_mean.png")
            pass
        elif graph_group_type == 2:
            #Agrupar por ano
            graph_result = data_frame.groupBy(F.year(col("Date")).alias("Date") ).agg(_mean(column_name).alias(column_name)).orderBy("Date").toPandas()

            ax.plot(graph_result["Date"], graph_result[column_name])
            stringDate = datetime.today().strftime("%d-%m-%Y-%H-%M-%S")
            fig.savefig(f"{stringDate}_line_year_mean.png")
            pass
        pass
    elif graph_type == 2:
        #Gráfico de barras
        column_name = input("Informe a coluna que deseja analisar: ")
        
        graph_group_type = print_graph_group_menu()
        
        if graph_group_type == 1:
            #Agrupar por mês
            graph_result = data_frame.groupBy(F.month(col("Date")).alias("Date") ).agg(_mean(column_name).alias(column_name)).orderBy("Date").toPandas()

            ax.bar(graph_result["Date"], graph_result[column_name])
            stringDate = datetime.today().strftime("%d-%m-%Y-%H-%M-%S")
            fig.savefig(f"{stringDate}_bar_month_mean.png")
            pass
        elif graph_group_type == 2:
            #Agrupar por ano
            graph_result = data_frame.groupBy(F.year(col("Date")).alias("Date") ).agg(_mean(column_name).alias(column_name)).orderBy("Date").toPandas()

            ax.bar(graph_result["Date"], graph_result[column_name])
            stringDate = datetime.today().strftime("%d-%m-%Y-%H-%M-%S")
            fig.savefig(f"{stringDate}_bar_year_mean.png")
            pass
        pass                
    elif graph_type == 3:
        #Gráfico de dispersão
        x_column_name = input("Informe a coluna que deseja analisar no eixo X: ")
        y_column_name = input("Informe a coluna que deseja analisar no eixo Y: ")
        
        graph_group_type = print_graph_group_menu()
        if graph_group_type == 1:
            #Agrupar por mês
            x_graph_result = data_frame.groupBy(F.month(col("Date")).alias("Date") ).agg(_mean(x_column_name).alias(x_column_name)).orderBy("Date").toPandas()
            y_graph_result = data_frame.groupBy(F.month(col("Date")).alias("Date") ).agg(_mean(y_column_name).alias(y_column_name)).orderBy("Date").toPandas()
            
            ax.scatter(x_graph_result[x_column_name], y_graph_result[x_column_name])
            stringDate = datetime.today().strftime("%d-%m-%Y-%H-%M-%S")
            fig.savefig(f"{stringDate}_scatter_month_mean.png")
            pass
        elif graph_group_type == 2:
            #Agrupar por ano
            x_graph_result = data_frame.groupBy(F.month(col("Date")).alias("Date") ).agg(_mean(x_column_name).alias(x_column_name)).orderBy("Date").toPandas()
            y_graph_result = data_frame.groupBy(F.month(col("Date")).alias("Date") ).agg(_mean(y_column_name).alias(y_column_name)).orderBy("Date").toPandas()
            
            ax.scatter(x_graph_result[x_column_name], y_graph_result[x_column_name])
            stringDate = datetime.today().strftime("%d-%m-%Y-%H-%M-%S")
            fig.savefig(f"{stringDate}_scatter_month_mean.png")
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

def handle_mmq():
    global data_frame

    start_date, end_date = datetime(1971, 1, 1), datetime(1971, 10, 1)

    x_column_name = input("Informe a coluna que deseja analisar no eixo X: ")
    y_column_name = input("Informe a coluna que deseja analisar no eixo Y: ")    

    number_of_samples = int(input("Informe o número de amostras para o cálculo da regressão: "))

    print("Carregando dados...")

    data = data_frame.filter((col("DATE") >= start_date) & \
                             (col("DATE") <= end_date)) \
                     .select(x_column_name, y_column_name) \
                     .toPandas().sample(n = number_of_samples).astype(np.float32).sort_values(by=[x_column_name])

    print("Treinando modelo...")

    model = LinearRegression()
    model.fit(data[[x_column_name]], data[[y_column_name]])

    a = model.intercept_[0]
    b = model.coef_

    print("Treinamento finalizado...")

    print(f"O valor do intecept (a) é {a}")
    print(f"O valor do coeficiente (b) é {b[0, 0]}")
    function = lambda x : a + b[0,0]*x
    print(f"A fórmula para a regressão linear para x: {x_column_name} e y: {y_column_name} é f(x)={a} + {b[0, 0]}*x")       
    
    fig, ax = plt.subplots()
    ax.scatter(data[x_column_name], data[y_column_name])
    ax.plot(data[x_column_name], function(data[x_column_name].to_numpy().astype(np.float)), c='red', label=f"f(x)={a} + {b[0, 0]}*x")

    stringDate = datetime.today().strftime("%d-%m-%Y-%H-%M-%S")
    graph_name = f"{stringDate}_mmq.png"
    fig.savefig(graph_name)

    print(f"O gráfico foi salvo em: {graph_name}")

    saveModel = input("Gostaria de salvar o modelo atual para uso futuro? (S/N): ")
    if(saveModel == 'S'):
        model_path = f"model/{stringDate}_mmq_{x_column_name}_{y_column_name}.joblib"
        model_dump(model, model_path)
        print(f"Seu modelo foi salvo em {model_path}")

def handle_predict():

    model = None

    while(model == None):
        model_path = input("Informe a localização do arquivo do modelo: ")
        model = model_load(model_path)

        if(model == None):
            print("Modelo não encontrado ou não carregado corretamente, tente novamente.\n")
    
    stop = False

    while(stop == False):

        x_value = int(input("Informe o valor da variável do eixo X: "))

        predction = model.predict([[x_value]])

        print(f"O valor previsto foi: {predction[0, 0]}")

        new_predction = input("Gostaria de realizar uma nova predição? (S/N): ")

        if(new_predction == "S"):
            pass
        else:
            stop = True

print("\n\nBem vindo! ")

print("Informe os anos em que gostaria de fazer a análise")
start_year = int(input("Ano inicial: "))
end_year = int(input("Ano Final: "))

base_dir = "assets/raw"

years_list = list(filter(lambda x: int(x) in range(start_year, end_year), os.listdir(base_dir)))

print("Carregando dados...")
data_frame = spark.read.option("header", True).csv([ f"assets/raw/{x}/*.csv" for x in years_list ])
print("\n\n")

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
        handle_mmq()
        menu_item = print_menu()
    elif menu_item == 7:
        #Carregar modelo de predição e realizar previsões
        handle_predict()
        menu_item = print_menu()
    elif menu_item == 8:
        #Listar colunas e tipos de dados
        for field in data_frame.schema.fields:
            print(f"{field.name} -> {field.dataType}")        
        menu_item = print_menu()
    elif menu_item == 9:
        pass
    else: 
        pass

print("Saindo do sistema...")