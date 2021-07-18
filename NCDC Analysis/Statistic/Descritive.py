'''
Arquivo com análises estatisticas descritivas
    + Media
    - Moda
    + Mediana
    + Desvio Padrão
'''
from pyspark.sql import functions as F

def media(df, coluna):
    df = df.select(
        F.mean(F.col(coluna)).alias('mean')
    ).collect()
    return round(df[0]['mean'],2)


def desvioPadrao(df, coluna):
    df = df.select(
        F.stddev(F.col(coluna)).alias('std')
    ).collect()
    return round(df[0]['std'],2)

def mediana(df, coluna):
    df = df.approxQuantile(coluna, [0.5],0.01)
    return df[0]
