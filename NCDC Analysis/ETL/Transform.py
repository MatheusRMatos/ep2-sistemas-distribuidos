import pyspark.sql.types
from pyspark.sql.functions import round, col, when

'''
Arquivo de Transformação dos dados do DataFrame:
    + Sistema universal de medidas
        + Transformar dados
        + Tratar dos casos especiais
    + Transformação dos tipos de colunas
    - Filtrar missing data
'''

# Transforma colunas String nos tipos adequados, chamado antes do renameColumns()
def transformColumns(df):
    df = df.withColumn("DATE", df.DATE.cast('date')) \
        .withColumn("LATITUDE", df.LATITUDE.cast('float')) \
        .withColumn("LONGITUDE", df.LONGITUDE.cast('float')) \
        .withColumn("ELEVATION", df.ELEVATION.cast('float')) \
        .withColumn("TEMP", df.TEMP.cast('float')) \
        .withColumn("TEMP_ATTRIBUTES", df.TEMP_ATTRIBUTES.cast('integer')) \
        .withColumn("DEWP", df.DEWP.cast('float')) \
        .withColumn("DEWP_ATTRIBUTES", df.DEWP_ATTRIBUTES.cast('integer')) \
        .withColumn("SLP", df.SLP.cast('float')) \
        .withColumn("SLP_ATTRIBUTES", df.SLP_ATTRIBUTES.cast('integer')) \
        .withColumn("STP", df.STP.cast('float')) \
        .withColumn("STP_ATTRIBUTES", df.STP_ATTRIBUTES.cast('integer')) \
        .withColumn("VISIB", df.VISIB.cast('float')) \
        .withColumn("VISIB_ATTRIBUTES", df.VISIB_ATTRIBUTES.cast('integer')) \
        .withColumn("WDSP", df.WDSP.cast('float')) \
        .withColumn("WDSP_ATTRIBUTES", df.WDSP_ATTRIBUTES.cast('integer')) \
        .withColumn("MXSPD", df.MXSPD.cast('float')) \
        .withColumn("GUST", df.GUST.cast('float')) \
        .withColumn("MAX", df.MAX.cast('float')) \
        .withColumn("MAX_ATTRIBUTES", df.MAX_ATTRIBUTES.cast('float')) \
        .withColumn("MIN", df.MIN.cast('float')) \
        .withColumn("MIN_ATTRIBUTES", df.MIN_ATTRIBUTES.cast('float')) \
        .withColumn("PRCP", df.PRCP.cast('float')) \
        .withColumn("SNDP", df.SNDP.cast('float'))
    return df

# Transforma os dados de acordo com as unidades de medida universal
def transformData(df):
    df = df .withColumn("TEMP", when(df.TEMP >= 9999.9, df.TEMP).otherwise((df.TEMP - 32) * 5 / 9)).withColumn("TEMP", round(col("TEMP"), 2)) \
            .withColumn("DEWP", when(df.DEWP >= 9999.9, df.DEWP).otherwise((df.DEWP - 32) * 5 / 9)).withColumn("DEWP", round(col("DEWP"), 2)) \
            .withColumn("SLP", when(df.SLP >= 9999.9, df.SLP).otherwise(df.SLP / 1023.25)).withColumn("SLP", round(col("SLP"), 2)) \
            .withColumn("STP", when(df.STP >= 9999.9, df.STP).otherwise((df.STP / 1023.25))).withColumn("STP", round(col("STP"), 2)).withColumn("SLP", round(col("SLP"), 2)) \
            .withColumn("VISIB", when(df.VISIB >= 999.9, df.VISIB).otherwise((df.VISIB * 1.60934))).withColumn("VISIB", round(col("VISIB"), 2)) \
            .withColumn("WDSP", when(df.WDSP >= 999.9, df.WDSP).otherwise((df.WDSP * 1.852))).withColumn("WDSP", round(col("WDSP"), 2)) \
            .withColumn("MXSPD", when(df.MXSPD >= 999.9, df.MXSPD).otherwise((df.MXSPD * 1.852))).withColumn("MXSPD", round(col("MXSPD"), 2)) \
            .withColumn("GUST", when(df.GUST >= 999.9, df.GUST).otherwise((df.GUST * 1.852))).withColumn("GUST", round(col("GUST"), 2)) \
            .withColumn("MAX", when(df.MAX >= 9999.9, df.MAX).otherwise((df.MAX - 32) * 5 / 9)).withColumn("MAX", round(col("MAX"), 2)) \
            .withColumn("MIN", when(df.MIN >= 9999.9, df.MIN).otherwise((df.MIN - 32) * 5 / 9)).withColumn("MIN", round(col("MIN"), 2)) \
            .withColumn("PRCP", when(df.PRCP >= 99.99, df.PRCP).otherwise((df.PRCP * 25.4))).withColumn("PRCP", round(col("PRCP"), 2)) \
            .withColumn("SNDP", when(df.SNDP >= 999.9, df.SNDP).otherwise((df.SNDP * 25.4))).withColumn("SNDP", round(col("SNDP"), 2))
    return df

# Transforma colunas String nos tipos adequados
def renameColumns(df):
    df = df.withColumnRenamed("STATION", "CÓDIGO ESTAÇÃO") \
        .withColumnRenamed("DATE", "DATA") \
        .withColumnRenamed("ELEVATION", "ALTITUDE") \
        .withColumnRenamed("NAME", "NOME")
    return df
