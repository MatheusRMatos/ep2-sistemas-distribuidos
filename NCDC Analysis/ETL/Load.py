'''
Arquivo para carregar os dados
    - Exportar o dataframe em CSV
    - Subir para algum local

'''

# Export dataframe to csv file
# Para escrever em um arquivo: coalesce(1), (não tem paralelismo)
def exportarCSV(df):
    df.write.csv("assets/samples/export/data.csv")

