import os
import pandas as pd
#from tqdm import tqdm  

# Caminho da pasta onde estão os arquivos
pasta_socios = "arquivos_extraidos/SIMPLES"

# Iterando por todos os arquivos na pasta
for arquivo in os.listdir(pasta_socios):
    caminho_arquivo = os.path.join(pasta_socios, arquivo)
    
    # Verificando se o arquivo não tem a extensão .csv
    if not arquivo.endswith(".csv"):
        novo_nome = arquivo + ".csv"  # Adicionando a extensão .csv
        novo_caminho = os.path.join(pasta_socios, novo_nome)
        
        # Renomeando o arquivo
        os.rename(caminho_arquivo, novo_caminho)
        print(f"Arquivo renomeado para: {novo_nome}")

