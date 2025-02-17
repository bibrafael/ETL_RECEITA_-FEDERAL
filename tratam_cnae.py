import pandas as pd
import glob, os
from tqdm import tqdm
import pyarrow.parquet as pq
import pyarrow as pa

# Caminho da pasta onde estão os arquivos CSV
pasta_cnae = "arquivos_extraidos/CNAE/"

# Usar glob para listar todos os arquivos CSV dentro da pasta
arquivo_cnae = glob.glob(os.path.join(pasta_cnae, "*.csv"))

# Lista para armazenar os DataFrames de cada arquivo
lista_dfs = []

# Quantidade de linhas processadas por vez, para não estourar a memória
chunk_size = 30000

# Definir o nome do arquivo parquet
output_parquet = "cnae_completo.parquet"

# Definir o caminho completo do arquivo parquet
caminho_completo = pasta_cnae+output_parquet

# Preparar o ParquetWriter para salvar os dados progressivamente
with tqdm(total=len(arquivo_cnae), desc="Processando arquivos", unit="arquivo") as pbar:
    for arquivo in arquivo_cnae:
        print(f"Lendo arquivo {arquivo}")

        # Processar arquivo em chunks
        for chunk in pd.read_csv(
                            arquivo, 
                            sep=";", 
                            encoding="latin1",  
                            dtype=str, 
                            quotechar='"', 
                            header=None,  # Indica que não há cabeçalho na primeira linha
                            chunksize=chunk_size
                            ):
            
            # Definindo manualmente os nomes das colunas
            chunk.columns = ["CÓDIGO", "DESCRIÇÃO"]
            
            # Remover espaços extras das colunas
            chunk = chunk.map(lambda x: x.strip() if isinstance(x, str) else x)

            # Converter o DataFrame em um TAble (para o formato Parquet)
            tabela_cnae = pa.Table.from_pandas(chunk)

            # Usar ParquetWriter para salvar os dados progressivamente
            if not os.path.exists(output_parquet): # Se o arquivo não existe, cria o arquivo
                pq.write_table(tabela_cnae, caminho_completo, compression='SNAPPY')
            else: # Caso contrário, abre o arquivo e anexa os dados
                with pq.ParquetWriter(caminho_completo, tabela_cnae.schema, compression='SNAPPY') as writer:
                    writer.write_table(tabela_cnae)
                    
print("Arquivo Parquet salvo com sucesso!")