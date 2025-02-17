import pandas as pd
import glob
from tqdm import tqdm
import pyarrow.parquet as pq
import pyarrow as pa
import os

# Caminho da pasta onde estão os arquivos CSV
pasta_socios = "arquivos_extraidos/SIMPLES/"

# Usar glob para listar todos os arquivos CSV dentro da pasta
arquivos_csv = glob.glob(os.path.join(pasta_socios, "*.csv"))

# Lista para armazenar os DataFrames de cada arquivo
lista_dfs = []

chunk_size = 20000  # 20.000 linhas por vez

# Definir o nome do arquivo Parquet
output_parquet = "simples_completo.parquet"

# Prepara o ParquetWirter para salvar os dados progressivamente
with tqdm(total=len(arquivos_csv), desc="Processando arquivos", unit="arquivo") as pbar:
    for arquivo in arquivos_csv:
        print(f"Lendo arquivo {arquivo}")

        # Processar o arquivo em chunks
        for chunk in pd.read_csv(arquivo,
                                 sep=";",
                                 encoding="latin1",
                                 dtype=str,
                                 quotechar='"',
                                 header=None,
                                 chunksize=chunk_size):
            
            # Atribuir os nomes das colunas manualmente
            chunk.columns = ['cnpj_basico',
                             'op_simples',
                             'data_simples',
                             'data_exc_simples',
                             'mei',
                             'data_mei',
                             'data_exc_mei',]
            
            # Remover espaços extras das colunas
            chunk = chunk.map(lambda x: x.strip() if isinstance(x, str) else x)

            # Converter o DataFrame em um Table (para o formato Parquet)
            table = pa.Table.from_pandas(chunk)

            # Usar ParquetWriter para salvar os dados progressivamente
            if not os.path.exists(pasta_socios+output_parquet):
                pq.write_table(table, pasta_socios+output_parquet, compression='SNAPPY')
            else:
                with pq.ParquetWriter(pasta_socios+output_parquet, table.schema, compression='SNAPPY') as writer:
                    writer.write_table(table)

        pbar.update(1)
print("Arquivo Parquet salvo com sucesso!")