import pandas as pd
import os
import glob
from tqdm import tqdm  # Importando a barra de progresso
import pyarrow.parquet as pq
import pyarrow as pa
from funcoes_uteis import csvToSeriesIndex

# Caminho da pasta onde estão os arquivos CSV
pasta_estabele = "arquivos_extraidos/ESTABELE"
pasta_pais = "arquivos_extraidos/PAIS/"
pasta_motivo = "arquivos_extraidos/MOTIC/"
pasta_municipio = "arquivos_extraidos/MUNIC/"

# Usar glob para listar todos os arquivos CSV dentro da pasta
arquivos_csv = glob.glob(os.path.join(pasta_estabele, "*.csv"))
arquivos_pais = glob.glob(os.path.join(pasta_pais, "*.csv"))
arquivos_motivo = glob.glob(os.path.join(pasta_motivo, "*.csv"))
arquivos_munic = glob.glob(os.path.join(pasta_municipio, "*.csv"))

# Lista para armazenar os DataFrames de cada arquivo
lista_dfs = []

situacao = {'01':'Nula', '2':'Ativa', '3':'Suspensa','4':'Inapta','08':'Baixada'}
pais = csvToSeriesIndex(arquivos_pais[0])
motivo = csvToSeriesIndex(arquivos_motivo[0])
municipio = csvToSeriesIndex(arquivos_munic[0])

chunk_size = 30000  # 30.000 linhas por vez

# Definir o nome do arquivo Parquet
output_parquet = "estabelecimentos_completo.parquet"

# Definir o caminho completo do parquet
caminho_completo = pasta_estabele+output_parquet

# Preparar o ParquetWriter para salvar os dados progressivamente
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
                             'cnpj_ordem',
                             'cnpj_dv',
                             'identificador_matriz_filial',
                             'nome_fantasia',
                             'situacao_cadastral',
                             'data_situacao_cadastral',
                             'motivo_situacao_cadastral',
                             'nome_cidade_exterior',
                             'pais',
                             'data_inicio_atividade',
                             'cnae_fiscal_principal',
                             'cnae_fiscal_secundaria',
                             'tipo_logradouro',
                             'logradouro',
                             'numero',
                             'complemento',
                             'bairro',
                             'cep',
                             'uf',
                             'municipio',
                             'ddd_1',
                             'telefone_1',
                             'ddd_2',
                             'telefone_2',
                             'ddd_fax',
                             'fax',
                             'correio_eletronico',
                             'situacao_especial',
                             'data_situacao_especial']
            
            # Remover espaços extras das colunas
            chunk = chunk.map(lambda x: x.strip() if isinstance(x, str) else x)

            chunk['situacao_cadastral'] = chunk['situacao_cadastral'].map(situacao)
            chunk['pais'] = chunk['pais'].map(pais)
            chunk['motivo_situacao_cadastral'] = chunk['motivo_situacao_cadastral'].map(motivo)
            chunk['municipio'] = chunk['municipio'].map(motivo)
            
            # Reordenar o DataFrame (se necessário)
            chunk_reordenado = chunk[['cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial', 
                                      'nome_fantasia', 'situacao_cadastral', 'data_situacao_cadastral', 
                                      'motivo_situacao_cadastral', 'nome_cidade_exterior', 'pais', 
                                      'data_inicio_atividade', 'cnae_fiscal_principal', 
                                      'cnae_fiscal_secundaria', 'tipo_logradouro', 'logradouro', 
                                      'numero', 'complemento', 'bairro', 'cep', 'uf', 'municipio', 
                                      'ddd_1', 'telefone_1', 'ddd_2', 'telefone_2', 'ddd_fax', 'fax', 
                                      'correio_eletronico', 'situacao_especial', 'data_situacao_especial']]
            
            # Converter o DataFrame em um Table (para o formato Parquet)
            table = pa.Table.from_pandas(chunk_reordenado)
            
            # Usar ParquetWriter para salvar os dados progressivamente
            if not os.path.exists(output_parquet):  # Se o arquivo não existe, cria o arquivo
                pq.write_table(table, caminho_completo, compression='SNAPPY')
            else:  # Caso contrário, abre o arquivo e anexa os dados
                with pq.ParquetWriter(caminho_completo, table.schema, compression='SNAPPY') as writer:
                    writer.write_table(table)

        # Atualiza a barra de progresso após processar cada arquivo
        pbar.update(1)

print("Arquivo Parquet salvo com sucesso!")
