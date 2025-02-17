import glob, os
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from tqdm import tqdm
from funcoes_uteis import csvToSeriesIndex

# Caminho da pasta onde estão os arquivos CSV
pasta_empresa = "arquivos_extraidos/EMPRE/"
pasta_natureza = "arquivos_extraidos/NATJU/"
pasta_qualificacao = "arquivos_extraidos/QUALS/"

# Usar glob para listar todos os arquivos CSV dentro da pasta
arquivos_csv = glob.glob(os.path.join(pasta_empresa, "*.csv"))
arquivo_natureza = glob.glob(os.path.join(pasta_natureza, "*.csv"))
arquivo_qualificacao = glob.glob(os.path.join(pasta_qualificacao, "*.csv"))

# Lista para armazenar os DataFrames de cada arquivo
lista_dfs = []

# Lê os arquivos CSV e transforma em Series para substituição de valores
natureza = csvToSeriesIndex(arquivo_natureza[0])
qualificacao = csvToSeriesIndex(arquivo_qualificacao[0])

# Define uma Series para substituicao
porte = {'00':'Não Informado', '01':'Micro Empresa', '03':'Empresa de Pequeno Porte', '05':'Demais'}

# Define a quantidade de linhas processadas por vez, evita estouro de memória
chunk_size = 30000

# Definir o nome do arquivo Parquet
output_parquet = "empresa_completo.parquet"

# Caminho completo do arquivo parquet
caminho_completo = pasta_empresa+output_parquet

# Preparar o ParquetWriter para salvar os dados progressivamente
with tqdm(total=len(arquivos_csv), desc="Processando arquivos", unit="arquivo") as pbar:

    # Iterar sobre os arquivos CSV e ler cada um
    for arquivo in arquivos_csv:
        print(f"Lendo arquivo {arquivo}")

        for chunk in pd.read_csv(
            arquivo,
            sep=";",
            encoding="latin1",
            dtype=str,
            quotechar='"',
            header=None,
            chunksize=chunk_size):
            
            # Atribuir os nomes das colunas manualmente, com base na estrutura esperada
            chunk.columns = [
                'cnpj_basico',
                'razao_social',
                'natureza_juridica',
                'qualificacao_responsavel',
                'capital_social',
                'porte_empresa',
                'ente_federativo'
            ]

            # Remover espaços extras das colunas
            chunk = chunk.map(lambda x: x.strip() if isinstance(x, str) else x)

            # Fazer as substituições de acordo com o documento de metadados
            chunk['natureza_juridica'] = chunk['natureza_juridica'].map(natureza)
            chunk['qualificacao_responsavel'] = chunk['qualificacao_responsavel'].map(qualificacao)
            chunk['porte_empresa'] = chunk['porte_empresa'].map(porte)

            # Adicionar o DataFrame à lista
            tabela = pa.Table.from_pandas(chunk)

            # Usar ParquetWriter para salvar os dados progressivamente
            if not os.path.exists(caminho_completo): # Se o arquivo não existe, cria o arquivo
                pq.write_table(tabela, caminho_completo, compression='SNAPPY')
            else: # Caso contrário, abre o arquivo e anexa os dados
                with pq.ParquetWriter(caminho_completo, tabela.schema, compression='SNAPPY') as writer:
                    writer.write_table(tabela)
        
        # Atualiza a barra de progresso após processar cada arquivo
        pbar.update(1)

print("Arquivo Parquet salvo com sucesso!")