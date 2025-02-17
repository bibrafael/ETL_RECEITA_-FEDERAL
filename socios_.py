import pandas as pd
import os
import glob
from tqdm import tqdm
import pyarrow.parquet as pq
import pyarrow as pa
from funcoes_uteis import csvToSeriesIndex

# Caminho da pasta onde estão os arquivos CSV
pasta_socios = "arquivos_extraidos/SOCIO/"
pasta_paises = "arquivos_extraidos/PAIS/"
pasta_quals = "arquivos_extraidos/QUALS/"

# Usar glob para listar todos os arquivos CSV dentro da pasta
arquivos_csv = glob.glob(os.path.join(pasta_socios, "*.csv"))
arquivo_paises = glob.glob(os.path.join(pasta_paises, "*.csv"))
arquivo_quals = glob.glob(os.path.join(pasta_quals, "*.csv"))

paises = csvToSeriesIndex(arquivo_paises[0])
quals = csvToSeriesIndex(arquivo_quals[0])


# Lista para armazenar os DataFrames de cada arquivo
lista_dfs = []

chunk_size = 30000 # 30.000 linhas por vez
tipo_socio = {'1':'Pessoa Jurídica', '2':'Pessoa Física', '3':'Estrangeiro'} 



# Definir o nome do arquivo Parquet
output_parquet = "socio_completo.parquet"
caminho_completo = pasta_socios+output_parquet

# Preparar o ParquetWriter para salvar os dados progressivamente
with tqdm(total=len(arquivos_csv), desc="Processando arquivos", unit="arquivo") as pbar:
    # Iterar sobre os arquivos CSV e ler cada um
    for arquivo in arquivos_csv:
        print(f"Lendo arquivo {arquivo}")

        for chunk in pd.read_csv(arquivo, 
                        sep=";", 
                        encoding="latin1",  
                        dtype=str, 
                        quotechar='"', 
                        header=None, # Não usa a primeira linha como cabeçalho
                        chunksize=chunk_size): # Quantidade de linhas do arquivo que vai ser processada por vez
                    
            # Atribuir os nomes das colunas manualmente, com base na estrutura esperada
            chunk.columns = ['cnpj_basico', 
                            'identificador_socio',
                            'nome_socio_razao_social',
                            'cpf_cnpj_socio',
                            'qualificacao_socio',
                            'data_entrada_sociedade',
                            'pais',
                            'representante_legal',
                            'nome_do_representante',
                            'qualificacao_representante_legal',
                            'faixa_etaria'
            ]
        
            # Remover espaços extras das colunas
            chunk = chunk.map(lambda x: x.strip() if isinstance(x, str) else x)

            # Fazer as substituições de acordo com o documento de metadados
            chunk['identificador_socio'] = chunk['identificador_socio'].map(tipo_socio)
            chunk['pais'] = chunk['pais'].map(paises)
            chunk['qualificacao_socio'] = chunk['qualificacao_socio'].map(quals)
            chunk['qualificacao_representante_legal'] = chunk['qualificacao_representante_legal'].map(quals)
            
            # Adicionar o DataFrame à lista
            table = pa.Table.from_pandas(chunk)

            # Usar ParquetWriter para salvar os dados progressivamente
            if not os.path.exists(caminho_completo): # Se o arquivo não existe, cria o arquivo
                pq.write_table(table, caminho_completo, compression='SNAPPY')
            else: # Caso contrário, abre o arquivo e anexa os dados
                with pq.ParquetWriter(caminho_completo, table.schema, compression='SNAPPY') as writer:
                    writer.write_table(table)
        # Atualiza a barra de progresso após processar cada arquivo
        pbar.update(1)

print("Arquivo Parquet salvo com sucesso!")