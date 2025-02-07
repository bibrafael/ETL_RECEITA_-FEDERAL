import pandas as pd
import os
import glob

# Caminho da pasta onde estão os arquivos CSV
pasta_socios = "arquivos_extraidos/SOCIO/"

# Usar glob para listar todos os arquivos CSV dentro da pasta
arquivos_csv = glob.glob(os.path.join(pasta_socios, "*.csv"))

# Lista para armazenar os DataFrames de cada arquivo
lista_dfs = []

# Iterar sobre os arquivos CSV e ler cada um
for arquivo in arquivos_csv:
    # Ler o arquivo CSV, agora utilizando a primeira linha como cabeçalho
    df = pd.read_csv(arquivo, 
                     sep=";", 
                     encoding="latin1",  
                     dtype=str, 
                     quotechar='"', 
                     header=None)  # Não usa a primeira linha como cabeçalho
    
    # Atribuir os nomes das colunas manualmente, com base na estrutura esperada
    colunas = [
        "cnpj_basico", "identificador_socio", "nome_socio_razao_social", 
        "cpf_cnpj_socio", "qualificacao_socio", "data_entrada_sociedade", 
        "pais", "representante_legal", "nome_do_representante", 
        "qualificacao_representante_legal", "faixa_etaria"
    ]
    
    # Atribuir as colunas ao DataFrame
    df.columns = colunas

    # Verificar os nomes das colunas para garantir que estão corretos
    print(f"Colunas do arquivo {arquivo}: {df.columns.tolist()}")  # Exibe as colunas do arquivo

    # Verificar se a coluna 'cnpj_basico' existe, e se não, criar com base no identificador_socio
    if 'cnpj_basico' not in df.columns:
        print(f"Coluna 'cnpj_basico' não encontrada no arquivo {arquivo}, criando a partir de 'identificador_socio'.")
        df['cnpj_basico'] = df['identificador_socio'].str[:8]  # Criar a coluna com os 8 primeiros caracteres de 'identificador_socio'

    # Verificar se a coluna 'nome_socio_razao_social' existe, e se não, criar com base no identificador_socio
    if 'nome_socio_razao_social' not in df.columns:
        print(f"Coluna 'nome_socio_razao_social' não encontrada no arquivo {arquivo}, criando a partir de 'identificador_socio'.")
        df['nome_socio_razao_social'] = df['identificador_socio'].str[8:]  # Criar a coluna com o restante de 'identificador_socio'

    # Remover espaços extras das colunas
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # Reordenar as colunas conforme o formato desejado
    df_reordenado = df[['cnpj_basico', 'identificador_socio', 'nome_socio_razao_social', 'cpf_cnpj_socio', 
                        'qualificacao_socio', 'data_entrada_sociedade', 'pais', 'representante_legal', 
                        'nome_do_representante', 'qualificacao_representante_legal', 'faixa_etaria']]

    # Adicionar o DataFrame à lista
    lista_dfs.append(df_reordenado)

# Concatenar todos os DataFrames em um único
df_completo = pd.concat(lista_dfs, ignore_index=True)

# Salvar o DataFrame completo em um novo arquivo CSV
df_completo.to_csv("socios_completo.csv", index=False, sep=";", 
                   encoding="latin1", quoting=1)  # quoting=1 para colocar aspas em todos os campos
