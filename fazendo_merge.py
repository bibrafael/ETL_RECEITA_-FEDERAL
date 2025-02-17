import pandas as pd

# Caminhos dos arquivos
caminho_cnae = r'arquivos_extraidos/CNAE/CNAE_FORMATADO.csv'
caminho_estabelecimento = r'arquivos_extraidos/ESTABELE/estabelecimentos_completo.parquet'

# Lendo os arquivos
cnae = pd.read_csv(caminho_cnae, delimiter=';', encoding='ISO-8859-1', dtype=str)
estabelecimento = pd.read_parquet(caminho_estabelecimento)

# Verificando os tipos de dados
#print(cnae.dtypes)
#print(estabelecimento.dtypes)

# Convertendo para string para evitar problemas de tipos
cnae['CÓDIGO'] = cnae['CÓDIGO'].astype(str)
estabelecimento['cnae_fiscal_principal'] = estabelecimento['cnae_fiscal_principal'].astype(str)
estabelecimento['cnae_fiscal_secundaria'] = estabelecimento['cnae_fiscal_secundaria'].astype(str)
estabelecimento['nome_fantasia'] = estabelecimento['nome_fantasia'].astype(str)

# Realizando o merge
df_merged = estabelecimento.merge(cnae, left_on='cnae_fiscal_principal', right_on='CÓDIGO', how='left')

# Listas de CNAEs e Estados desejados
lista_cnae = ['5320202']
#lista_uf = ['MG']
#situacao = ['02']
# Filtrando o DataFrame com base nas listas
df_filtrado = df_merged[
    (df_merged['cnae_fiscal_principal'].isin(lista_cnae)) #|
    #(df_merged['uf'].isin(lista_uf)) #& 
    #(df_merged['situacao_cadastral'].isin(situacao))
]

# Exibindo o resultado
print(df_filtrado)

