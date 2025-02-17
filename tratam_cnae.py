import pandas as pd

caminho_arquivo = "arquivos_extraidos/CNAE/F.K03200$Z.D50111.CNAECSV"
novo_arquivo = "arquivos_extraidos/CNAE/CNAE_FORMATADO.csv"  # Nome do novo arquivo

# Lendo o arquivo sem cabeçalho
df = pd.read_csv(
    caminho_arquivo, 
    sep=";", 
    encoding="latin1",  
    dtype=str, 
    quotechar='"', 
    header=None  # Indica que não há cabeçalho na primeira linha
)

# Definindo manualmente os nomes das colunas
df.columns = ["CÓDIGO", "DESCRIÇÃO"]

df["CÓDIGO"] = df["CÓDIGO"].str.replace(",", ".").astype(int)

# Salvando em um novo CSV com os novos cabeçalhos
df.to_csv(novo_arquivo, sep=";", index=False, encoding="latin1")

print(df.info())
