import os
import pandas as pd

# Dicionário de estados e suas respectivas regiões
regioes = {
    "Norte": ["AC", "AP", "AM", "PA", "RO", "RR", "TO"],
    "Nordeste": ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"],
    "Centro-Oeste": ["DF", "GO", "MT", "MS"],
    "Sudeste": ["ES", "MG", "RJ", "SP"],
    "Sul": ["PR", "RS", "SC"]
}

# Carregar o DataFrame do Parquet
df = pd.read_parquet("arquivos_extraidos/ESTABELE/estabelecimentos_completo.parquet")

# Criar a pasta base para salvar os arquivos
base_dir = "arquivos_extraidos/ESTABELE/Regioes"
os.makedirs(base_dir, exist_ok=True)

# Iterar sobre os estados únicos no DataFrame
for estado in df['uf'].unique():   # ------ obs: colocar o Cnae para verificaçao tbm.
    # Identificar a região do estado
    regiao = next((key for key, value in regioes.items() if estado in value), None)
    
    if regiao:  # Se encontrou a região
        # Criar a pasta da região
        pasta_regiao = os.path.join(base_dir, regiao)
        os.makedirs(pasta_regiao, exist_ok=True)

        # Filtrar os dados do estado
        df_estado = df[df['uf'] == estado]

        # Caminho do arquivo CSV
        caminho_arquivo = os.path.join(pasta_regiao, f"ESTABELE_{estado}.csv")

        # Salvar em CSV com encoding latin1 e separador ;
        df_estado.to_csv(caminho_arquivo, index=False, sep=";", encoding="latin1", quoting=1)

        print(f"Arquivo salvo: {caminho_arquivo}")

print("Processo concluído! 🚀")
