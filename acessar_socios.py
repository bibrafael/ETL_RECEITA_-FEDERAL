from union import combinar_socios

# Caminho onde estão os arquivos CSV
pasta_socios = "D:/projeto_novo/arquivos_extraidos/SOCIO"
# Caminho do arquivo final
caminho_arquivo_final = "D:/projeto_novo/df_combinados/socios_combinados.csv"

# Chamando a função para combinar os arquivos
df_combinado = combinar_socios(pasta_socios, caminho_arquivo_final)

# Agora você pode acessar o df_combinado
if df_combinado is not None:
    print(df_combinado.head())  # Exemplo de como usar o DataFrame
    print(f"DataFrame combinado contém {df_combinado.shape[0]} linhas e {df_combinado.shape[1]} colunas.")

else:
    print("Nenhum dado foi combinado.")
