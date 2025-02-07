import pandas as pd


    
    # Tente ler o arquivo com a codificação latin1
df = pd.read_csv("arquivos_extraidos/SOCIO/socios_completo.csv", sep=";", encoding="latin1", on_bad_lines='skip')

print(df.head(10))

  