import pandas as pd

caminho_completo = "arquivos_extraidos/SOCIO/socio_completo.parquet"
socios = pd.read_parquet(caminho_completo)
socios['identificador_socio'] = socios['identificador_socio'].astype(str)
for socio in socios['identificador_socio']:
    
    if socio == '1':
        socio = 'Pessoa Jurídica'
    elif socio == '2':
        socio = 'Pessoa Física'
    else:
        socio = 'Estrangeiro'

    
    
print(socios)