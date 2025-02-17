import pandas as pd
import glob, os

def csvToSeriesIndex(filename):

    codigo = ""
    descricao = ""    

    for linha in pd.read_csv(filename,
                        sep=";", 
                        encoding="latin1",  
                        dtype=str, 
                        quotechar='"', 
                        header=None,  # Não usa a primeira linha como cabeçalho
                        chunksize=500):
        
        linha.columns = [
            'CÓDIGO','DESCRIÇÃO'
        ]

        codigo +=  linha['CÓDIGO']
        descricao += linha['DESCRIÇÃO']

    lista = pd.Series(list(descricao), index=list(codigo))
    
    return lista