import os 
import shutil

pasta = "arquivos_extraidos/"

categorias = ["EMPRE", "SOCIO", "ESTABELE","MUNIC", "MOTIC","PAIS","NATJU","CNAE", "QUALS", "SIMPLES"]

for categoria in categorias:
    rota_da_categoria = os.path.join(pasta,categoria)
    if not os.path.exists(rota_da_categoria):
        os.makedirs(rota_da_categoria)
        
for arquivos in os.listdir(pasta):
    rota_arquivos = os.path.join(pasta, arquivos)
    

    if os.path.isfile(rota_arquivos):
        for categoria in categorias:
            if categoria.lower() in arquivos.lower():
                
                destino = os.path.join(pasta, categoria, arquivos)
                shutil.move(rota_arquivos, destino)
                print(f"Arquivo {arquivos} movido para a pasta {categoria}")
                break
            