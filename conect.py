import os
import requests
from bs4 import BeautifulSoup
import urllib.request
import zipfile
import sys

# Função para criar diretórios
def makedirs(path):
    if not os.path.exists(path):
        os.makedirs(path)

# Função para verificar se o arquivo já foi baixado
def check_diff(url, file_name):
    if not os.path.isfile(file_name):
        return True  

    response = requests.head(url)
    new_size = int(response.headers.get('content-length', 0))
    old_size = os.path.getsize(file_name)
    if new_size != old_size:
        os.remove(file_name)
        return True  

    return False  # Arquivos são iguais

# Função para fazer o download dos arquivos com progresso
def download_file(url, file_name):
    print(f"Baixando: {file_name}")
    with open(file_name, 'wb') as f:
        
        # Fazer o download do arquivo em blocos de 1 KB
        with requests.get(url, stream=True) as r:
            total_size = int(r.headers.get('content-length', 0))
            downloaded = 0
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    
                    
                    # Calcular a porcentagem de progresso
                    progress = (downloaded / total_size) * 100
                    sys.stdout.write(f"\rProgresso do download: {progress:.2f}%")
                    sys.stdout.flush()
    print("\nDownload concluído!")

# Função para extrair arquivos zip com progresso
def extract_zip(file_name, extract_path):
    print(f"Extraindo: {file_name}")
    
    # Abrir o arquivo ZIP
    with zipfile.ZipFile(file_name, 'r') as zip_ref:
        # Listar todos os arquivos dentro do ZIP
        zip_files = zip_ref.namelist()
        total_files = len(zip_files)
        
        for i, file in enumerate(zip_files):
            
            # Extrair o arquivo
            zip_ref.extract(file, extract_path)
            
            # Calcular o progresso da extração
            progress = (i + 1) * 100 / total_files
            sys.stdout.write(f"\rProgresso da extração: {progress:.2f}% - {file}")
            sys.stdout.flush()
    print("\nExtração concluída!")



# URL da página com os arquivos
url_base = 'https://arquivos.receitafederal.gov.br/cnpj/dados_abertos_cnpj/2025-01/'

# Caminhos de saída
output_files = 'arquivos_download'
extracted_files = 'arquivos_extraidos'

# Criação dos diretórios de saída
makedirs(output_files)
makedirs(extracted_files)

# Acessando a página com os links dos arquivos
response = requests.get(url_base)
soup = BeautifulSoup(response.text, 'html.parser')

# Encontrar todos os links para arquivos .zip
links = []
for a_tag in soup.find_all('a', href=True):
    if a_tag['href'].endswith('.zip'):
        links.append(a_tag['href'])

print(f"Arquivos encontrados: {len(links)}")



for link in links:
    file_name = os.path.join(output_files, link.split('/')[-1])  
    file_url = url_base + link  

    # Verificar se o arquivo já foi baixado e se há diferenças
    if check_diff(file_url, file_name):
        download_file(file_url, file_name)
        
        
        # Extrair o arquivo .zip depois de baixar
        extract_zip(file_name, extracted_files) 
         
    else:
        print(f"O arquivo {file_name} já está atualizado.")
