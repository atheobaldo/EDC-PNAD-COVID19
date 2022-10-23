import zipfile
import requests
from io import BytesIO
import os
import boto3
from urllib.request import urlretrieve

names = [
     "PNAD_COVID_052020",
     "PNAD_COVID_062020",
     "PNAD_COVID_072020",
     "PNAD_COVID_082020",
     "PNAD_COVID_092020",
     "PNAD_COVID_102020",
     "PNAD_COVID_112020",
]

urls = [
    "https://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_PNAD_COVID19/Microdados/Dados/PNAD_COVID_052020.zip",
    "https://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_PNAD_COVID19/Microdados/Dados/PNAD_COVID_062020.zip",
    "https://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_PNAD_COVID19/Microdados/Dados/PNAD_COVID_072020.zip",
    "https://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_PNAD_COVID19/Microdados/Dados/PNAD_COVID_082020.zip",
    "https://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_PNAD_COVID19/Microdados/Dados/PNAD_COVID_092020.zip",
    "https://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_PNAD_COVID19/Microdados/Dados/PNAD_COVID_102020.zip",
    "https://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_PNAD_COVID19/Microdados/Dados/PNAD_COVID_112020.zip",
]

basepath = './pnad_covid_2020'
bucketname = 'igti-datalake-astheobaldo'
datalake = '/raw/pnad/'

def obter_dados(url, name):
    filename = basepath + '/' + name + '.zip'
    urlretrieve(url, filename=filename)
    print(filename)
    
    filebytes = BytesIO(requests.get(url, stream=True).content)
    archive = zipfile.ZipFile(filebytes)
    archive.extractall(basepath)
    archive.close()
    os.remove(filename)
    return True

def upload_to_datalake(s3_client, name):
    filenamesource = basepath + '/' + name + '.csv'
    filenametarget = name + '.csv'
    print(f"Upload {filenamesource} to S3...")
    s3_client.upload_file(filenamesource, bucketname, datalake + filenametarget)

if __name__ == "__main__":
    os.makedirs(basepath, exist_ok=True)

    s3_client = boto3.client('s3', aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
    
    #Download dos arquivos do ibge
    for i in range(len(urls)):
        print(f"Extracting from {urls[i]}")
        res = obter_dados(urls[i], names[i])
        print(res)
    
    #Upload dos arquivos para datalake no S3
    for i in range(len(urls)):
        print(f"Upload do arquivo {urls[i]} para o datalake")
        res = upload_to_datalake(s3_client, names[i])
        print(res)
        
    print("Done!")