import aiohttp
import asyncio
import pandas as pd
import nest_asyncio
import numpy as np
import os
from Credenciais import obter_credenciais

# Permitir a execução de loops de eventos aninhados
nest_asyncio.apply()


# Função para fazer a requisição à API com tentativas e repetições
async def fazer_requisicao(session, url, subdominio, tentativas=3, intervalo=5):
    token = obter_credenciais(subdominio)
    headers = {
        'Authorization': token
    }
    for tentativa in range(tentativas):
        try:
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()  # Lança uma exceção para erros HTTP
                return await response.json()
        except aiohttp.ClientResponseError as http_err:
            print(f"Erro na requisição HTTP: {http_err}")
            print(f"URL: {url}")
            print(f"Headers: {headers}")
            resposta_texto = await response.text()
            print(f"Resposta: {resposta_texto}")  # Exibe o conteúdo da resposta para depuração
            
            if response.status in [400, 504]:
                if tentativa < tentativas - 1:
                    print(f"Tentando novamente em {intervalo} segundos...")
                    await asyncio.sleep(intervalo)
                else:
                    raise
            else:
                raise

# Função para processar os dados da API
async def processar_dados(session, subdominio):
    base_url = f'https://api.sienge.com.br/{subdominio}/public/api/v1/sales-contracts?'
    limit = 200
    all_data = []

    offset = 0
    while True:
        url = f"{base_url}limit={limit}&offset={offset}"
        print(f"Fazendo requisição para URL: {url}")  # Adiciona logging da URL
        try:
            data = await fazer_requisicao(session, url, subdominio)
        except aiohttp.ClientResponseError as e:
            print(f"Erro ao processar dados para subdomínio {subdominio}: {e}")
            break
        
        if 'resultSetMetadata' not in data:
            print("Estrutura dos dados não contém 'resultSetMetadata'")
            break
            
        result_set_metadata = data['resultSetMetadata']
        qtd_result = result_set_metadata.get('count', 0)
        results = data.get('results', [])
        
        if not results:
            break

        # Adiciona os dados ao acumulador
        for result in results:
            result['subdominio'] = subdominio
            all_data.append(result)
        
        offset += limit
        if offset >= qtd_result:
            break

    return pd.DataFrame(all_data)

# Função para excluir arquivos se existirem
def excluir_arquivos(caminho_csv, caminho_txt):
    for caminho in [caminho_csv, caminho_txt]:
        if os.path.exists(caminho):
            os.remove(caminho)
            print(f"Arquivo excluído: {caminho}")
        else:
            print(f"Arquivo não encontrado: {caminho}")

async def main():
    subdominios = ['macapainvest', 'sej']
    async with aiohttp.ClientSession() as session:
        tasks = [processar_dados(session, subdominio) for subdominio in subdominios]
        resultados = await asyncio.gather(*tasks)

    dados_combinados = pd.concat(resultados, ignore_index=True)
    # Converter a coluna 'receivableBillId' para string
    dados_combinados['receivableBillId'] = dados_combinados['receivableBillId'].astype(str)
    # Remover a parte '.0' das strings
    dados_combinados['receivableBillId'] = dados_combinados['receivableBillId'].str.replace('.0', '', regex=False)
    # Tratar valores nulos: substituir 'nan' por np.nan
    dados_combinados['receivableBillId'] = dados_combinados['receivableBillId'].replace('nan', np.nan)
    # Converter a coluna de volta para inteiro, tratando valores nulos
    dados_combinados['receivableBillId'] = pd.to_numeric(dados_combinados['receivableBillId'], errors='coerce').astype('Int64')
    # Filtrar linhas onde 'receivableBillId' não é nulo, não é vazio e não é zero
    dados_combinados = dados_combinados[dados_combinados['receivableBillId'].notna() & 
                                        (dados_combinados['receivableBillId'] != 0) & 
                                        (dados_combinados['receivableBillId'].astype(str).str.strip() != '')]
    # Certificar-se de que as colunas estão no formato de string
    dados_combinados['companyId'] = dados_combinados['companyId'].astype(str)
    dados_combinados['enterpriseId'] = dados_combinados['enterpriseId'].astype(str)
    # Garantir que 'receivableBillId' está no formato de string
    dados_combinados['receivableBillId'] = dados_combinados['receivableBillId'].astype(str)
    # Adicionar a coluna 'ChaveEspecifica'
    dados_combinados['ChaveEspecifica'] = (dados_combinados['companyId'] + '-' + 
                                           dados_combinados['receivableBillId'])
    
    # Substituir ponto por vírgula nas colunas 'value' e 'totalSellingValue'
    dados_combinados['value'] = dados_combinados['value'].astype(str).str.replace('.', ',', regex=False)
    dados_combinados['totalSellingValue'] = dados_combinados['totalSellingValue'].astype(str).str.replace('.', ',', regex=False)
    
    # Adicionar a coluna 'subdominio'
    #dados_combinados['subdominio'] = subdominio
    # Filtrar e normalizar dados
    colunas = ['salesContractCustomers','salesContractUnits']
    for coluna in colunas:
        for index, row in dados_combinados.iterrows():
            for d in row[coluna]:
                if isinstance(d, dict):
                        d['ChaveEspecifica'] = row['ChaveEspecifica']
                        d['enterpriseId'] = row['enterpriseId']
                        d['receivableBillId'] = row['receivableBillId']
                            
    salesContractCustomers = pd.json_normalize(dados_combinados['salesContractCustomers'].explode())
    salesContractUnits = pd.json_normalize(dados_combinados['salesContractUnits'].explode())
    
    # Dropar as colunas especificadas do DataFrame
    dados_combinados = dados_combinados.drop(columns=['links', 'salesContractCustomers', 'salesContractUnits', 'paymentConditions', 'brokers'])

    print("Dados combinados de 'Macapa' e 'sej' armazenados em um único DataFrame.")
    
    # Caminhos dos arquivos
    caminho_vendas = 'Vendas.csv'
    caminho_salesContractCustomers = 'Vendas_salesContractCustomers.csv'
    caminho_salesContractUnits = 'Vendas_salesContractUnits.csv'
    
    # Excluir arquivos existentes
    excluir_arquivos(caminho_vendas, caminho_salesContractCustomers)
    excluir_arquivos(caminho_salesContractUnits, caminho_salesContractCustomers)

    # Salvar novos arquivos
    salesContractCustomers.to_csv(caminho_salesContractCustomers, index=False)
    salesContractUnits.to_csv(caminho_salesContractUnits, index=False)
    dados_combinados.to_csv(caminho_vendas, index=False)
    
    return dados_combinados

# Executa a função main e armazena o resultado em uma variável global
if __name__ == '__main__':
    try:
        dados_combinados = asyncio.run(main())
    except ValueError as e:
        print(e)
    except Exception as err:
        print(f"Erro inesperado: {err}")
