import requests
import pandas as pd
import time
import os  # Importar o módulo os
from Credenciais import obter_credenciais


# Função para fazer a requisição à API com tentativas e repetições
def fazer_requisicao(url, subdominio, tentativas=3, intervalo=5):
    token = obter_credenciais(subdominio)
    headers = {
        'Authorization': token
    }
    for tentativa in range(tentativas):
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()  # Lança uma exceção para erros HTTP
            return response.json()
        except requests.HTTPError as http_err:
            print(f"Erro na requisição HTTP: {http_err}")
            print(f"URL: {url}")
            print(f"Headers: {headers}")
            print(f"Resposta: {response.text}")  # Exibe o conteúdo da resposta para depuração
            
            if response.status_code in [400, 504]:
                if tentativa < tentativas - 1:
                    print(f"Tentando novamente em {intervalo} segundos...")
                    time.sleep(intervalo)
                else:
                    raise
            else:
                raise

# Função para processar os dados da API
def processar_dados(subdominio):
    base_url = f'https://api.sienge.com.br/{subdominio}/public/api/v1/units?'
    limit = 200
    all_data = []

    offset = 0
    while True:
        url = f"{base_url}limit={limit}&offset={offset}"
        print(f"Fazendo requisição para URL: {url}")  # Adiciona logging da URL
        try:
            data = fazer_requisicao(url, subdominio)
        except requests.HTTPError as e:
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

if __name__ == '__main__':
    subdominio_macapa = 'macapainvest'
    subdominio_sej = 'sej'

    # Medir o tempo de execução
    tempo_inicio = time.time()

    try:
        dados_macapa = processar_dados(subdominio_macapa)
        print("Dados de 'Macapa' armazenados em DataFrame.")

        dados_sej = processar_dados(subdominio_sej)
        print("Dados de 'sej' armazenados em DataFrame.")

        # Concatenar os DataFrames
        dados_combinados = pd.concat([dados_macapa, dados_sej], ignore_index=True)
        print("Dados combinados de 'Macapa' e 'sej' armazenados em um único DataFrame.")

        # Colunas a serem convertidas para int
        colunas_para_int = ['privateArea', 'enterpriseId', 'indexerId']

        # Converter colunas para int
        for col in colunas_para_int:
            dados_combinados[col] = dados_combinados[col].astype(int)

        # Função para substituir ponto por vírgula em valores float
        def format_float(value):
            if isinstance(value, float):
                return f"{value:.2f}".replace('.', ',')
            return value

        # Aplicar a função de formatação para cada valor no DataFrame
        dados_combinados = dados_combinados.applymap(format_float)

        # Dropar colunas indesejadas
        colunas_para_dropar = ['childUnits', 'groupings', 'specialValues', 'links', 'subdominio']
        dados_combinados = dados_combinados.drop(columns=colunas_para_dropar)

        # Caminho do arquivo CSV
        caminho_unidades = 'unidades.csv'

        # Verificar se o arquivo já existe e excluí-lo
        if os.path.exists(caminho_unidades):
            os.remove(caminho_unidades)
            print(f"Arquivo existente excluído: {caminho_unidades}")

        # Salvar o DataFrame em CSV
        dados_combinados.to_csv(caminho_unidades, index=False)
        print("Dados combinados salvos em 'unidades'.")

    except requests.HTTPError as http_err:
        print(f"Erro na requisição HTTP: {http_err}")
    except ValueError as e:
        print(e)
    except Exception as err:
        print(f"Erro inesperado: {err}")

    # Medir o tempo de execução
    tempo_fim = time.time()
    tempo_execucao = tempo_fim - tempo_inicio
    print(f"Tempo de execução: {tempo_execucao:.2f} segundos")

    # Salvar o tempo de execução em um arquivo
    caminho_tempo_execucao = os.path.join(os.path.dirname(caminho_unidades), 'tempo_execucao.txt')
    with open(caminho_tempo_execucao, 'w') as arquivo_tempo:
        arquivo_tempo.write(f"Tempo de execução: {tempo_execucao:.2f} segundos\n")
    print(f"Tempo de execução salvo em: {caminho_tempo_execucao}")
