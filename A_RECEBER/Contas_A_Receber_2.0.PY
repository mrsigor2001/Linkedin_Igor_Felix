import requests
import pandas as pd
from datetime import datetime
import base64
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from Credenciais import obter_credenciais
from threading import Semaphore  # Importar o semáforo



# Função para fazer o log dos status
def log_status(message):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}")

# Função para formatar o tempo gasto
def format_time(seconds):
    minutes = int(seconds // 60)
    remaining_seconds = int(seconds % 60)
    return f"{minutes} minutos e {remaining_seconds} segundos"
# Definir o semáforo para permitir até N threads simultâneas (modifique o valor de N conforme necessário)
sem = Semaphore(5)
# Função para buscar dados da API com o uso de semáforo
def fetch_data(url, token_autorizacao, start_date, end_date, subdominio):
    with sem:  # Adquirir o semáforo antes de executar a função
        params = {
            'startDate': start_date,
            'endDate': end_date,
            'selectionType': 'D'
        }
        headers = {'Authorization': token_autorizacao}

        log_status(f"Fazendo requisição para o período: {subdominio} - {start_date} a {end_date} para o subdomínio: {subdominio}")

        for attempt in range(5):  # Tentativas: 0 e 1
            try:
                start_time = time.time()  # Tempo antes da requisição
                response = requests.get(url, params=params, headers=headers)
                end_time = time.time()  # Tempo após a requisição
                duration = end_time - start_time  # Tempo gasto na requisição

                log_status(f"Status da requisição de {subdominio} - {start_date} a {end_date}: {response.status_code} - {response.reason}")
                log_status(f"Tempo da requisição {subdominio} - {start_date} a {end_date}: {format_time(duration)}")

                if response.status_code == 200:
                    data = response.json()
                    return data.get('data', [])
                else:
                    log_status(f"Erro na requisição {subdominio} - {start_date} a {end_date}: {response.status_code} - {response.reason}")
                    time.sleep()
            except requests.RequestException as e:
                log_status(f"Erro durante a requisição {subdominio} - {start_date} a {end_date}: {e}. Tentativa {attempt + 1}")
                time.sleep(20)  # Aguarda 20 segundos antes de tentar novamente

        log_status(f"Falha ao obter dados para o período: {start_date} a {end_date} no subdomínio: {subdominio}. Pulando para o próximo intervalo.")
        return []
    
    
# Função para processar os dados
def process_data(subdominio, start_date, end_date):
    url = f'https://api.sienge.com.br/{subdominio}/public/api/bulk-data/v1/income'
    token_autorizacao = obter_credenciais(subdominio)

    data = fetch_data(url, token_autorizacao, start_date, end_date, subdominio)

    if not data:
        log_status(f"Nenhum dado encontrado para o período: {start_date} a {end_date} no subdomínio: {subdominio}")
        return pd.DataFrame()  # Retorna um DataFrame vazio

    df = pd.json_normalize(data)

    if df.empty:
        log_status(f"Nenhum dado encontrado para o período: {start_date} a {end_date} no subdomínio: {subdominio}")
        return pd.DataFrame()  # Retorna um DataFrame vazio

    # Verificar se 'companyId', 'billId' e 'installmentNumber' estão presentes
    if 'companyId' not in df.columns or 'billId' not in df.columns or 'installmentNumber' not in df.columns:
        return pd.DataFrame()  # Retorna um DataFrame vazio

    # Criar um índice único sequencial
    df['uniqueIndex'] = pd.RangeIndex(start=0, stop=len(df))

    # Criar a coluna 'ChaveEspecifica'
    df['ChaveEspecifica'] = (df['companyId'].astype(str) + '-' +
                              df['billId'].astype(str) + '-' +
                              df['installmentNumber'].astype(str))

    # Adicionar a coluna 'subdominio'
    df['subdominio'] = subdominio

    # Filtrar o DataFrame para manter apenas registros com saldo corrigido maior que zero
    df = df[df['correctedBalanceAmount'] > 0]

    # Explodir as colunas 'receipts' e 'receiptsCategories', se existirem
    if 'receiptsCategories' in df.columns:
        receiptsCategories_df = pd.json_normalize(df['receiptsCategories'].explode())
        # Preservar o índice sequencial correspondente
        receiptsCategories_df['uniqueIndex'] = df['uniqueIndex'].repeat(df['receiptsCategories'].apply(len)).reset_index(drop=True)
    else:
        receiptsCategories_df = pd.DataFrame()

    df = df.reset_index(drop=True)
    receiptsCategories_df = receiptsCategories_df.reset_index(drop=True)

    # Fazer o merge usando o índice sequencial
    df_merged = pd.merge(df, receiptsCategories_df, on='uniqueIndex', how='left')

    # Reordenar colunas
    column_order = [
        'ChaveEspecifica', 'subdominio', 'companyId', 'companyName', 'businessAreaId', 'businessAreaName',
        'projectId', 'projectName', 'groupCompanyId', 'groupCompanyName', 'holdingId',
        'holdingName', 'subsidiaryId', 'subsidiaryName', 'businessTypeId', 'businessTypeName',
        'clientId', 'clientName', 'billId', 'installmentId', 'documentIdentificationId',
        'documentIdentificationName', 'documentNumber', 'documentForecast', 'originId',
        'originalAmount', 'discountAmount', 'taxAmount', 'indexerId', 'indexerName',
        'dueDate', 'issueDate', 'billDate', 'installmentBaseDate', 'balanceAmount',
        'correctedBalanceAmount', 'periodicityType', 'embeddedInterestAmount', 'interestType',
        'interestRate', 'correctionType', 'interestBaseDate', 'defaulterSituation',
        'subJudicie', 'mainUnit', 'installmentNumber', 'paymentTerm.id', 'paymentTerm.descrition',
        'costCenterId', 'costCenterName', 'financialCategoryId', 'financialCategoryName',
        'financialCategoryReducer', 'financialCategoryType', 'financialCategoryRate','operationTypeId','operationTypeName'
    ]

    df_merged = df_merged.reindex(columns=column_order, fill_value=None)


    # Filtrar o DataFrame para excluir linhas onde 'documentIdentificationId' é 'TXCE' e 'mainUnit' está vazio
    df_merged = df_merged[~((df_merged['documentIdentificationId'] == 'TXCE') & 
                              (df_merged['mainUnit'].isna() | (df_merged['mainUnit'] == '')))]

    # Ajustar IDs e formatar valores numéricos
    df_merged = adjust_data(df_merged)

    return df_merged




# Função para ajustar dados
def adjust_data(df):
    # Garantir que clientId e billId sejam tratados como strings, removendo espaços extras
    for coluna in ['clientId', 'billId']:
        if coluna in df.columns:
            df[coluna] = df[coluna].astype(str).str.strip()  # Manter como string e remover espaços extras
            # Converter de volta para inteiro apenas se a string for um número inteiro válido
            df[coluna] = df[coluna].apply(lambda x: int(float(x)) if x.replace('.', '', 1).isdigit() else x)

    for coluna in df.select_dtypes(include=['float', 'int']).columns:
        if coluna in ['originalAmount', 'correctedBalanceAmount', 'taxAmount']:
            # Formatação para valores monetários com ponto decimal e vírgula como separador de milhar
            df[coluna] = df[coluna].apply(lambda x: f'{x:,.2f}'.replace('.', 'X').replace(',', '.').replace('X', ','))
        else:
            # Formatação para outros valores com vírgula decimal e ponto como separador de milhar
            df[coluna] = df[coluna].apply(lambda x: f'{x:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.'))

    return df

# Função para salvar o DataFrame em blocos
def save_to_csv_in_chunks(df, filename, chunk_size=10000):
    # Remove o arquivo se ele já existir
    if os.path.exists(filename):
        os.remove(filename)
    
    # Salva o DataFrame em blocos
    for start in range(0, len(df), chunk_size):
        df.iloc[start:start + chunk_size].to_csv(filename, mode='a', header=not os.path.exists(filename), index=False)

# Função para gerar intervalos de datas
def generate_date_ranges(start_year, end_year, interval):
    ranges = []
    for year in range(start_year, end_year + 1, interval):
        start_date = f'{year}-01-01'
        end_date = f'{year + interval - 1}-12-31'
        ranges.append((start_date, end_date))
    return ranges

# Função principal para gerenciar o processamento
def main(subdominios, start_year, end_year, filename):
    date_ranges = generate_date_ranges(start_year, end_year, 5)  # Intervalo de 5 anos
    all_data = []

    with ThreadPoolExecutor() as executor:
        futures = []
        for subdominio in subdominios:
            for start_date, end_date in date_ranges:
                futures.append(executor.submit(process_data, subdominio, start_date, end_date))
        
        for future in as_completed(futures):
            df = future.result()
            if not df.empty:
                all_data.append(df)

    if all_data:
        all_data_df = pd.concat(all_data, ignore_index=True)
        save_to_csv_in_chunks(all_data_df, filename)
        log_status(f"Todos os dados foram salvos no arquivo: {filename}")
    else:
        log_status("Nenhum dado foi processado.")

if __name__ == "__main__":
    start_time = time.time() 
    subdominios = ['sej', 'macapainvest']
    start_year = 2001
    end_year = 2040
    filename = os.path.join('dados_recebidos.csv')
    main(subdominios, start_year, end_year, filename)
    end_time = time.time()
    # Caminho do arquivo
    file_path = r'tempo_execucao.txt'
    duration = end_time - start_time
    # Excluir o arquivo se já existir
    if os.path.exists(file_path):
        os.remove(file_path)

    # Gravar o tempo total de execução
    with open(file_path, 'w') as log_file:
        log_file.write(f"Tempo total de execução: {format_time(duration)}\n")
