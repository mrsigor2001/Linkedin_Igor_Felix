import requests
import pandas as pd
from datetime import datetime
import os
import time
from Credenciais import obter_credenciais


def rename_columns(col_name):
    if '_x' in col_name:
        return col_name.replace('_x', '_recepts')
    elif '_y' in col_name:
        return col_name.replace('_y', '')
    return col_name

def fetch_data(url, token_autorizacao, start_date, end_date, subdominio):
    params = {
        'startDate': start_date,
        'endDate': end_date,
        'selectionType': 'P'
    }
    headers = {'Authorization': token_autorizacao}
    
    print(f"Fazendo requisição para o período: {start_date} a {end_date} para o subdomínio: {subdominio}")

    for attempt in range(2):  # Tentativas: 0 e 1
        start_time = datetime.now()
        try:
            response = requests.get(url, params=params, headers=headers)
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            print(f"Hora atual: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Tempo da requisição: {duration:.2f} segundos")
            print(f"Status da requisição: {response.status_code} - {response.reason}")
            if response.status_code == 200:
                data = response.json()
                return data.get('data', [])
            else:
                print(f"Erro na requisição: {response.status_code} - {response.reason}")
        except requests.RequestException as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            print(f"Hora atual: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Tempo da requisição: {duration:.2f} segundos")
            print(f"Erro durante a requisição: {e}. Tentativa {attempt + 1}")
            time.sleep(2)  # Aguarda 2 segundos antes de tentar novamente
    
    print(f"Falha ao obter dados para o período: {start_date} a {end_date} no subdomínio: {subdominio}. Pulando para o próximo ano.")
    return []

def process_data(subdominio, start_date, end_date):
    url = f'https://api.sienge.com.br/{subdominio}/public/api/bulk-data/v1/income'
    token_autorizacao = obter_credenciais(subdominio)

    data = fetch_data(url, token_autorizacao, start_date, end_date, subdominio)

    if not data:
        print(f"Nenhum dado encontrado para o período: {start_date} a {end_date} no subdomínio: {subdominio}")
        return pd.DataFrame()  # Retorna um DataFrame vazio

    df = pd.json_normalize(data)

    if df.empty:
        print(f"Nenhum dado encontrado para o período: {start_date} a {end_date} no subdomínio: {subdominio}")
        return pd.DataFrame()  # Retorna um DataFrame vazio

    # Verificar se 'companyId', 'billId' e 'installmentNumber' estão presentes
    if 'companyId' not in df.columns or 'billId' not in df.columns or 'installmentNumber' not in df.columns:
        return pd.DataFrame()  # Retorna um DataFrame vazio

    # Criar um índice único sequencial
    df['uniqueIndex'] = pd.RangeIndex(start=0, stop=len(df))

    # Adicionar a coluna 'ChaveEspecifica'
    df['ChaveEspecifica'] = (df['companyId'].astype(str) + '-' + 
                              df['billId'].astype(str) + '-' + 
                              df['installmentNumber'].astype(str))

    # Adicionar a coluna 'subdominio'
    df['subdominio'] = subdominio

    # Filtrar e normalizar dados
    df = df[df['receipts'].apply(lambda x: isinstance(x, list) and len(x) > 0)]
    for index, row in df.iterrows():
        for d in row['receipts']:
            if isinstance(d, dict):
                d['ChaveEspecifica'] = row['ChaveEspecifica']

    receipts_df = pd.json_normalize(df['receipts'].explode())
    receiptsCategories_df = pd.json_normalize(df['receiptsCategories'].explode())
    
    df = df.reset_index(drop=True)
    receiptsCategories_df = receiptsCategories_df.reset_index(drop=True)
    
    # Adicionar 'uniqueIndex' no receipts_df
    receipts_df['uniqueIndex'] = df['uniqueIndex'].repeat(df['receipts'].apply(len)).reset_index(drop=True)
    
    # Adicionar 'uniqueIndex' no receiptsCategories_df
    receiptsCategories_df['uniqueIndex'] = df['uniqueIndex'].repeat(df['receiptsCategories'].apply(len)).reset_index(drop=True)
    
    # Fazer o merge usando 'uniqueIndex'
    df_categories = pd.merge(df, receiptsCategories_df, on='uniqueIndex', how='left')
    df_merged = pd.merge(receipts_df, df_categories, on='uniqueIndex', how='left')

    df_merged.columns = [rename_columns(col) for col in df_merged.columns]

    # Remover a coluna 'uniqueIndex' após o merge
    df_merged = df_merged.drop(columns=['uniqueIndex'])

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
        'financialCategoryReducer', 'financialCategoryType', 'financialCategoryRate', 
        'operationTypeId', 'operationTypeName', 'grossAmount', 'monetaryCorrectionAmount', 
        'interestAmount', 'fineAmount', 'discountAmount_recepts', 'taxAmount_recepts', 
        'netAmount', 'additionAmount', 'insuranceAmount', 'dueAdmAmount', 'calculationDate', 
        'paymentDate', 'accountCompanyId', 'accountNumber', 'accountType', 'sequencialNumber', 
        'indexerId_recepts', 'embeddedInterestAmount_recepts', 'proRata'
    ]

    df_merged = df_merged.reindex(columns=column_order)
    
    # Ajustar dados
    df_merged = adjust_data(df_merged)

    # Limpar espaços e caracteres não numéricos
    df_merged['operationTypeId'] = df_merged['operationTypeId'].astype(str)

    # Remover caracteres não numéricos e converter vírgula para ponto
    df_merged['operationTypeId'] = df_merged['operationTypeId'].str.replace(',', '.', regex=False)
    
    # Converter para float, depois para int (remover os centavos)
    df_merged['operationTypeId'] = pd.to_numeric(df_merged['operationTypeId'], errors='coerce').astype(int)

    # Filtrar onde 'operationTypeId' é igual a 2
    #df_merged = df_merged[df_merged['operationTypeId'] == 2]
    
    return df_merged


def adjust_data(df):
    # Garantir que clientId e billId sejam tratados como strings, mantendo valores originais
    for coluna in ['clientId', 'billId', 'installmentNumber']:
        if coluna in df.columns:
            df[coluna] = df[coluna].astype(str).str.strip()  # Manter como string e remover espaços extras

    for coluna in df.select_dtypes(include=['float', 'int']).columns:
        if coluna in ['originalAmount', 'correctedBalanceAmount', 'taxAmount']:
            # Formatação para valores monetários com ponto decimal e vírgula como separador de milhar
            df[coluna] = df[coluna].apply(lambda x: f'{x:,.2f}'.replace('.', 'X').replace(',', '.').replace('X', ','))
        else:
            # Formatação para valores monetários padrão
            df[coluna] = df[coluna].apply(lambda x: f'{x:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.'))

    return df

def criar_arquivo_tempo(diretorio, hora_inicio, hora_fim, nome_arquivo="tempo_info.txt"):
    caminho_arquivo = os.path.join(diretorio, nome_arquivo)
    
    # Verifica se o arquivo já existe e o remove se necessário
    if os.path.exists(caminho_arquivo):
        os.remove(caminho_arquivo)
        print(f"Arquivo existente removido: {caminho_arquivo}")
    
    # Calcula o tempo decorrido
    tempo_decorrido = hora_fim - hora_inicio
    horas, resto = divmod(tempo_decorrido.total_seconds(), 3600)
    minutos, segundos = divmod(resto, 60)
    
    # Cria e escreve informações no arquivo
    with open(caminho_arquivo, 'w') as file:
        file.write(f"Hora inicial: {hora_inicio.strftime('%H:%M:%S')}\n")
        file.write(f"Hora final: {hora_fim.strftime('%H:%M:%S')}\n")
        file.write(f"TEMPO DECORRIDO: {int(horas):02}:{int(minutos):02}:{int(segundos):02}\n")
    
    print(f"Arquivo de tempo criado em: {caminho_arquivo}")

def save_historical_data(subdominios, start_year, end_year):
    df_total = pd.DataFrame()
    hora_inicio = datetime.now()
    
    for subdominio in subdominios:
        for year in range(start_year, end_year + 1):
            start_date = f'{year}-01-01'
            end_date = f'{year}-12-31'
            df = process_data(subdominio, start_date, end_date)
            df_total = pd.concat([df_total, df], ignore_index=True)
    
    hora_fim = datetime.now()
    
    if not df_total.empty:
        file_path = r'C:\Bloko Capital\Financeiro - Documentos\Financeiro - Bloko Investimentos\9. BI\BI\Bases_API\RECEBIDAS\dados_historicos.csv'
        df_total.to_csv(file_path, index=False)
        print(f"Dados históricos salvos em: {file_path}")
        
        # Criar arquivo .txt com informações de tempo
        criar_arquivo_tempo(os.path.dirname(file_path), hora_inicio, hora_fim)
    else:
        print("Nenhum dado histórico disponível para salvar.")

def save_current_data(subdominios):
    df_total = pd.DataFrame()
    hora_inicio = datetime.now()
    
    today = datetime.today()
    start_date = f'{today.year}-01-01'
    end_date = today.strftime('%Y-%m-%d')
    
    for subdominio in subdominios:
        df = process_data(subdominio, start_date, end_date)
        df_total = pd.concat([df_total, df], ignore_index=True)
    
    hora_fim = datetime.now()
    
    if not df_total.empty:
        file_path = r'dados_atualizaveis.csv'
        df_total.to_csv(file_path, index=False)
        print(f"Dados atuais salvos em: {file_path}")
        
        # Criar arquivo .txt com informações de tempo
        criar_arquivo_tempo(os.path.dirname(file_path), hora_inicio, hora_fim)
    else:
        print("Nenhum dado atual disponível para salvar.")

# Exemplos de chamada
subdominios = ['sej', 'macapainvest']
#save_historical_data(subdominios, 1994, 2023)
save_current_data(subdominios)
