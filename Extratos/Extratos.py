import asyncio
import aiohttp
import base64
import pandas as pd
from datetime import datetime
import nest_asyncio

# Permitir loops aninhados no Jupyter
nest_asyncio.apply()

# Função para obter as credenciais de autenticação para o subdomínio
def obter_credenciais(subdominio):
    credenciais = {
        "sej": ("sej-gvapi", "yfuHc8SwmjFzaErWwSDYaRyMPo2ZJkdm"),
        "macapainvest": ("macapainvest-igorsantos", "QKQjlS3D3FFalzKNrOopOtJKTMbjiuYZ")
    }
    if subdominio not in credenciais:
        raise ValueError(f"Subdomínio não reconhecido: {subdominio}")

    usuario_api, senha_api = credenciais[subdominio]
    usuario_senha = f'{usuario_api}:{senha_api}'
    token_base64 = base64.b64encode(usuario_senha.encode('utf-8')).decode('utf-8')
    return f'Basic {token_base64}'

# Função para obter os dados do extrato do cliente via API
async def obter_dados_do_extrato(subdominio, start_due_date, end_due_date, semaphore, bill_receivable_id=None):
    url = f"https://api.sienge.com.br/{subdominio}/public/api/bulk-data/v1/customer-extract-history"
    
    params = {
        "startDueDate": start_due_date,
        "endDueDate": end_due_date,
    }
    
    if bill_receivable_id:
        params["billReceivableId"] = bill_receivable_id
    
    headers = {
        "Authorization": obter_credenciais(subdominio)
    }

    max_retries = 3
    attempt = 0

    while attempt < max_retries:
        attempt += 1
        try:
            async with semaphore:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, headers=headers, params=params) as response:
                        if response.status == 200:
                            print(f"✅ Dados extraídos com sucesso de {subdominio} {start_due_date} a {end_due_date}.")
                            return await response.json()
                        else:
                            raise Exception(f"Erro: {response.status}, {await response.text()}")
        except Exception as e:
            print(f"⚠️ Tentativa {attempt} falhou para {subdominio} {start_due_date} a {end_due_date}: {e}")
            if attempt == max_retries:
                print(f"❌ Falha após {max_retries} tentativas para {subdominio} {start_due_date} a {end_due_date}.")
                raise

# Função para converter os dados extraídos para um DataFrame
def converter_para_dataframe(dados):
    extrato_cliente = []
    
    for item in dados.get('data', []):
        # Desestruturando os campos do JSON
        company = item.get('company', {})
        cost_center = item.get('costCenter', {})
        customer = item.get('customer', {})
        units = item.get('units', [])
        installments = item.get('installments', [])
        
        # Extraindo dados principais
        row = {
            'billReceivableId': item.get('billReceivableId'),
            'company_id': company.get('id'),
            'company_name': company.get('name'),
            'costCenter_id': cost_center.get('id'),
            'costCenter_name': cost_center.get('name'),
            'customer_id': customer.get('id'),
            'customer_name': customer.get('name'),
            'customer_document': customer.get('document'),
            'emissionDate': item.get('emissionDate'),
            'lastRenegotiationDate': item.get('lastRenegotiationDate'),
            'correctionDate': item.get('correctionDate'),
            'document': item.get('document'),
            'privateArea': item.get('privateArea'),
            'oldestInstallmentDate': item.get('oldestInstallmentDate'),
            'revokedBillReceivableDate': item.get('revokedBillReceivableDate')
        }
        
        # Extraindo informações de unidades (usando a primeira unidade, caso existam múltiplas)
        if units:
            row['unit_id'] = units[0].get('id')
            row['unit_name'] = units[0].get('name')
        else:
            row['unit_id'] = None
            row['unit_name'] = None
        
        # Processando os dados das parcelas (installments)
        for installment in installments:
            installment_row = row.copy()
            installment_row.update({
                'installment_id': installment.get('id'),
                'annualCorrection': installment.get('annualCorrection'),
                'sentToScripturalCharge': installment.get('sentToScripturalCharge'),
                'paymentTerms_id': installment.get('paymentTerms', {}).get('id'),
                'paymentTerms_description': installment.get('paymentTerms', {}).get('description'),
                'baseDate': installment.get('baseDate'),
                'originalValue': installment.get('originalValue'),
                'dueDate': installment.get('dueDate'),
                'indexerId': installment.get('indexerId'),
                'calculationDate': installment.get('calculationDate'),
                'currentBalance': installment.get('currentBalance'),
                'currentBalanceWithAddition': installment.get('currentBalanceWithAddition'),
                'generatedBillet': installment.get('generatedBillet'),
                'installmentSituation': installment.get('installmentSituation'),
                'installmentNumber': installment.get('installmentNumber')
            })
            
            # Processando os recibos dentro das parcelas (receipts)
            receipts = installment.get('receipts', [])
            for receipt in receipts:
                receipt_row = installment_row.copy()
                receipt_row.update({
                    'receipt_days': receipt.get('days'),
                    'receipt_date': receipt.get('date'),
                    'receipt_value': receipt.get('value'),
                    'receipt_extra': receipt.get('extra'),
                    'receipt_discount': receipt.get('discount'),
                    'receipt_netReceipt': receipt.get('netReceipt'),
                    'receipt_type': receipt.get('type')
                })
                extrato_cliente.append(receipt_row)
        # Se não houver parcelas ou recibos, adiciona a linha principal
        if not installments or not receipts:
            extrato_cliente.append(row)
    
    return pd.DataFrame(extrato_cliente)

# Função assíncrona para obter dados de forma eficiente
async def obter_dados_assincronos(subdominio, start_date, end_date, bill_receivable_id=None, max_concurrent_requests=5):
    start_year = datetime.strptime(start_date, "%Y-%m-%d").year
    end_year = datetime.strptime(end_date, "%Y-%m-%d").year
    tasks = []
    semaphore = asyncio.Semaphore(max_concurrent_requests)

    # Criação de intervalos de 5 anos
    for year in range(start_year, end_year + 1, 5):
        start_due_date = f"{year}-01-01"
        end_due_date = f"{min(year + 4, end_year)}-12-31"  # Garante que não ultrapasse o end_year
        tasks.append(obter_dados_do_extrato(subdominio, start_due_date, end_due_date, semaphore, bill_receivable_id))

    # Executa as tarefas de forma assíncrona
    results = await asyncio.gather(*tasks)

    combined_data = []
    for result in results:
        if result:
            combined_data.extend(result.get('data', []))
    
    return converter_para_dataframe({'data': combined_data})

# Função principal para orquestrar o processo
async def main(subdominios, bill_receivable_id=None):
    start_date = '1990-01-01'
    end_date = '2100-12-31'

    print(f"⏳ Iniciando extração de dados para os subdomínios: {', '.join(subdominios)}")
    
    # Executar as extrações de ambos os subdomínios de forma assíncrona
    tasks = [obter_dados_assincronos(subdominio, start_date, end_date, bill_receivable_id) for subdominio in subdominios]
    results = await asyncio.gather(*tasks)

    # Combinar os dados dos subdomínios em um único DataFrame
    combined_df = pd.concat(results, ignore_index=True)

    # Salvando os dados extraídos em um arquivo CSV
    combined_df.to_csv('Extratos_combined.csv', index=False)
    print(f"✅ Dados salvos no arquivo 'Extratos_combined.csv'.")
    print(f"📊 Total de registros salvos: {len(combined_df)}")

# Execução do script para ambos os subdomínios
if __name__ == "__main__":
    subdominios = ["sej"]  # Lista de subdomínios

    # Executando o processo para ambos os subdomínios
    asyncio.run(main(subdominios))
