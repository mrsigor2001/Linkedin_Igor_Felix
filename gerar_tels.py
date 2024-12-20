import aiohttp
import asyncio
import pandas as pd
import base64
from Credenciais import obter_credenciais



# Função assíncrona para fazer a requisição à API com paginação
async def buscar_clientes(subdominio):
    url = f'https://api.sienge.com.br/{subdominio}/public/api/v1/customers'
    token = obter_credenciais(subdominio)
    headers = {
        'Authorization': token
    }
    limit = 200  # Número máximo de registros por página
    offset = 0
    all_data = []

    while True:
        params = {
            'limit': limit,
            'offset': offset
        }

        # Imprime que uma requisição está sendo feita
        print(f"Fazendo requisição para {subdominio} - Offset: {offset}")

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, params=params) as response:
                    response.raise_for_status()  # Lança uma exceção para erros HTTP
                    data = await response.json()

                    # Verifica a estrutura dos dados
                    if isinstance(data, dict) and 'results' in data:
                        results = data['results']
                    elif isinstance(data, list):
                        results = data
                    else:
                        raise ValueError("Estrutura de dados inesperada.")

                    if not results:
                        break  # Sai do loop se não houver mais resultados

                    # Adiciona o subdomínio à cada registro de cliente
                    for cliente in results:
                        cliente['subdominio'] = subdominio

                    all_data.extend(results)
                    offset += limit  # Atualiza o offset para a próxima página

        except aiohttp.ClientError as e:
            print(f"Erro na requisição para {subdominio}: {e}")
            break

    return all_data

# Função principal assíncrona para buscar clientes de múltiplos subdomínios
async def main():
    subdominios = ['macapainvest', 'sej']  # Subdomínios desejados
    df_clientes = []

    tasks = []  # Lista para armazenar tarefas assíncronas
    for subdominio in subdominios:
        tasks.append(buscar_clientes(subdominio))

    # Aguarda todas as tarefas assíncronas
    results = await asyncio.gather(*tasks)

    # Processa os resultados e cria o DataFrame
    for clientes in results:
        if clientes:
            df_temp = pd.DataFrame(clientes)
            df_clientes.append(df_temp)

    # Concatenar todos os DataFrames
    if df_clientes:
        df_final = pd.concat(df_clientes, ignore_index=True)

        # Adicionar coluna 'indexador_unico'
        df_final['indexador_unico'] = df_final.index + 1  # Adiciona o indexador começando de 1

        # Expandir e tratar colunas que são listas
        colunas_para_expandir = ['phones']

        for col in colunas_para_expandir:
            if col in df_final.columns:
                df_temp = df_final[['indexador_unico', col]].copy()
                df_temp[col] = df_temp.apply(lambda row: [{'indexador_unico': row['indexador_unico'], **item} for item in row[col]] if isinstance(row[col], list) else row[col], axis=1)
                expanded_df = pd.json_normalize(df_temp.explode(col)[col])
                expanded_df = expanded_df.dropna(subset=['indexador_unico'])
                expanded_df['indexador_unico'] = expanded_df['indexador_unico'].astype(int)
                df_final = df_final.drop(columns=[col])
                df_final = pd.merge(df_final, expanded_df, on='indexador_unico', how='left')
                df_final = df_final.drop(columns=['addresses', 'procurators', 'contacts', 'spouse', 'familyIncome'])

        print("DataFrame final dos clientes:")
    else:
        print("Nenhum cliente foi buscado.")

    # Salvar DataFrame em CSV
    df_final.to_csv('clientes.csv', index=False, sep=';')

# Rodar a função principal
if __name__ == '__main__':
    asyncio.run(main())