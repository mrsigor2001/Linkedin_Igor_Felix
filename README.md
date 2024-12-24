# Extraçôes API_SIENGE

Este repositório contém scripts e ferramentas desenvolvidos para integração e extração de dados da API Sienge. A solução foi projetada para facilitar o consumo de informações relacionadas à gestão de obras e processos financeiros, permitindo automatizar e otimizar a análise de dados.

Funcionalidades
Autenticação: Configuração de credenciais para acesso seguro à API Sienge.
Extração de Dados: Scripts para realizar consultas e armazenar os dados extraídos em arquivos Excel ou bancos de dados.
Filtros Personalizados: Capacidade de personalizar filtros para extração de informações específicas, como cost center, parcelas, e indicadores financeiros.
Automação: Integração com tarefas automatizadas, como envio de e-mails com relatórios em anexo.
Processamento de Dados: Manipulação e limpeza de dados extraídos para atender às necessidades de análise do usuário.
Requisitos
Python 3.8 ou superior
Bibliotecas:
requests
pandas
openpyxl
streamlit (para interface, se aplicável)
Outras especificadas no arquivo requirements.txt.
Configuração
API Base URL: Certifique-se de configurar o endpoint da API com o seguinte formato:

arduino
Copiar código
api.sienge.com.br/{subdominio-do-cliente}/public/api/v1
Substitua {subdominio-do-cliente} pelo subdomínio correspondente.

Credenciais de Acesso: As credenciais de autenticação devem ser configuradas no arquivo .env ou diretamente nos parâmetros do script.

Estrutura do Diretório:

Scripts principais: Localizados na raiz do repositório.
Saída de dados: Dados extraídos são salvos na pasta output.
Uso
Extração Automática:

Execute o script principal para iniciar a extração com base nos parâmetros configurados.
Relatórios gerados são salvos automaticamente na pasta output.
Customizações:

É possível ajustar os filtros e endpoints diretamente no script para atender às necessidades específicas de extração.
Contribuição
Contribuições são bem-vindas! Sinta-se à vontade para abrir issues ou enviar pull requests para melhorias ou correções.
