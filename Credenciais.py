import base64

# Função para obter credenciais de autenticação
def obter_credenciais(subdominio):
    credenciais = {
        "preencha_seu_subdominio": ("preencher_o_usuário", "preencha_senha"),
        "preencha_seu_subdominio": ("preencher_o_usuário", "preencha_senha")
    }
    if subdominio not in credenciais:
        raise ValueError(f"Subdomínio não reconhecido: {subdominio}")

    usuario_api, senha_api = credenciais[subdominio]
    usuario_senha = f'{usuario_api}:{senha_api}'
    token_base64 = base64.b64encode(usuario_senha.encode('utf-8')).decode('utf-8')
    return f'Basic {token_base64}'
