import requests

# --- API da Sungrow (iSolarCloud) ---
SUNGROW_BASE_URL = "https://gateway.isolarcloud.com.hk"

# --- Credenciais ---
SUNGROW_LOGIN_EMAIL = "Lvengenharia02@gmail.com"
SUNGROW_LOGIN_PASS = "BlueLv@123"

# --- Chaves da API ---
SUNGROW_APPKEY = "62B088F8332864F557F1F74AC56A869E"
SUNGROW_SECRET_KEY = "vtg1dr7cjjkh9qmbkupc22c9grax3bmy"

def obter_token_sungrow():
    """Faz login na API iSolarCloud (Sungrow) e retorna apenas o token."""
    print("Fazendo login na API iSolarCloud (Sungrow)...")

    # Payload do login
    login_payload = {
        "user_account": SUNGROW_LOGIN_EMAIL,
        "user_password": SUNGROW_LOGIN_PASS,
        "appkey": SUNGROW_APPKEY,
        "lang": "_en_US",
        "sys_code": "207"  # código comum para iSolarCloud internacional
    }

    # Cabeçalhos da requisição
    headers_login = {
        "Content-Type": "application/json;charset=UTF-8",
        "sys_code": "207",
        "x-access-key": SUNGROW_SECRET_KEY,
        "x-client-tz": "GMT-3",
        "x-sign-code": "0"
    }

    # Faz a requisição de login
    response = requests.post(f"{SUNGROW_BASE_URL}/openapi/login", json=login_payload, headers=headers_login)

    try:
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print("Erro ao processar resposta:", e)
        print(response.text)
        return None

    # Verifica se o login foi bem-sucedido
    if data.get("result_code") == "1":
        token = data.get("result_data", {}).get("token")
        print(token)
        return token
    else:
        print("Erro no login:", data.get("result_msg"), "| Código:", data.get("result_code"))
        return None

if __name__ == "__main__":
    obter_token_sungrow()
