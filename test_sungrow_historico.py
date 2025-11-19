import requests
import sys
import json
from datetime import datetime, timedelta, date, time as dt_time
import time

# --- CONFIGURAÇÕES ---
SUNGROW_BASE_URL = "https://gateway.isolarcloud.com.hk"
SUNGROW_LOGIN_EMAIL = "Lvengenharia02@gmail.com" 
SUNGROW_LOGIN_PASS = "BlueLv@123"
SUNGROW_APPKEY = "62B088F8332864F557F1F74AC56A869E"
SUNGROW_SECRET_KEY = "vtg1dr7cjjkh9qmbkupc22c9grax3bmy"
# -------------------------

# --- PARÂMETROS DO TESTE ---
# Vamos testar um inversor e um dia que sabemos que tem dados
TEST_PS_KEY = "1682915_1_1_3"
TEST_DATA = "20251105" # 05 de Nov de 2025
TEST_BLOCO_INICIO = "110000" # 11:00:00
TEST_BLOCO_FIM = "115959"   # 11:59:59 (Bloco de 1h)
# -------------------------


def get_sungrow_token():
    """Obtém o token de login da Sungrow."""
    print("Obtendo novo token de login da Sungrow...")
    
    login_payload = {
        "user_account": SUNGROW_LOGIN_EMAIL,
        "user_password": SUNGROW_LOGIN_PASS,
        "appkey": SUNGROW_APPKEY,
        "lang": "_en_US",
        "sys_code": "207" 
    }
    headers_login = {
        'Content-Type': 'application/json;charset=UTF-8',
        'sys_code': '200', 
        'x-access-key': SUNGROW_SECRET_KEY, 
        'x-client-tz': 'GMT-3',
        'x-sign-code': '0'
    }
    
    try:
        response = requests.post(f"{SUNGROW_BASE_URL}/openapi/login", json=login_payload, headers=headers_login, timeout=20)
        response.raise_for_status()
        data = response.json()
        if data.get('result_code') == '1':
            print("Token da Sungrow obtido com sucesso.")
            return data.get('result_data', {}).get('token')
        else:
            print(f"ERRO no login Sungrow: {data.get('result_msg')}")
            return None
    except Exception as e:
        print(f"ERRO HTTP no login Sungrow: {e}")
        return None

def test_endpoint_historico():
    
    token = get_sungrow_token()
    if not token:
        print("Falha no login. Encerrando teste.")
        return

    # --- LÓGICA DE AUTENTICAÇÃO (Baseada no script que funcionou) ---
    headers_api = {
        'Content-Type': 'application/json;charset=UTF-8',
        'sys_code': '200', # TESTE: Usando '200' (do login) em vez de '901'
        'x-access-key': SUNGROW_SECRET_KEY, 
        'x-client-tz': 'GMT-3',
        'x-sign-code': '0'
    }
    base_payload = {
        "appkey": SUNGROW_APPKEY,
        "token": token,
        "lang": "_en_US",
        "sys_code": 207 # TESTE: Usando '207' (do login) no body
    }
    # -----------------------------------------------------------------

    # Prepara o payload para a chamada de histórico
    payload_hist = {
        **base_payload,
        "start_time_stamp": f"{TEST_DATA}{TEST_BLOCO_INICIO}",
        "end_time_stamp": f"{TEST_DATA}{TEST_BLOCO_FIM}",
        "ps_key_list": [TEST_PS_KEY],
        "points": "1,24" # Pedindo apenas Energia do Dia e Potência Ativa
    }

    print("\n--- TESTANDO ENDPOINT: /getDevicePointMinuteDataList ---")
    print(f"URL: {SUNGROW_BASE_URL}/openapi/getDevicePointMinuteDataList")
    print(f"HEADERS: {json.dumps(headers_api, indent=2)}")
    print(f"PAYLOAD: {json.dumps(payload_hist, indent=2)}")
    print("----------------------------------------------------------")

    try:
        response_api = requests.post(
            f"{SUNGROW_BASE_URL}/openapi/getDevicePointMinuteDataList", 
            json=payload_hist, 
            headers=headers_api, 
            timeout=30
        )
        response_api.raise_for_status()
        
        json_data = response_api.json()
        
        print("\n--- RESPOSTA DA API ---")
        print(json.dumps(json_data, indent=2))
        print("-------------------------")

        if json_data.get('result_code') == '1':
            registros = json_data.get('result_data', {}).get(TEST_PS_KEY, [])
            if registros:
                print(f"\n✅ SUCESSO! A API retornou {len(registros)} registros.")
                print("A lógica de autenticação está CORRETA.")
            else:
                print("\n⚠️ ATENÇÃO: A chamada foi bem-sucedida, mas não retornou dados.")
                print("Isso pode significar que não há dados para este horário (11:00-12:00) ou o PS_KEY está errado.")
        else:
            print(f"\n❌ FALHA: A API retornou um erro: {json_data.get('result_msg')}")

    except requests.exceptions.HTTPError as e:
        print(f"\n❌ ERRO HTTP: {e.response.status_code}")
        print(f"Resposta: {e.response.text}")
    except Exception as e:
        print(f"\n❌ ERRO DE EXECUÇÃO: {e}")

# --- Ponto de Entrada Principal do Script ---
if __name__ == "__main__":
    test_endpoint_historico()