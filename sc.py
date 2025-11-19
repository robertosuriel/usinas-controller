import requests
import json
from datetime import datetime

# --- CONFIGURAÇÕES ---
SUNGROW_BASE_URL = "https://gateway.isolarcloud.com.hk"
SUNGROW_LOGIN_EMAIL = "Lvengenharia02@gmail.com"
SUNGROW_LOGIN_PASS = "BlueLv@123"
SUNGROW_APPKEY = "62B088F8332864F557F1F74AC56A869E"
SUNGROW_SECRET_KEY = "vtg1dr7cjjkh9qmbkupc22c9grax3bmy"

# Código da planta e chave do dispositivo (do seu /openapi/getDeviceList)
SYS_CODE = "1647526_1_1_1"
PS_KEY_LIST = ["1647526_1_1_1"]

# --- FUNÇÃO PARA LOGIN ---
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
        response = requests.post(f"{SUNGROW_BASE_URL}/openapi/login",
                                 json=login_payload,
                                 headers=headers_login,
                                 timeout=20)
        response.raise_for_status()
        data = response.json()

        if data.get('result_code') == '1':
            print("Token da Sungrow obtido com sucesso.\n")
            return data.get('result_data', {}).get('token')
        else:
            print(f"ERRO no login Sungrow: {data.get('result_msg')}")
            return None
    except Exception as e:
        print(f"ERRO HTTP no login Sungrow: {e}")
        return None

# --- FUNÇÃO PARA BUSCAR DADOS ---
def get_device_point_minute_data(token):
    """Consulta os dados de ponto minuto a minuto de um dispositivo."""
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "Authorization": token,
        "x-client-tz": "GMT-3"
    }

    payload = {
        "sys_code": SYS_CODE,
        "ps_key_list": PS_KEY_LIST,
        "points": "p18,p19,p20,p21,p22,p23,p24",
        "start_time_stamp": "20251107000000",
        "end_time_stamp": "20251107235959",
        "minute_interval": 10,
        "is_get_data_acquisition_time": "1"
    }

    print("Enviando requisição para getDevicePointMinuteDataList...\n")

    response = requests.post(f"{SUNGROW_BASE_URL}/openapi/getDevicePointMinuteDataList",
                             json=payload,
                             headers=headers,
                             timeout=30)

    print(f"Status code: {response.status_code}")
    data = response.json()
    print(json.dumps(data, indent=2, ensure_ascii=False))


# --- EXECUÇÃO ---
if __name__ == "__main__":
    token = get_sungrow_token()
    if token:
        get_device_point_minute_data(token)
