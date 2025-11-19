import psycopg2
import requests
import sys
import json
from datetime import datetime

# --- CONFIGURAÇÕES ---
DB_HOST = "db-usinas.c54mquckeem4.us-east-2.rds.amazonaws.com"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASS = "$Uriel171217"
FRONIUS_ACCESS_KEY_ID = "FKIABDA70962868A47488C9D7649E64DF0C1"
FRONIUS_ACCESS_KEY_VALUE = "d2cce829-d736-45bc-9da2-2071d56bfaeb"
FRONIUS_BASE_URL = "https://api.solarweb.com/swqapi" 
SUNGROW_BASE_URL = "https://gateway.isolarcloud.com.hk"
SUNGROW_LOGIN_EMAIL = "Lvengenharia02@gmail.com" 
SUNGROW_LOGIN_PASS = "BlueLv@123"
SUNGROW_APPKEY = "62B088F8332864F557F1F74AC56A869E"
SUNGROW_SECRET_KEY = "vtg1dr7cjjkh9qmbkupc22c9grax3bmy"
# -------------------------

def get_db_connection():
    try:
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
        return conn
    except Exception as e:
        print(f"ERRO de Conexão com DB: {e}")
        sys.exit(1)

def get_sungrow_token():
    """Obtém o token de login da Sungrow."""
    print("Obtendo token da Sungrow...")
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
        response = requests.post(f"{SUNGROW_BASE_URL}/openapi/login", json=login_payload, headers=headers_login, timeout=10)
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

def run_update_dates():
    conn = get_db_connection()
    if not conn:
        return

    try:
        with conn.cursor() as cur:
            # --- Processo da Fronius ---
            print("\nIniciando correção de datas da Fronius...")
            headers_fronius = {"accept": "application/json", "AccessKeyId": FRONIUS_ACCESS_KEY_ID, "AccessKeyValue": FRONIUS_ACCESS_KEY_VALUE}
            cur.execute("SELECT i.id_api_inversor, u.id_api_usina FROM tbl_inversores i JOIN tbl_usinas u ON i.id_usina = u.id_usina WHERE u.fabricante_api = 'Fronius'")
            inversores_fronius = cur.fetchall()
            
            for inv_api_id, usina_api_id in inversores_fronius:
                try:
                    url = f"{FRONIUS_BASE_URL}/pvsystems/{usina_api_id}/devices"
                    response = requests.get(url, headers=headers_fronius, timeout=10)
                    response.raise_for_status()
                    devices = response.json().get('devices', [])
                    for device in devices:
                        if device.get('deviceId') == inv_api_id:
                            activation_date_str = device.get('activationDate')
                            if activation_date_str:
                                activation_date = datetime.fromisoformat(activation_date_str.replace('Z', '+00:00')).date()
                                cur.execute("UPDATE tbl_inversores SET data_ativacao = %s WHERE id_api_inversor = %s", (activation_date, inv_api_id))
                                print(f"  -> Fronius {inv_api_id} atualizado para {activation_date}")
                except Exception as e:
                    print(f"  -> ERRO ao buscar dados do inversor Fronius {inv_api_id}: {e}")

            # --- Processo da Sungrow ---
            print("\nIniciando correção de datas da Sungrow...")
            token_sungrow = get_sungrow_token()
            if token_sungrow:
                headers_sungrow = {'Content-Type': 'application/json;charset=UTF-8', 'sys_code': '200', 'x-access-key': SUNGROW_SECRET_KEY, 'x-client-tz': 'GMT-3', 'x-sign-code': '0'}
                base_payload_sungrow = {"appkey": SUNGROW_APPKEY, "token": token_sungrow, "lang": "_en_US", "sys_code": 207}
                
                cur.execute("SELECT i.id_api_inversor, u.id_api_usina FROM tbl_inversores i JOIN tbl_usinas u ON i.id_usina = u.id_usina WHERE u.fabricante_api = 'Sungrow'")
                inversores_sungrow = cur.fetchall()

                for inv_api_id, usina_api_id in inversores_sungrow:
                    try:
                        payload = {**base_payload_sungrow, "curPage": 1, "size": 10, "ps_id": int(usina_api_id), "device_type_list": [1]}
                        response = requests.post(f"{SUNGROW_BASE_URL}/openapi/getDeviceList", json=payload, headers=headers_sungrow, timeout=10)
                        response.raise_for_status()
                        devices = response.json().get('result_data', {}).get('pageList', [])
                        for device in devices:
                            if device.get('ps_key') == inv_api_id:
                                rel_time_str = device.get('rel_time') # Ex: "2025-09-29 00:32:11"
                                if rel_time_str:
                                    activation_date = datetime.strptime(rel_time_str, '%Y-%m-%d %H:%M:%S').date()
                                    cur.execute("UPDATE tbl_inversores SET data_ativacao = %s WHERE id_api_inversor = %s", (activation_date, inv_api_id))
                                    print(f"  -> Sungrow {inv_api_id} atualizado para {activation_date}")
                    except Exception as e:
                        print(f"  -> ERRO ao buscar dados do inversor Sungrow {inv_api_id}: {e}")

            conn.commit()
            print("\n--- Correção de datas concluída! ---")

    except Exception as e:
        print(f"ERRO CRÍTICO no script de correção de datas: {e}")
        conn.rollback()
    finally:
        if conn:
            conn.close()
            print("Conexão com o banco de dados fechada.")

if __name__ == "__main__":
    run_update_dates()