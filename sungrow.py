import requests
import json
from datetime import datetime, timedelta

# --- CONFIGURAÇÕES ---
SUNGROW_BASE_URL = "https://gateway.isolarcloud.com.hk"
SUNGROW_LOGIN_EMAIL = "Lvengenharia02@gmail.com"
SUNGROW_LOGIN_PASS = "BlueLv@123"
SUNGROW_APPKEY = "62B088F8332864F557F1F74AC56A869E"
SUNGROW_SECRET_KEY = "vtg1dr7cjjkh9qmbkupc22c9grax3bmy"

# --- FUNÇÃO 1: LOGIN ---
def get_sungrow_token():
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

    r = requests.post(f"{SUNGROW_BASE_URL}/openapi/login", json=login_payload, headers=headers_login)
    r.raise_for_status()
    data = r.json()
    if data.get('result_code') != '1':
        raise Exception(f"Erro no login: {data}")
    token = data['result_data']['token']
    print(f"✅ Token obtido com sucesso: {token[:15]}...")
    return token

# --- FUNÇÃO 2: LISTAR USINAS ---
def get_power_stations(token):
    payload = {
        "appkey": SUNGROW_APPKEY,
        "token": token,
        "lang": "_en_US",
        "sys_code": 207,
        "curPage": 1,
        "size": 50
    }
    headers = {
        'Content-Type': 'application/json;charset=UTF-8',
        'sys_code': '200',
        'x-access-key': SUNGROW_SECRET_KEY,
        'x-client-tz': 'GMT-3',
        'x-sign-code': '0'
    }

    r = requests.post(f"{SUNGROW_BASE_URL}/openapi/getPowerStationList", json=payload, headers=headers)
    r.raise_for_status()
    data = r.json()
    if data.get('result_code') != '1':
        raise Exception(f"Erro ao listar usinas: {data}")

    usinas = data['result_data']['pageList']
    print(f"🔋 Encontradas {len(usinas)} usinas:")
    for u in usinas:
        print(f" - {u['ps_name']} (ps_id={u['ps_id']})")
    return usinas

# --- FUNÇÃO 3: LISTAR INVERSORES (DEVICES) ---
def get_devices(token, ps_id):
    url = "https://gateway.isolarcloud.com.hk/openapi/getDeviceList"
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "x-access-key": SUNGROW_SECRET_KEY,
    }

    payload = {
        "appkey": SUNGROW_APPKEY,
        "token": token,
        "sys_code": 207,
        "lang": "_en_US",
        "curPage": 1,
        "size": 50,
        "ps_id": ps_id,
        "device_type_list": [1]  # 1 = inversores
    }

    r = requests.post(url, json=payload, headers=headers)
    r.raise_for_status()
    data = r.json()

    if data.get("result_code") == "1" and data.get("result_data"):
        devices = data["result_data"]["pageList"]
        print(f"🔌 {len(devices)} devices encontrados para ps_id={ps_id}:")
        for d in devices:
            print(f"   - {d['device_name']} | ps_key={d['ps_key']} | modelo={d['device_model_code']}")
        return devices
    else:
        raise Exception(f"Erro ao listar devices: {data}")

# --- FUNÇÃO 4: OBTER DADOS MINUTO A MINUTO EM INTERVALOS DE 3H ---
def get_device_point_data_3h(token, ps_key, date_str="20251107"):
    """
    Obtém dados minuto a minuto dividindo o dia em blocos de 3 horas.
    date_str: "YYYYMMDD"
    """
    resultados = []
    dia = datetime.strptime(date_str, "%Y%m%d")

    for h in range(0, 24, 3):
        start_time = dia + timedelta(hours=h)
        end_time = dia + timedelta(hours=h+3) - timedelta(seconds=1)

        start_stamp = start_time.strftime("%Y%m%d%H%M%S")
        end_stamp = end_time.strftime("%Y%m%d%H%M%S")

        payload = {
            "appkey": SUNGROW_APPKEY,
            "token": token,
            "lang": "_en_US",
            "sys_code": 207,
            "start_time_stamp": start_stamp,
            "end_time_stamp": end_stamp,
            "minute_interval": 5,
            "is_get_data_acquisition_time": "1",
            "points": "p5,p6,p7,p8,p9,p10,p18,p19,p20,p21,p22,p23,p24,p45,p46,p47,p48,p49,p50,p51,p52,p53,p54",
            "ps_key_list": [ps_key]
        }

        headers = {
            'Content-Type': 'application/json;charset=UTF-8',
            'sys_code': '200',
            'x-access-key': SUNGROW_SECRET_KEY,
            'x-client-tz': 'GMT-3',
            'x-sign-code': '0'
        }

        r = requests.post(f"{SUNGROW_BASE_URL}/openapi/getDevicePointMinuteDataList", json=payload, headers=headers)
        r.raise_for_status()
        data = r.json()
        if data.get("result_code") == "1" and data.get("result_data"):
            resultados.append(data["result_data"])
        else:
            print(f"⚠️ Sem dados para o intervalo {start_stamp} - {end_stamp}")

    return resultados

# --- EXECUÇÃO PRINCIPAL ---
if __name__ == "__main__":
    token = get_sungrow_token()
    usinas = get_power_stations(token)

    resultados = []

    for usina in usinas:
        ps_id = usina['ps_id']
        nome = usina['ps_name']

        devices = get_devices(token, ps_id)
        for d in devices:
            ps_key = d['ps_key']
            print(f"📡 Buscando dados do inversor {ps_key} ({nome})...")
            dados = get_device_point_data_3h(token, ps_key, date_str="20251107")
            resultados.append({
                "usina": nome,
                "ps_id": ps_id,
                "ps_key": ps_key,
                "dados": dados
            })

    # --- SALVAR RESULTADO EM JSON ---
    filename = f"dados_usinas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(resultados, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Dados salvos em {filename}")
