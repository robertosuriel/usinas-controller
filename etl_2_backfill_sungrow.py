import psycopg2
import psycopg2.extras 
import requests
import sys
import json
import pytz 
from datetime import datetime, timedelta, date, time as dt_time
import time

# --- CONFIGURAÇÕES ---
# (Suas configurações, que sabemos que funcionam)
DB_HOST = "db-usinas.c54mquckeem4.us-east-2.rds.amazonaws.com"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASS = "$Uriel171217"

SUNGROW_BASE_URL = "https://gateway.isolarcloud.com.hk"
SUNGROW_LOGIN_EMAIL = "Lvengenharia02@gmail.com" 
SUNGROW_LOGIN_PASS = "BlueLv@123"
SUNGROW_APPKEY = "62B088F8332864F557F1F74AC56A869E"
SUNGROW_SECRET_KEY = "vtg1dr7cjjkh9qmbkupc22c9grax3bmy"
# -------------------------

# --- CONSTANTES ---
# Pontos de medição que queremos buscar (baseado no seu script de teste)
POINTS_DE_INTERESSE = "p5,p6,p7,p8,p9,p10,p18,p19,p20,p21,p22,p23,p24,p45,p46,p47,p48,p49,p50,p51,p52,p53,p54,p1,p43,p25,p26"

# Blocos de 3 horas para o loop da API da Sungrow
BLOCOS_DE_3H = [
    ("000000", "025959"), ("030000", "055959"), ("060000", "085959"), ("090000", "115959"),
    ("120000", "145959"), ("150000", "175959"), ("180000", "205959"), ("210000", "235959")
]
# -------------------------

def get_db_connection():
    """Conecta ao banco de dados PostgreSQL no RDS."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS
        )
        return conn
    except Exception as e:
        print(f"ERRO: Não foi possível conectar ao banco de dados: {e}")
        return None

def get_sungrow_token():
    """Obtém o token de login da Sungrow (lógica do seu script)."""
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

def get_inverters_from_db():
    """Busca a lista de inversores Sungrow para processar."""
    print("Buscando inversores Sungrow no banco de dados...")
    conn = None
    inversores = []
    try:
        conn = get_db_connection()
        if not conn:
            return []
        
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 
                    i.id_inversor, 
                    i.id_api_inversor, -- Este é o 'ps_key'
                    u.id_api_usina,   -- Este é o 'ps_id'
                    i.data_ativacao,
                    u.fuso_horario
                FROM tbl_inversores i
                JOIN tbl_usinas u ON i.id_usina = u.id_usina
                WHERE u.fabricante_api = 'Sungrow'; 
                """
            )
            inversores = cur.fetchall()
            print(f"Encontrados {len(inversores)} inversores Sungrow.")
            
    except Exception as e:
        print(f"ERRO ao buscar lista de inversores: {e}")
    finally:
        if conn:
            conn.close()
    return inversores

def safe_float(value):
    """Converte valores para float com segurança, tratando None e erros."""
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def run_sungrow_backfill():
    """
    Executa o backfill para todos os inversores Sungrow.
    """
    print("\n--- INICIANDO BACKFILL HISTÓRICO - SUNGROW (Final) ---")
    
    inversores = get_inverters_from_db()
    if not inversores:
        print("Nenhum inversor para processar. Encerrando.")
        return

    token = get_sungrow_token()
    if not token:
        print("Não foi possível obter o token da Sungrow. Encerrando.")
        return

    # --- LÓGICA DE HEADERS E PAYLOAD (Baseada no seu script funcional) ---
    headers_api = {
        'Content-Type': 'application/json;charset=UTF-8',
        'sys_code': '200', 
        'x-access-key': SUNGROW_SECRET_KEY, 
        'x-client-tz': 'GMT-3',
        'x-sign-code': '0'
    }
    base_payload = {
        "appkey": SUNGROW_APPKEY,
        "token": token,
        "lang": "_en_US",
        "sys_code": 207
    }
    # -----------------------------------------------------------------

    # Loop principal por cada inversor
    for inv in inversores:
        id_inversor_db, api_ps_key, api_ps_id, dt_ativacao, fuso_horario = inv
        
        print(f"\n--- Processando Inversor ID: {id_inversor_db} (PS_Key: {api_ps_key}) ---")

        if not dt_ativacao:
            print(f"AVISO: 'data_ativacao' está NULA. Pulando este inversor.")
            continue 
        
        if not fuso_horario: fuso_horario = 'GMT-3' # Fallback
        
        try:
            tz_local = pytz.timezone('America/Sao_Paulo' if fuso_horario == 'GMT-3' else fuso_horario)
        except pytz.exceptions.UnknownTimeZoneError:
            tz_local = pytz.timezone('America/Sao_Paulo')

        data_fim = date.today() - timedelta(days=1)
        data_atual = data_fim
        
        # Loop por cada DIA (de ontem até a data de ativação)
        while data_atual >= dt_ativacao:
            print(f"Processando dia: {data_atual.strftime('%Y-%m-%d')}")
            
            conn = None
            try:
                conn = get_db_connection()
                if not conn:
                    print("  -> ERRO: Não foi possível conectar ao DB. Pulando este dia.")
                    time.sleep(5) 
                    data_atual -= timedelta(days=1)
                    continue

                with conn.cursor() as cur:
                    # Verifica se o dia já existe, para pular
                    cur.execute(
                        """
                        SELECT 1 FROM tbl_leituras 
                        WHERE id_inversor = %s AND timestamp_utc::date = %s
                        LIMIT 1;
                        """,
                        (id_inversor_db, data_atual)
                    )
                    if cur.fetchone():
                        print(f"  -> Dados para {data_atual} já existem. Pulando.")
                        data_atual -= timedelta(days=1)
                        continue 
                    
                    todos_os_registros_do_dia = []
                    
                    # Loop por cada BLOCO DE 3 HORAS
                    for i, (bloco_inicio, bloco_fim) in enumerate(BLOCOS_DE_3H):
                        data_str = data_atual.strftime('%Y%m%d')
                        start_time_str = f"{data_str}{bloco_inicio}"
                        end_time_str = f"{data_str}{bloco_fim}"
                        
                        payload_hist = {
                            **base_payload,
                            "start_time_stamp": start_time_str,
                            "end_time_stamp": end_time_str,
                            "ps_key_list": [api_ps_key],
                            "points": POINTS_DE_INTERESSE,
                            "minute_interval": 5, # Garantindo que é 5 min
                            "is_get_data_acquisition_time": "1"
                        }
                        
                        print(f"  -> Bloco {i+1}/8 ({bloco_inicio}-{bloco_fim})...", end='')
                        
                        response_api = requests.post(f"{SUNGROW_BASE_URL}/openapi/getDevicePointMinuteDataList", json=payload_hist, headers=headers_api, timeout=30)
                        response_api.raise_for_status()
                        
                        json_data = response_api.json()
                        if json_data.get('result_code') != '1':
                            print(f" ERRO API Sungrow: {json_data.get('result_msg')}")
                            if json_data.get('result_code') == 'E904': # Token expirado
                                print("Token expirado. Obtendo um novo...")
                                token = get_sungrow_token()
                                if not token:
                                    print("Falha ao renovar token. Encerrando script.")
                                    sys.exit(1) # Encerra o script
                                base_payload['token'] = token # Atualiza o token
                                payload_hist['token'] = token
                                print("Repetindo a chamada do bloco...")
                                response_api = requests.post(f"{SUNGROW_BASE_URL}/openapi/getDevicePointMinuteDataList", json=payload_hist, headers=headers_api, timeout=30)
                                response_api.raise_for_status()
                                json_data = response_api.json()
                        
                        if json_data.get('result_code') == '1':
                            registros = json_data.get('result_data', {}).get(api_ps_key, [])
                            if registros:
                                print(f" {len(registros)} registros encontrados.")
                                todos_os_registros_do_dia.extend(registros)
                            else:
                                print(" Sem dados.")
                        
                        time.sleep(2) # CONTROLE DE LIMITE DA API

                    if not todos_os_registros_do_dia:
                        data_atual -= timedelta(days=1)
                        continue
                    
                    todos_os_registros_do_dia.sort(key=lambda x: x['time_stamp'])
                    
                    data_to_insert = []
                    p1_anterior = 0.0 
                    
                    for record in todos_os_registros_do_dia:
                        try:
                            ts_local_str = record.get('time_stamp')
                            ts_local_dt = datetime.strptime(ts_local_str, '%Y%m%d%H%M%S')
                            ts_local_aware = tz_local.localize(ts_local_dt)
                            ts_utc = ts_local_aware.astimezone(pytz.utc)
                        except Exception as e:
                            print(f"  -> Erro ao converter timestamp {ts_local_str}: {e}. Pulando registro.")
                            continue
                        
                        try:
                            p1_atual = float(record.get('p1', 0.0))
                        except (ValueError, TypeError):
                            p1_atual = p1_anterior 

                        energia_intervalo_wh = 0.0
                        if p1_anterior > 0 and p1_atual >= p1_anterior:
                            energia_intervalo_wh = p1_atual - p1_anterior
                        
                        p1_anterior = p1_atual 

                        row_tuple = (
                            ts_utc,
                            id_inversor_db,
                            safe_float(record.get('p24')) / 1000.0 if record.get('p24') is not None else None, 
                            safe_float(record.get('p43')), 
                            safe_float(record.get('p25')), 
                            safe_float(record.get('p26')), 
                            energia_intervalo_wh,
                            safe_float(record.get('p5')),  
                            safe_float(record.get('p6')),  
                            safe_float(record.get('p7')),  
                            safe_float(record.get('p8')),  
                            safe_float(record.get('p18')), 
                            safe_float(record.get('p19')), 
                            safe_float(record.get('p20')), 
                            safe_float(record.get('p21')), 
                            safe_float(record.get('p22')), 
                            safe_float(record.get('p23')), 
                        )
                        data_to_insert.append(row_tuple)

                    if data_to_insert:
                        sql_insert = """
                        INSERT INTO tbl_leituras (
                            timestamp_utc, id_inversor, 
                            potencia_ativa_kw, potencia_aparente_va, potencia_reativa_var, fator_potencia,
                            energia_intervalo_wh,
                            tensao_dc_mppt1, corrente_dc_mppt1,
                            tensao_dc_mppt2, corrente_dc_mppt2,
                            tensao_ac_fase_a, tensao_ac_fase_b, tensao_ac_fase_c,
                            corrente_ac_fase_a, corrente_ac_fase_b, corrente_ac_fase_c
                        ) VALUES %s
                        ON CONFLICT (timestamp_utc, id_inversor) DO NOTHING;
                        """
                        
                        psycopg2.extras.execute_values(cur, sql_insert, data_to_insert)
                        conn.commit()
                        print(f"  -> Dia {data_atual} concluído. {len(data_to_insert)} registros inseridos/verificados.")

            except requests.exceptions.HTTPError as e:
                print(f"  -> ERRO na API ({e.response.status_code}) ao buscar dia {data_atual}: {e.response.text}. Pulando este dia.")
                if conn: conn.rollback() 
            except requests.exceptions.RequestException as e:
                print(f"  -> ERRO de Conexão ao buscar dia {data_atual}: {e}. Pulando este dia.")
                if conn: conn.rollback()
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as db_err:
                print(f"  -> ERRO de Banco de Dados no dia {data_atual}: {db_err}. Pulando este dia.")
            except Exception as e:
                print(f"  -> ERRO no processamento do dia {data_atual}: {e}. Pulando.")
                if conn: conn.rollback() 
            finally:
                if conn:
                    conn.close() 
            
            data_atual -= timedelta(days=1)
    
    print("\n--- Backfill da Sungrow concluído. ---")


# --- Ponto de Entrada Principal do Script ---
if __name__ == "__main__":
    run_sungrow_backfill()