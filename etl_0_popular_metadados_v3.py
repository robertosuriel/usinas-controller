import psycopg2
import requests
import sys
import json
import re

# --- CONFIGURAÇÕES ---
# Preencha todos os campos com seus dados reais

# --- Banco de Dados (AWS RDS) ---
DB_HOST = "db-usinas.c54mquckeem4.us-east-2.rds.amazonaws.com"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASS = "$Uriel171217"

# --- API da Fronius (Solar.web) ---
FRONIUS_ACCESS_KEY_ID = "FKIABDA70962868A47488C9D7649E64DF0C1"
FRONIUS_ACCESS_KEY_VALUE = "d2cce829-d736-45bc-9da2-2071d56bfaeb"
FRONIUS_BASE_URL = "https://api.solarweb.com/swqapi" 

# --- API da Sungrow (iSolarCloud) ---
SUNGROW_BASE_URL = "https://gateway.isolarcloud.com.hk"

# --- Login (baseado na sua depuração) ---
SUNGROW_LOGIN_EMAIL = "Lvengenharia02@gmail.com" 
SUNGROW_LOGIN_PASS = "BlueLv@123"

# --- Chaves da API (baseado na sua depuração) ---
SUNGROW_APPKEY = "62B088F8332864F557F1F74AC56A869E"     # Para payload (body)
SUNGROW_SECRET_KEY = "vtg1dr7cjjkh9qmbkupc22c9grax3bmy" # Para x-access-key (header)
# -------------------------

def get_db_connection():
    """Conecta ao banco de dados PostgreSQL no RDS."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        print("Conexão com AWS RDS (PostgreSQL) estabelecida.")
        return conn
    except Exception as e:
        print(f"ERRO: Não foi possível conectar ao banco de dados: {e}")
        sys.exit(1)

def popular_usinas_fronius(conn):
    """Busca dados da Fronius e insere no banco."""
    print("\n--- Iniciando Job: Fronius ---")
    
    headers = {
        "accept": "application/json",
        "AccessKeyId": FRONIUS_ACCESS_KEY_ID,
        "AccessKeyValue": FRONIUS_ACCESS_KEY_VALUE
    }
    
    try:
        # 1. Buscar a lista de Usinas (PV Systems)
        print("Buscando lista de usinas Fronius...")
        response_usinas = requests.get(f"{FRONIUS_BASE_URL}/pvsystems", headers=headers)
        response_usinas.raise_for_status() 
        
        usinas = response_usinas.json().get('pvSystems', [])

        if not usinas:
            print("Nenhuma usina Fronius encontrada.")
            return

        with conn.cursor() as cur:
            for usina in usinas:
                pv_system_id = usina.get('pvSystemId')
                print(f"Processando Usina Fronius: {usina.get('name')} (ID: {pv_system_id})")
                
                cur.execute("SELECT id_usina FROM tbl_usinas WHERE id_api_usina = %s", (pv_system_id,))
                resultado = cur.fetchone()
                id_usina_no_banco = None

                if resultado:
                    id_usina_no_banco = resultado[0]
                    print(f"Usina já existe no banco. ID: {id_usina_no_banco}. (Pulando inserção)")
                else:
                    cur.execute(
                        """
                        INSERT INTO tbl_usinas (nome_usina, fabricante_api, id_api_usina, localizacao, potencia_pico_kwp, data_comissionamento, fuso_horario)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        RETURNING id_usina;
                        """,
                        (
                            usina.get('name'),
                            'Fronius',
                            pv_system_id,
                            usina.get('address', {}).get('city'),
                            usina.get('peakPower') / 1000.0 if usina.get('peakPower') else None,
                            usina.get('installationDate'),
                            usina.get('timeZone')
                        )
                    )
                    id_usina_no_banco = cur.fetchone()[0]
                    print(f"Usina inserida no banco com id_usina: {id_usina_no_banco}")


                # 3. Buscar os Inversores (Devices) desta usina
                print(f"Buscando inversores para a usina {pv_system_id}...")
                response_dev = requests.get(f"{FRONIUS_BASE_URL}/pvsystems/{pv_system_id}/devices", headers=headers)
                response_dev.raise_for_status()
                
                dev_json = response_dev.json()
                inversores = dev_json.get('devices', []) 

                if not inversores:
                       print(f"  -> Nenhum inversor encontrado na resposta da API para esta usina.")
                
                for inv in inversores:
                    device_id = inv.get('deviceId')
                    
                    # Filtra apenas Inversores
                    if inv.get('deviceType') != 'Inverter':
                        print(f"  -> Ignorando dispositivo do tipo '{inv.get('deviceType')}' (ID: {device_id})")
                        continue
                    
                    cur.execute("SELECT id_inversor FROM tbl_inversores WHERE id_api_inversor = %s", (device_id,))
                    resultado_inv = cur.fetchone()

                    if resultado_inv:
                        print(f"  -> Inversor {device_id} já existe. (Pulando inserção)")
                    else:
                        print(f"  -> Inserindo Inversor: {inv.get('serialNumber')} (ID: {device_id})")
                        
                        potencia_kw = None
                        if inv.get('nominalAcPower'):
                            potencia_kw = inv.get('nominalAcPower') / 1000.0

                        cur.execute(
                            """
                            INSERT INTO tbl_inversores (id_usina, nome_inversor, id_api_inversor, serial_number, modelo, potencia_kw)
                            VALUES (%s, %s, %s, %s, %s, %s);
                            """,
                            (
                                id_usina_no_banco,
                                inv.get('deviceName', 'Inversor Fronius'),
                                device_id,
                                inv.get('serialNumber'),
                                inv.get('deviceTypeDetails'),
                                potencia_kw
                            )
                        )
            
            conn.commit()
            print("Job Fronius concluído com sucesso.")

    except requests.exceptions.HTTPError as e:
        print(f"ERRO HTTP na API Fronius: {e.response.status_code}")
        print(f"Resposta: {e.response.text}")
    except requests.exceptions.RequestException as e:
        print(f"ERRO na API Fronius: {e}")
    except Exception as e:
        print(f"ERRO no processamento Fronius: {e}")
        conn.rollback()


def popular_usinas_sungrow(conn):
    """Busca dados da Sungrow e insere no banco."""
    print("\n--- Iniciando Job: Sungrow ---")
    
    try:
        # --- 1. Login na Sungrow para obter o token ---
        print("Fazendo login na API iSolarCloud (Sungrow)...")
        
        login_payload = {
            "user_account": SUNGROW_LOGIN_EMAIL,
            "user_password": SUNGROW_LOGIN_PASS,
            "appkey": SUNGROW_APPKEY,
            "lang": "_en_US",
            "sys_code": "207" # Baseado no seu log de teste bem-sucedido
        }
        
        headers_login = {
            'Content-Type': 'application/json;charset=UTF-8',
            'sys_code': '200', # Baseado no seu log de teste bem-sucedido
            'x-access-key': SUNGROW_SECRET_KEY, # Baseado na sua correção
            'x-client-tz': 'GMT-3',
            'x-sign-code': '0'
        }
        
        response_login = requests.post(f"{SUNGROW_BASE_URL}/openapi/login", json=login_payload, headers=headers_login)
        response_login.raise_for_status()
        login_data = response_login.json()
        
        if login_data.get('result_code') != '1':
            print(f"ERRO no login Sungrow: {login_data.get('result_msg')} (Código: {login_data.get('result_code')})")
            print("Verifique SUNGROW_LOGIN_EMAIL, SUNGROW_LOGIN_PASS, SUNGROW_APPKEY, e SUNGROW_SECRET_KEY.")
            return

        token = login_data.get('result_data', {}).get('token')
        print("Login na Sungrow realizado, token obtido.")

        # --- 2. Preparar payload e headers para chamadas FUTURAS ---
        headers_api = headers_login.copy()

        base_payload = {
            "appkey": SUNGROW_APPKEY,
            "token": token,
            "lang": "_en_US",
            "sys_code": 207
        }

        # --- 3. Buscar a lista de Usinas (Power Station List) ---
        print("Buscando lista de usinas Sungrow...")
        payload_usinas = {**base_payload, "curPage": 1, "size": 50}
        
        response_usinas = requests.post(f"{SUNGROW_BASE_URL}/openapi/getPowerStationList", json=payload_usinas, headers=headers_api)
        response_usinas.raise_for_status()
        usinas_data = response_usinas.json()

        if usinas_data.get('result_code') != '1':
            print(f"ERRO ao buscar usinas Sungrow: {usinas_data.get('result_msg')}")
            return
            
        usinas = usinas_data.get('result_data', {}).get('pageList', [])
        print(f"Encontradas {len(usinas)} usinas Sungrow.")

        with conn.cursor() as cur:
            for usina in usinas:
                ps_id = str(usina.get('ps_id')) 
                print(f"Processando Usina Sungrow: {usina.get('ps_name')} (ID: {ps_id})")

                cur.execute("SELECT id_usina FROM tbl_usinas WHERE id_api_usina = %s", (ps_id,))
                resultado = cur.fetchone()
                id_usina_no_banco = None
                
                if resultado:
                    id_usina_no_banco = resultado[0]
                    print(f"Usina já existe no banco. ID: {id_usina_no_banco}. (Pulando inserção)")
                else:
                    cur.execute(
                        """
                        INSERT INTO tbl_usinas (nome_usina, fabricante_api, id_api_usina, localizacao, potencia_pico_kwp, data_comissionamento, fuso_horario)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        RETURNING id_usina;
                        """,
                        (
                            usina.get('ps_name'),
                            'Sungrow',
                            ps_id,
                            usina.get('ps_location'),
                            usina.get('total_capcity', {}).get('value'),
                            usina.get('install_date'),
                            usina.get('ps_current_time_zone')
                        )
                    )
                    id_usina_no_banco = cur.fetchone()[0]
                    print(f"Usina inserida no banco com id_usina: {id_usina_no_banco}")


                # 5. Buscar os Inversores (Devices) desta usina
                print(f"Buscando inversores para a usina {ps_id}...")
                
                # --- CORREÇÃO DE LÓGICA DO PAYLOAD ---
                # O payload de 'getDeviceList' deve ser construído com o 'base_payload'
                payload_dev = {
                    **base_payload,
                    "curPage": 1,
                    "size": 50,
                    "ps_id": int(ps_id), 
                    "device_type_list": [1]
                }
                # ------------------------------------
                
                response_dev = requests.post(f"{SUNGROW_BASE_URL}/openapi/getDeviceList", json=payload_dev, headers=headers_api)
                response_dev.raise_for_status()
                inversores_data = response_dev.json()

                if inversores_data.get('result_code') != '1':
                    print(f"ERRO ao buscar inversores da usina {ps_id}: {inversores_data.get('result_msg')}")
                    continue 
                
                inversores = inversores_data.get('result_data', {}).get('pageList', [])
                if not inversores:
                    print(f"  -> Nenhum inversor encontrado na resposta da API para a usina {ps_id}.")

                for inv in inversores:
                    ps_key = inv.get('ps_key') 
                    
                    cur.execute("SELECT id_inversor FROM tbl_inversores WHERE id_api_inversor = %s", (ps_key,))
                    resultado_inv = cur.fetchone()

                    if resultado_inv:
                         print(f"  -> Inversor {ps_key} já existe. (Pulando inserção)")
                    else:
                        modelo = inv.get('device_model_code', '')
                        potencia_kw = 25.0 # Fallback padrão
                        
                        try:
                            # Tenta extrair o número do modelo (ex: SG25CX, SG75CX)
                            match = re.search(r'SG(\d+)', modelo)
                            if match:
                                potencia_kw = float(match.group(1))
                        except Exception:
                            pass # Mantém o fallback se a extração falhar
                        
                        print(f"  -> Inserindo Inversor: {inv.get('device_name')} (ps_key: {ps_key}, modelo: {modelo}, potência: {potencia_kw}kW)")
                        cur.execute(
                            """
                            INSERT INTO tbl_inversores (id_usina, nome_inversor, id_api_inversor, serial_number, modelo, potencia_kw)
                            VALUES (%s, %s, %s, %s, %s, %s);
                            """,
                            (
                                id_usina_no_banco,
                                inv.get('device_name'),
                                ps_key,
                                inv.get('device_sn'),
                                inv.get('device_model_code'),
                                potencia_kw 
                            )
                        )
            
            conn.commit() 
            print("Job Sungrow concluído com sucesso.")

    except requests.exceptions.HTTPError as e:
        print(f"ERRO HTTP na API Sungrow: {e.response.status_code} - {e.response.text}")
    except requests.exceptions.RequestException as e:
        print(f"ERRO na API Sungrow: {e}")
    except Exception as e:
        print(f"ERRO no processamento Sungrow: {e}")
        conn.rollback()

def atualizar_totais_usinas(conn):
    """
    Atualiza a coluna 'potencia_inversor_kw' na tbl_usinas somando
    a potência de seus inversores filhos na tbl_inversores.
    """
    print("\n--- Iniciando Job: Atualização dos Totais ---")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE tbl_usinas u
                SET potencia_inversor_kw = (
                    SELECT SUM(i.potencia_kw)
                    FROM tbl_inversores i
                    WHERE i.id_usina = u.id_usina
                )
                WHERE u.potencia_inversor_kw IS NULL OR u.potencia_inversor_kw = 0;
                """
            )
            count = cur.rowcount
            conn.commit()
            print(f"Totais de potência de inversores atualizados para {count} usinas.")
    except Exception as e:
        print(f"ERRO ao atualizar totais das usinas: {e}")
        conn.rollback()


# --- Ponto de Entrada Principal do Script ---
if __name__ == "__main__":
    conn = None
    try:
        conn = get_db_connection()
        if conn:
            popular_usinas_fronius(conn)
            popular_usinas_sungrow(conn)
            
            # --- NOVA ETAPA ADICIONADA ---
            # Após popular os inversores, calcula e preenche os totais
            atualizar_totais_usinas(conn)
            
            print("\n--- Processo de Metadados Concluído ---")

    finally:
        if conn:
            conn.close()
            print("\nConexão com o banco de dados fechada.")