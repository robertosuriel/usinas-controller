import psycopg2
import psycopg2.extras 
import requests
import sys
import json
import pytz 
from datetime import datetime, timedelta, date
import time

# --- CONFIGURAÇÕES ---
DB_HOST = "db-usinas.c54mquckeem4.us-east-2.rds.amazonaws.com"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASS = "$Uriel171217"
FRONIUS_ACCESS_KEY_ID = "FKIABDA70962868A47488C9D7649E64DF0C1"
FRONIUS_ACCESS_KEY_VALUE = "d2cce829-d736-45bc-9da2-2071d56bfaeb"
FRONIUS_BASE_URL = "https://api.solarweb.com/swqapi" 
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
        return conn
    except Exception as e:
        print(f"ERRO: Não foi possível conectar ao banco de dados: {e}")
        return None # Retorna None em vez de sair

def parse_fronius_channels(channels_list):
    """Transforma a lista de canais da Fronius em um dicionário."""
    channels_dict = {}
    for channel in channels_list:
        channels_dict[channel.get('channelName')] = channel.get('value')
    return channels_dict

def get_inverters_from_db():
    """Busca a lista de inversores Fronius para processar."""
    print("Buscando inversores Fronius no banco de dados...")
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
                    i.id_api_inversor, 
                    u.id_api_usina, 
                    i.data_ativacao, -- Usando a data corrigida
                    u.fuso_horario
                FROM tbl_inversores i
                JOIN tbl_usinas u ON i.id_usina = u.id_usina
                WHERE u.fabricante_api = 'Fronius'; 
                """
            )
            inversores = cur.fetchall()
            print(f"Encontrados {len(inversores)} inversores Fronius.")
            
    except Exception as e:
        print(f"ERRO ao buscar lista de inversores: {e}")
    finally:
        if conn:
            conn.close()
    return inversores

def run_fronius_backfill():
    """
    Executa o backfill para todos os inversores Fronius.
    """
    print("\n--- INICIANDO BACKFILL HISTÓRICO - FRONIUS (v6 - Super Robusto) ---")
    
    inversores = get_inverters_from_db()
    if not inversores:
        print("Nenhum inversor para processar. Encerrando.")
        return

    headers = {
        "accept": "application/json",
        "AccessKeyId": FRONIUS_ACCESS_KEY_ID,
        "AccessKeyValue": FRONIUS_ACCESS_KEY_VALUE
    }

    # 2. Loop por cada inversor
    for inv in inversores:
        id_inversor_db, api_device_id, api_pv_system_id, dt_ativacao, fuso_horario = inv
        
        print(f"\n--- Processando Inversor ID: {id_inversor_db} (API ID: {api_device_id}) ---")

        if not dt_ativacao:
            print(f"AVISO: 'data_ativacao' está NULA. Rode o script 'etl_0_5_corrigir_datas.py'. Pulando este inversor.")
            continue 
        
        if not fuso_horario: fuso_horario = 'America/Sao_Paulo'
        try:
            tz = pytz.timezone(fuso_horario)
        except pytz.exceptions.UnknownTimeZoneError:
            tz = pytz.timezone('America/Sao_Paulo')

        data_fim = date.today() - timedelta(days=1)
        data_atual = data_fim
        
        # 3. Loop por cada DIA (de ontem até a data de ativação)
        while data_atual >= dt_ativacao:
            print(f"Processando dia: {data_atual.strftime('%Y-%m-%d')}", end='')
            
            conn = None
            try:
                # --- CORREÇÃO DE CONEXÃO: Abre a conexão para este dia ---
                conn = get_db_connection()
                if not conn:
                    print("  -> ERRO: Não foi possível conectar ao DB. Pulando este dia.")
                    time.sleep(5) # Espera 5s antes de tentar o próximo dia
                    data_atual -= timedelta(days=1)
                    continue

                with conn.cursor() as cur:
                
                    # --- CORREÇÃO DE EFICIÊNCIA: Verifica se o dia já existe ---
                    cur.execute(
                        """
                        SELECT 1 FROM tbl_leituras 
                        WHERE id_inversor = %s AND timestamp_utc::date = %s
                        LIMIT 1;
                        """,
                        (id_inversor_db, data_atual)
                    )
                    if cur.fetchone():
                        print(f"  -> Dados já existem. Pulando.")
                        data_atual -= timedelta(days=1)
                        continue # Pula para o próximo dia
                    # --------------------------------------------------------

                    # Converte o dia local para os timestamps UTC corretos
                    try:
                        from_dt_local = tz.localize(datetime(data_atual.year, data_atual.month, data_atual.day, 0, 0, 0))
                        to_dt_local = tz.localize(datetime(data_atual.year, data_atual.month, data_atual.day, 23, 59, 59))
                        from_utc_str = from_dt_local.astimezone(pytz.utc).isoformat()
                        to_utc_str = to_dt_local.astimezone(pytz.utc).isoformat()
                    except (pytz.exceptions.NonExistentTimeError, pytz.exceptions.AmbiguousTimeError) as e:
                        print(f"  -> Erro de fuso (horário de verão): {e}. Pulando este dia.")
                        data_atual -= timedelta(days=1)
                        continue

                    # Chama a API (com 'Limit=300')
                    url = f"{FRONIUS_BASE_URL}/pvsystems/{api_pv_system_id}/devices/{api_device_id}/histdata"
                    params = {'from': from_utc_str, 'to': to_utc_str, 'limit': 300 }
                    
                    response_api = requests.get(url, headers=headers, params=params, timeout=20)
                    response_api.raise_for_status()
                    
                    json_data = response_api.json()
                    all_records_for_this_day = json_data.get('data', [])

                    if not all_records_for_this_day:
                        print("  -> Sem dados retornados pela API.")
                        data_atual -= timedelta(days=1)
                        continue

                    # Transforma (ETL) e prepara para o Batch Insert
                    data_to_insert = []
                    for record in all_records_for_this_day:
                        timestamp_utc = record.get('logDateTime')
                        duration_sec = record.get('logDuration')
                        
                        channels_dict = parse_fronius_channels(record.get('channels', []))
                        
                        energia_wh = channels_dict.get('EnergyExported')
                        potencia_ativa_kw = None
                        
                        if energia_wh is not None and duration_sec is not None and duration_sec > 0:
                            potencia_ativa_w = (energia_wh * 3600) / duration_sec
                            potencia_ativa_kw = potencia_ativa_w / 1000.0

                        row_tuple = (
                            timestamp_utc, id_inversor_db,
                            potencia_ativa_kw, channels_dict.get('ApparentPower'), 
                            channels_dict.get('ReactivePower'), channels_dict.get('PowerFactor'),   
                            energia_wh, channels_dict.get('VoltageDC1'),    
                            channels_dict.get('CurrentDC1'), channels_dict.get('VoltageDC2'),    
                            channels_dict.get('CurrentDC2'), channels_dict.get('VoltageA'),      
                            channels_dict.get('VoltageB'), channels_dict.get('VoltageC'),      
                            channels_dict.get('CurrentA'), channels_dict.get('CurrentB'),      
                            channels_dict.get('CurrentC'),      
                        )
                        data_to_insert.append(row_tuple)

                    # Carrega (Load) os dados do dia
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
                        conn.commit() # Salva a transação deste dia
                        print(f"  -> Sucesso. {len(data_to_insert)} registros inseridos.")

            # Tratamento de erro por dia (se um dia falhar, continua no próximo)
            except requests.exceptions.HTTPError as e:
                print(f"  -> ERRO na API ({e.response.status_code}) ao buscar dia {data_atual}: {e.response.text}. Pulando este dia.")
                if conn: conn.rollback() 
            except requests.exceptions.RequestException as e:
                print(f"  -> ERRO de Conexão ao buscar dia {data_atual}: {e}. Pulando este dia.")
                if conn: conn.rollback()
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as db_err:
                print(f"  -> ERRO de Banco de Dados no dia {data_atual}: {db_err}. Pulando este dia.")
                # Não precisa de rollback, pois a conexão já deve ter morrido
            except Exception as e:
                print(f"  -> ERRO no processamento do dia {data_atual}: {e}. Pulando.")
                if conn: conn.rollback() 
            finally:
                if conn:
                    conn.close() # Garante que a conexão seja fechada ao final de cada dia
            
            data_atual -= timedelta(days=1)
            time.sleep(0.5) # Pausa de 0.5s por dia para ser gentil com a API
    
    print("\n--- Backfill da Fronius concluído. ---")


# --- Ponto de Entrada Principal do Script ---
if __name__ == "__main__":
    run_fronius_backfill()