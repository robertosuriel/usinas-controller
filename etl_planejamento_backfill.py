import psycopg2
import sys
from datetime import date, timedelta

# --- CONFIGURAÇÕES ---
# (Copie as mesmas do seu script de metadados)

# --- Banco de Dados (AWS RDS) ---
DB_HOST = "db-usinas.c54mquckeem4.us-east-2.rds.amazonaws.com"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASS = "$Uriel171217"
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
        sys.exit(1)

def calcular_plano_backfill():
    """
    Verifica o banco de dados e calcula o número de dias e chamadas
    de API necessárias para o backfill.
    """
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return

        with conn.cursor() as cur:
            # Busca todas as usinas e CONTA quantos inversores cada uma tem
            cur.execute(
                """
                SELECT 
                    u.nome_usina, 
                    u.fabricante_api, 
                    u.data_comissionamento,
                    COUNT(i.id_inversor) AS num_inversores
                FROM tbl_usinas u
                LEFT JOIN tbl_inversores i ON u.id_usina = i.id_usina
                GROUP BY u.id_usina, u.nome_usina, u.fabricante_api, u.data_comissionamento
                ORDER BY u.fabricante_api, u.nome_usina;
                """
            )
            usinas = cur.fetchall()
            
            if not usinas:
                print("Nenhuma usina encontrada nas tabelas de metadados. Rode o script de metadados primeiro.")
                return

            hoje = date.today()
            ontem = hoje - timedelta(days=1)
            
            total_chamadas_fronius = 0
            total_chamadas_sungrow = 0

            print("\n--- PLANO DE EXECUÇÃO DO BACKFILL (ATÉ ONTEM) ---")
            print(f"{'Usina':<30} | {'Fabricante':<10} | {'Inversores':<10} | {'Dias de Backfill':<16} | {'Chamadas API':<15}")
            print("-" * 88)

            for usina in usinas:
                nome, fabricante, dt_comissionamento, num_inversores = usina
                
                if not dt_comissionamento:
                    print(f"AVISO: {nome} não tem data de comissionamento. Pulando.")
                    continue
                
                # Ignora usinas instaladas hoje ou no futuro
                if dt_comissionamento >= hoje:
                    dias_de_backfill = 0
                else:
                    dias_de_backfill = (ontem - dt_comissionamento).days + 1 # +1 para incluir o dia de início
                
                chamadas_total_usina = 0
                
                if fabricante == 'Fronius':
                    # Lógica Fronius: 1 chamada/dia POR INVERSOR
                    chamadas_dia = num_inversores * 1 
                    chamadas_total_usina = dias_de_backfill * chamadas_dia
                    total_chamadas_fronius += chamadas_total_usina
                
                elif fabricante == 'Sungrow':
                    # Lógica Sungrow: 8 chamadas/dia POR USINA (blocos de 3h)
                    # A API aceita uma lista de inversores, então otimizamos por usina.
                    chamadas_dia = 8 
                    chamadas_total_usina = dias_de_backfill * chamadas_dia
                    total_chamadas_sungrow += chamadas_total_usina
                
                print(f"{nome:<30} | {fabricante:<10} | {num_inversores:<10} | {dias_de_backfill:<16} | {chamadas_total_usina:<15}")

            print("-" * 88)
            print("\n--- TOTAIS E ESTIMATIVAS DE TEMPO ---")
            
            # --- Análise Fronius ---
            print("\nFRONIUS:")
            print(f"  -> Total de chamadas de API necessárias: {total_chamadas_fronius}")
            print(f"  -> Limite da API: ~30.000 chamadas/hora")
            horas_fronius = total_chamadas_fronius / 30000.0
            print(f"  -> Tempo de execução estimado: {horas_fronius * 60:.2f} minutos")

            # --- Análise Sungrow ---
            print("\nSUNGROW:")
            print(f"  -> Total de chamadas de API necessárias: {total_chamadas_sungrow}")
            print(f"  -> Limite da API: 2.000 chamadas/hora")
            # Adicionamos uma margem de segurança (rodando a 1800/h)
            horas_sungrow = total_chamadas_sungrow / 1800.0 
            print(f"  -> Tempo de execução estimado (com segurança): {horas_sungrow:.2f} horas")

            print("\nNOTA: O backfill da Sungrow pode precisar ser executado em sessões separadas se o tempo total exceder o limite de 24h do token de login.")
            
    except Exception as e:
        print(f"ERRO CRÍTICO no script de planejamento: {e}")
    finally:
        if conn:
            conn.close()
            print("\nConexão com o banco de dados fechada.")

# --- Ponto de Entrada Principal do Script ---
if __name__ == "__main__":
    calcular_plano_backfill()