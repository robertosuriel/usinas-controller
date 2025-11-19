import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

# --- CONFIGURAÇÕES DO BANCO ---
# Preencha com seus dados do AWS RDS
DB_HOST = "db-usinas.c54mquckeem4.us-east-2.rds.amazonaws.com"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASS = "$Uriel171217"
# ------------------------------

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(
    page_title="Dashboard de Usinas",
    page_icon="☀️",
    layout="wide"
)

# --- CONEXÃO COM O BANCO ---
@st.cache_resource
def init_connection():
    """Conecta ao banco de dados PostgreSQL no RDS."""
    try:
        conn = st.connection(
            "postgresql", # Nome da conexão
            type="sql",
            host=DB_HOST,
            port=5432,
            database=DB_NAME,
            username=DB_USER,
            password=DB_PASS,
            dialect="postgresql" 
        )
        return conn
    except Exception as e:
        st.error(f"ERRO: Não foi possível conectar ao banco de dados: {e}")
        return None

conn = init_connection()

# --- FUNÇÕES DE BUSCA DE DADOS ---
@st.cache_data(ttl=600) 
def get_usinas():
    """Busca a lista de todas as usinas."""
    df = conn.query("SELECT id_usina, nome_usina, fabricante_api FROM tbl_usinas ORDER BY nome_usina", ttl=600)
    return df

@st.cache_data(ttl=600)
def get_inversores_por_usina(id_usina):
    """Busca os inversores de uma usina específica."""
    query = "SELECT id_inversor, nome_inversor FROM tbl_inversores WHERE id_usina = :id_usina ORDER BY nome_inversor"
    df = conn.query(query, params={"id_usina": id_usina}, ttl=600)
    return df

@st.cache_data(ttl=300) 
def get_dados_historicos(id_inversor, data_inicio, data_fim):
    """Busca os dados da tbl_leituras para um inversor e período."""
    query = """
    SELECT 
        timestamp_utc, 
        potencia_ativa_kw,
        energia_intervalo_wh
    FROM tbl_leituras
    WHERE id_inversor = :id_inversor 
      AND timestamp_utc BETWEEN :data_inicio AND :data_fim_ajustada
    ORDER BY timestamp_utc;
    """
    data_fim_ajustada = data_fim + timedelta(days=1)
    
    df = conn.query(query, params={
        "id_inversor": id_inversor, 
        "data_inicio": data_inicio, 
        "data_fim_ajustada": data_fim_ajustada
    }, ttl=300)
    return df

# --- INTERFACE DO USUÁRIO (Sidebar de Filtros) ---

st.sidebar.title("Filtros")

if conn is None:
    st.sidebar.error("A conexão com o banco de dados falhou. Verifique as credenciais.")
else:
    usinas_df = get_usinas()

    if usinas_df.empty:
        st.sidebar.error("Nenhuma usina encontrada no banco de dados.")
    else:
        usina_selecionada_index = st.sidebar.selectbox(
            "Selecione a Usina:",
            options=usinas_df.index, 
            format_func=lambda idx: f"{usinas_df.loc[idx]['nome_usina']} ({usinas_df.loc[idx]['fabricante_api']})"
        )
        
        usina_selecionada = usinas_df.loc[usina_selecionada_index]

        # --- CORREÇÃO PRINCIPAL AQUI (TypeError) ---
        # Converte o 'id_usina' (que é numpy.int64) para um 'int' padrão do Python
        id_usina_python_int = int(usina_selecionada['id_usina'])
        
        inversores_df = get_inversores_por_usina(id_usina_python_int)
        # ----------------------------------------------
        
        if inversores_df.empty:
            st.sidebar.error("Nenhum inversor encontrado para esta usina.")
            inversor_selecionado = None
        else:
            inversor_selecionado_index = st.sidebar.selectbox(
                "Selecione o Inversor:",
                options=inversores_df.index,
                format_func=lambda idx: f"{inversores_df.loc[idx]['nome_inversor']} (ID: {inversores_df.loc[idx]['id_inversor']})"
            )
            inversor_selecionado = inversores_df.loc[inversor_selecionado_index]
        
        # Filtro 3: Selecionar o Período
        data_padrao = datetime.now().date() - timedelta(days=7)
        data_inicio = st.sidebar.date_input("Data de Início", data_padrao)
        data_fim = st.sidebar.date_input("Data de Fim", datetime.now().date())


        # --- EXIBIÇÃO DO DASHBOARD (Página Principal) ---

        if usina_selecionada is None or inversor_selecionado is None:
            st.info("Por favor, selecione uma usina e um inversor na barra lateral.")
        else:
            st.title(f"☀️ Dashboard: {usina_selecionada['nome_usina']}")
            
            # --- CORREÇÃO AQUI TAMBÉM ---
            # Converte o 'id_inversor' para int antes de passar para a query
            id_inversor_python_int = int(inversor_selecionado['id_inversor'])
            st.subheader(f"Análise do Inversor: {inversor_selecionado['nome_inversor']}")

            # Carrega os dados com base nos filtros
            df = get_dados_historicos(id_inversor_python_int, data_inicio, data_fim)
            # ---------------------------

            if df.empty:
                st.warning("Nenhum dado encontrado para este inversor no período selecionado.")
            else:
                # Define o timestamp como o índice do DataFrame (melhor para gráficos)
                df = df.set_index('timestamp_utc')

                # --- Métricas (KPIs) ---
                st.header("Métricas Principais no Período")
                
                # Calcula os KPIs
                potencia_max_kw = df['potencia_ativa_kw'].max()
                energia_total_kwh = df['energia_intervalo_wh'].sum() / 1000.0 # Converte de Wh para kWh
                dias_com_dados = df.index.normalize().nunique() # Conta dias únicos

                col1, col2, col3 = st.columns(3)
                col1.metric("Pico de Potência (kW)", f"{potencia_max_kw:,.2f}")
                col2.metric("Energia Total Gerada (kWh)", f"{energia_total_kwh:,.2f}")
                col3.metric("Dias Analisados", f"{dias_com_dados}")

                # --- Gráfico de Potência ---
                st.header("Curva de Potência Ativa (kW)")
                st.line_chart(df['potencia_ativa_kw'])
                
                # --- Gráfico de Energia Diária ---
                st.header("Geração Diária (kWh)")
                # Agrupa os dados por dia e soma a energia
                energia_diaria_kwh = (df['energia_intervalo_wh'].resample('D').sum()) / 1000.0
                st.bar_chart(energia_diaria_kwh)

                # --- Tabela de Dados Brutos ---
                with st.expander("Ver dados brutos"):
                    st.dataframe(df.reset_index())