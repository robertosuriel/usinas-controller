import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, date

# ------------------------------

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(
    page_title="Dashboard de Usinas",
    page_icon="☀️",
    layout="wide"
)

# # --- CONEXÃO COM O BANCO ---
# @st.cache_resource
# def init_connection():
#     """Conecta ao banco de dados PostgreSQL no RDS."""
#     try:
#         conn = st.connection(
#             "postgresql", # Nome da conexão
#             type="sql",
#             host=DB_HOST,
#             port=5432,
#             database=DB_NAME,
#             username=DB_USER,
#             password=DB_PASS,
#             dialect="postgresql" 
#         )
#         return conn
#     except Exception as e:
#         st.error(f"ERRO: Não foi possível conectar ao banco de dados: {e}")
#         return None

# --- CONEXÃO COM O BANCO ---
@st.cache_resource
def init_connection():
    try:
        # O Streamlit busca automaticamente em .streamlit/secrets.toml
        # a seção [connections.postgresql]
        conn = st.connection("postgresql", type="sql")
        return conn
    except Exception as e:
        st.error(f"ERRO: {e}")
        return None

conn = init_connection()

# --- FUNÇÕES DE BUSCA DE DADOS ---
@st.cache_data(ttl=600) 
def get_usinas():
    """Busca a lista de todas as usinas."""
    df = conn.query("SELECT id_usina, nome_usina, fabricante_api FROM tbl_usinas ORDER BY nome_usina", ttl=600)
    return df

@st.cache_data(ttl=600)
def get_inversores_por_usinas(lista_ids_usinas: list):
    """Busca os inversores de UMA OU MAIS usinas."""
    query = "SELECT id_inversor, nome_inversor, id_usina FROM tbl_inversores WHERE id_usina = ANY(:lista_ids) ORDER BY nome_inversor"
    df = conn.query(query, params={"lista_ids": lista_ids_usinas}, ttl=600)
    return df

# --- CORREÇÃO AQUI ---
@st.cache_data(ttl=300) 
def get_dados_historicos(lista_ids_inversores: list, data_inicio: date, data_fim: date):
    """Busca os dados da tbl_leituras para um ou mais inversores e período."""
    query = """
    SELECT 
        l.timestamp_utc, 
        l.potencia_ativa_kw,
        l.energia_intervalo_wh,
        i.nome_inversor,
        u.nome_usina 
    FROM tbl_leituras l
    JOIN tbl_inversores i ON l.id_inversor = i.id_inversor
    JOIN tbl_usinas u ON i.id_usina = u.id_usina
    WHERE l.id_inversor = ANY(:lista_ids)
      AND l.timestamp_utc BETWEEN :data_inicio AND :data_fim_ajustada
    ORDER BY l.timestamp_utc;
    """
    data_fim_ajustada = data_fim + timedelta(days=1)
    
    df = conn.query(query, params={
        "lista_ids": lista_ids_inversores, 
        "data_inicio": data_inicio, 
        "data_fim_ajustada": data_fim_ajustada
    }, ttl=300)
    return df
# ------------------------

# --- INTERFACE DO USUÁRIO (Sidebar de Filtros) ---

st.sidebar.title("Filtros")

if conn is None:
    st.sidebar.error("A conexão com o banco de dados falhou. Verifique as credenciais.")
else:
    usinas_df = get_usinas()

    if usinas_df.empty:
        st.sidebar.error("Nenhuma usina encontrada no banco de dados.")
    else:
        nomes_usinas_selecionadas = st.sidebar.multiselect(
            "Selecione as Usinas:",
            options=usinas_df['nome_usina'].unique(),
            default=usinas_df['nome_usina'].iloc[0] 
        )
        
        if not nomes_usinas_selecionadas:
            st.sidebar.warning("Por favor, selecione pelo menos uma usina.")
            st.stop() 
            
        ids_usinas_selecionadas = usinas_df[usinas_df['nome_usina'].isin(nomes_usinas_selecionadas)]['id_usina'].tolist()
        
        inversores_df = get_inversores_por_usinas(ids_usinas_selecionadas)
        
        opcoes_inversores = ["-- TODOS OS INVERSORES --"] + inversores_df['nome_inversor'].unique().tolist()
        
        selecao_inversores = st.sidebar.multiselect(
            "Selecione os Inversores:",
            options=opcoes_inversores,
            default="-- TODOS OS INVERSORES --"
        )

        if not selecao_inversores:
            st.sidebar.warning("Por favor, selecione pelo menos um inversor.")
            st.stop()
            
        if "-- TODOS OS INVERSORES --" in selecao_inversores or len(selecao_inversores) == 0:
            ids_inversores_finais = inversores_df['id_inversor'].tolist()
        else:
            ids_inversores_finais = inversores_df[inversores_df['nome_inversor'].isin(selecao_inversores)]['id_inversor'].tolist()
            
        agrupar_por = st.sidebar.radio(
            "Agrupar gráfico por:",
            ("Usina", "Inversor"),
            horizontal=True
        )
        
        data_padrao = datetime.now().date() - timedelta(days=7)
        data_inicio = st.sidebar.date_input("Data de Início", data_padrao)
        data_fim = st.sidebar.date_input("Data de Fim", datetime.now().date())


        # --- ESTRUTURA DE ABAS ---
        tab_historico, tab_status_dia = st.tabs(["Análise Histórica", "Status do Dia (em breve)"])

        with tab_historico:
            st.title(f"☀️ Análise Histórica")
            st.write(f"Analisando **{len(ids_inversores_finais)}** inversores em **{len(ids_usinas_selecionadas)}** usinas.")

            # Carrega os dados com base nos filtros
            df = get_dados_historicos(ids_inversores_finais, data_inicio, data_fim)

            if df.empty:
                st.warning("Nenhum dado encontrado para esta seleção no período selecionado.")
            else:
                df = df.set_index('timestamp_utc')

                # --- CORREÇÃO E NOVOS CÁLCULOS ---
                # 1. Define a coluna de agrupamento (Usina ou Inversor)
                if agrupar_por == "Usina":
                    group_col = 'nome_usina'
                else: # Agrupar por Inversor
                    df['nome_inversor_unico'] = df['nome_usina'] + ' - ' + df['nome_inversor']
                    group_col = 'nome_inversor_unico'
                
                # 2. Calcula a energia acumulada (em kWh)
                # O groupby().cumsum() garante que o "acumulado" reinicie para cada grupo
                df['energia_acumulada_kwh'] = (
                    df.groupby(group_col, group_keys=False)['energia_intervalo_wh']
                    .apply(lambda x: x.cumsum() / 1000.0)
                )
                
                # 3. Calcula o total agrupado por dia (para o gráfico de barras)
                energia_diaria_kwh = (
                    df.groupby(group_col)['energia_intervalo_wh'].resample('D').sum() / 1000.0
                )
                # Precisamos reformatar para o gráfico de barras
                energia_diaria_kwh = energia_diaria_kwh.unstack(level=group_col)
                # -----------------------------------------------

                # --- Métricas (KPIs) ---
                st.header("Métricas Totais Agregadas")
                
                potencia_agregada_por_timestamp = df.groupby(level=0)['potencia_ativa_kw'].sum()
                potencia_max_kw = potencia_agregada_por_timestamp.max()
                energia_total_kwh = df['energia_intervalo_wh'].sum() / 1000.0
                dias_com_dados = df.index.normalize().nunique() 

                col1, col2, col3 = st.columns(3)
                col1.metric("Pico de Potência (kW)", f"{potencia_max_kw:,.2f}")
                col2.metric("Energia Total Gerada (kWh)", f"{energia_total_kwh:,.2f}")
                col3.metric("Dias Analisados", f"{dias_com_dados}")

                # --- Gráfico de Potência (Comparativo) ---
                st.header(f"Curva de Potência Ativa (kW) por {agrupar_por}")
                df_agrupado_potencia = df.pivot_table(index='timestamp_utc', columns=group_col, values='potencia_ativa_kw', aggfunc='sum')
                st.line_chart(df_agrupado_potencia)
                
                # --- NOVO Gráfico de Energia Acumulada ---
                st.header(f"Energia Acumulada no Período (kWh) por {agrupar_por}")
                df_agrupado_energia_acc = df.pivot_table(index='timestamp_utc', columns=group_col, values='energia_acumulada_kwh')
                st.area_chart(df_agrupado_energia_acc)
                
                # --- Gráfico de Energia Diária (Comparativo) ---
                st.header(f"Geração Diária (kWh) por {agrupar_por}")
                st.bar_chart(energia_diaria_kwh)

                # --- Tabela de Dados Brutos ---
                with st.expander("Ver dados brutos (com energia acumulada)"):
                    # Mostra o DF completo com a nova coluna
                    st.dataframe(df.reset_index())
        
        with tab_status_dia:
            st.header("Status em Tempo Real")
            st.info("Em breve: Esta aba mostrará o status de operação atual e os alertas.")