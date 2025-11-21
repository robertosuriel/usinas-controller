import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, date
import plotly.graph_objects as go

# --- CONFIGURAÇÕES DO BANCO ---
# (Seus dados)
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
            "postgresql", 
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
    df = conn.query("SELECT id_usina, nome_usina, fabricante_api FROM tbl_usinas ORDER BY nome_usina", ttl=600)
    return df

@st.cache_data(ttl=600)
def get_inversores_por_usinas(lista_ids_usinas: list):
    query = "SELECT id_inversor, nome_inversor, id_usina FROM tbl_inversores WHERE id_usina = ANY(:lista_ids) ORDER BY nome_inversor"
    df = conn.query(query, params={"lista_ids": lista_ids_usinas}, ttl=600)
    return df

@st.cache_data(ttl=300) 
def get_dados_historicos(lista_ids_inversores: list, data_inicio: date, data_fim: date):
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

# --- INTERFACE DO USUÁRIO ---

st.sidebar.title("Filtros")

if conn is None:
    st.sidebar.error("A conexão com o banco de dados falhou.")
else:
    usinas_df = get_usinas()

    if usinas_df.empty:
        st.sidebar.error("Nenhuma usina encontrada.")
    else:
        # Filtros Principais
        nomes_usinas_selecionadas = st.sidebar.multiselect(
            "Selecione as Usinas:",
            options=usinas_df['nome_usina'].unique(),
            default=usinas_df['nome_usina'].iloc[0] 
        )
        
        if not nomes_usinas_selecionadas:
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
            st.stop()
            
        if "-- TODOS OS INVERSORES --" in selecao_inversores or len(selecao_inversores) == 0:
            ids_inversores_finais = inversores_df['id_inversor'].tolist()
        else:
            ids_inversores_finais = inversores_df[inversores_df['nome_inversor'].isin(selecao_inversores)]['id_inversor'].tolist()
            
        # Filtros de Data Globais
        data_padrao = datetime.now().date() - timedelta(days=7)
        data_inicio = st.sidebar.date_input("Data de Início", data_padrao)
        data_fim = st.sidebar.date_input("Data de Fim", datetime.now().date())

        # --- ESTRUTURA DE ABAS ---
        tab_historico, tab_comparacao, tab_status_dia = st.tabs(["Análise Histórica", "Comparação de Dias", "Status em Tempo Real"])

        # ==============================================================================
        # ABA 1: ANÁLISE HISTÓRICA (Mantida similar, com ajustes)
        # ==============================================================================
        with tab_historico:
            st.title(f"☀️ Análise Histórica")
            
            # Opção de agrupamento específica desta aba
            agrupar_por = st.radio("Agrupar visualização por:", ("Usina", "Inversor"), horizontal=True)

            df = get_dados_historicos(ids_inversores_finais, data_inicio, data_fim)

            if df.empty:
                st.warning("Nenhum dado encontrado para esta seleção.")
            else:
                df = df.set_index('timestamp_utc')

                if agrupar_por == "Usina":
                    group_col = 'nome_usina'
                else:
                    df['nome_inversor_unico'] = df['nome_usina'] + ' - ' + df['nome_inversor']
                    group_col = 'nome_inversor_unico'
                
                # Cálculos
                df['energia_acumulada_kwh'] = (df.groupby(group_col, group_keys=False)['energia_intervalo_wh'].apply(lambda x: x.cumsum() / 1000.0))
                energia_diaria_kwh = (df.groupby(group_col)['energia_intervalo_wh'].resample('D').sum() / 1000.0).unstack(level=group_col)

                # Métricas Totais (Soma de tudo que foi selecionado)
                st.header("Visão Geral do Período")
                potencia_agregada_por_timestamp = df.groupby(level=0)['potencia_ativa_kw'].sum()
                
                col1, col2, col3 = st.columns(3)
                col1.metric("Pico Máximo de Potência (kW)", f"{potencia_agregada_por_timestamp.max():,.2f}")
                col2.metric("Energia Total Produzida (kWh)", f"{(df['energia_intervalo_wh'].sum() / 1000.0):,.2f}")
                col3.metric("Dias com Dados", f"{df.index.normalize().nunique()}")

                st.divider()

                # Gráficos
                st.subheader(f"Curva de Potência (kW) por {agrupar_por}")
                df_agrupado_potencia = df.pivot_table(index='timestamp_utc', columns=group_col, values='potencia_ativa_kw', aggfunc='sum')
                st.line_chart(df_agrupado_potencia)
                
                st.subheader(f"Geração Diária (kWh) por {agrupar_por}")
                st.bar_chart(energia_diaria_kwh)

        # ==============================================================================
        # ABA 2: COMPARAÇÃO DE DIAS (Totalmente Reformulada)
        # ==============================================================================
        with tab_comparacao:
            st.title("📊 Comparação Detalhada de Dias")
            st.markdown("Selecione dias específicos para sobrepor as curvas e comparar a performance lado a lado.")

            col_config, col_grafico = st.columns([1, 3])

            with col_config:
                st.subheader("Configuração")
                num_datas = st.number_input("Quantos dias comparar?", min_value=2, max_value=5, value=2)
                
                datas_comp = []
                for i in range(num_datas):
                    d = st.date_input(f"Dia {i+1}", value=datetime.now().date() - timedelta(days=i+1), key=f"d_{i}")
                    datas_comp.append(d)

                metrica_visual = st.selectbox("Métrica Visual:", ["Potência Ativa (kW)", "Energia Acumulada (kWh)"])
                
                if st.button("Comparar Dias"):
                    st.session_state['run_comp'] = True

            with col_grafico:
                if st.session_state.get('run_comp'):
                    # 1. Buscar dados de TODOS os dias selecionados de uma vez (otimização)
                    # (Mas precisamos processar dia a dia para garantir a sobreposição)
                    
                    fig = go.Figure()
                    stats_data = []
                    
                    cores = ['#29b6f6', '#66bb6a', '#ffa726', '#ab47bc', '#ef5350']

                    for idx, data_alvo in enumerate(datas_comp):
                        # Busca dados APENAS do dia específico para os inversores selecionados
                        df_dia = get_dados_historicos(ids_inversores_finais, data_alvo, data_alvo)
                        
                        if df_dia.empty:
                            st.warning(f"Sem dados para {data_alvo}")
                            continue

                        # --- PROCESSAMENTO CRUCIAL ---
                        # 1. Agrupar por Timestamp (soma todos os inversores selecionados num único ponto no tempo)
                        # Isso cria a visão da "Usina Virtual" composta pela seleção
                        df_agrupado = df_dia.groupby('timestamp_utc').agg({
                            'potencia_ativa_kw': 'sum',
                            'energia_intervalo_wh': 'sum'
                        }).reset_index()

                        # 2. Criar eixo X normalizado (Hora do Dia) para sobreposição
                        df_agrupado['hora_decimal'] = df_agrupado['timestamp_utc'].dt.hour + df_agrupado['timestamp_utc'].dt.minute / 60.0
                        
                        # 3. Calcular acumulado DO DIA (começando do zero)
                        df_agrupado['energia_acumulada_kwh'] = df_agrupado['energia_intervalo_wh'].cumsum() / 1000.0

                        # 4. Plotar
                        eixo_y = df_agrupado['potencia_ativa_kw'] if metrica_visual == "Potência Ativa (kW)" else df_agrupado['energia_acumulada_kwh']
                        
                        fig.add_trace(go.Scatter(
                            x=df_agrupado['hora_decimal'],
                            y=eixo_y,
                            mode='lines',
                            name=f"{data_alvo.strftime('%d/%m/%Y')}",
                            line=dict(width=2, color=cores[idx % len(cores)]),
                            hovertemplate='%{y:.2f}'
                        ))

                        # 5. Coletar Estatísticas
                        stats_data.append({
                            "Data": data_alvo.strftime('%d/%m/%Y'),
                            "Energia Total (kWh)": df_agrupado['energia_acumulada_kwh'].max(),
                            "Pico de Potência (kW)": df_agrupado['potencia_ativa_kw'].max()
                        })

                    # Configuração do Gráfico
                    fig.update_layout(
                        title=f"Comparativo: {metrica_visual}",
                        xaxis_title="Hora do Dia",
                        yaxis_title=metrica_visual,
                        hovermode="x unified",
                        height=500,
                        xaxis=dict(
                            tickmode='linear',
                            tick0=0,
                            dtick=2 # Marcar a cada 2 horas
                        )
                    )
                    st.plotly_chart(fig, use_container_width=True)

                    # Tabela de Estatísticas
                    st.subheader("Resultados da Comparação")
                    df_stats = pd.DataFrame(stats_data)
                    
                    # Formatação
                    if not df_stats.empty:
                        df_stats = df_stats.set_index("Data")
                        st.dataframe(df_stats.style.format("{:.2f}"), use_container_width=True)
                        
                        # Insight rápido
                        melhor_dia = df_stats['Energia Total (kWh)'].idxmax()
                        pior_dia = df_stats['Energia Total (kWh)'].idxmin()
                        diff = df_stats.loc[melhor_dia, 'Energia Total (kWh)'] - df_stats.loc[pior_dia, 'Energia Total (kWh)']
                        
                        if len(datas_comp) > 1:
                            st.info(f"💡 O dia **{melhor_dia}** gerou **{diff:.2f} kWh** a mais que o dia {pior_dia}.")

        # ==============================================================================
        # ABA 3: STATUS EM TEMPO REAL
        # ==============================================================================
        with tab_status_dia:
            st.header("Monitoramento em Tempo Real")
            st.info("Em breve: Alertas de falha, status de conexão e última atualização dos inversores.")