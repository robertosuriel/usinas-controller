import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, date
import plotly.graph_objects as go
import plotly.express as px
import pytz

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(
    page_title="Dashboard de Usinas",
    page_icon="☀️",
    layout="wide"
)

# --- CONEXÃO COM O BANCO ---
@st.cache_resource
# --- CONEXÃO COM O BANCO ---
@st.cache_resource
def init_connection():
    try:
        # O Streamlit busca automaticamente as credenciais em .streamlit/secrets.toml
        # ou nos Secrets do Cloud, procurando pela chave "postgresql"
        conn = st.connection("postgresql", type="sql")
        return conn
    except Exception as e:
        st.error(f"ERRO DE CONEXÃO: {e}")
        return None

conn = init_connection()

# --- FUNÇÕES DE BUSCA DE DADOS ---
@st.cache_data(ttl=600) 
def get_usinas():
    # Adicionado potencia_pico_kwp para cálculo de meta
    df = conn.query("SELECT id_usina, nome_usina, fabricante_api, potencia_pico_kwp FROM tbl_usinas ORDER BY nome_usina", ttl=600)
    return df

@st.cache_data(ttl=600)
def get_inversores_por_usinas(lista_ids_usinas: list):
    if not lista_ids_usinas: return pd.DataFrame()
    query = "SELECT id_inversor, nome_inversor, id_usina FROM tbl_inversores WHERE id_usina = ANY(:lista_ids) ORDER BY nome_inversor"
    df = conn.query(query, params={"lista_ids": lista_ids_usinas}, ttl=600)
    return df

@st.cache_data(ttl=300) 
def get_dados_historicos(lista_ids_inversores: list, data_inicio: date, data_fim: date):
    if not lista_ids_inversores: return pd.DataFrame()
    query = """
    SELECT 
        l.timestamp_utc, 
        l.potencia_ativa_kw,
        l.energia_intervalo_wh,
        i.nome_inversor,
        u.nome_usina,
        u.id_usina
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
    st.sidebar.error("Erro de conexão com o banco de dados.")
else:
    usinas_df = get_usinas()

    if usinas_df.empty:
        st.sidebar.error("Nenhuma usina encontrada.")
    else:
        # Filtro 1: Usinas
        nomes_usinas_selecionadas = st.sidebar.multiselect(
            "Selecione as Usinas:",
            options=usinas_df['nome_usina'].unique(),
            default=usinas_df['nome_usina'].iloc[0] 
        )
        
        if not nomes_usinas_selecionadas:
            st.warning("Selecione ao menos uma usina.")
            st.stop() 
            
        # Dados das usinas selecionadas (incluindo capacidade para meta)
        usinas_sel_df = usinas_df[usinas_df['nome_usina'].isin(nomes_usinas_selecionadas)]
        ids_usinas_selecionadas = [int(x) for x in usinas_sel_df['id_usina'].tolist()]
        
        # Filtro 2: Inversores
        inversores_df = get_inversores_por_usinas(ids_usinas_selecionadas)
        opcoes_inversores = ["-- TODOS OS INVERSORES --"] + inversores_df['nome_inversor'].unique().tolist()
        
        selecao_inversores = st.sidebar.multiselect(
            "Selecione os Inversores:",
            options=opcoes_inversores,
            default="-- TODOS OS INVERSORES --"
        )

        if not selecao_inversores:
            st.warning("Selecione ao menos um inversor.")
            st.stop()
            
        if "-- TODOS OS INVERSORES --" in selecao_inversores or len(selecao_inversores) == 0:
            ids_inversores_finais = [int(x) for x in inversores_df['id_inversor'].tolist()]
        else:
            ids_inversores_finais = [int(x) for x in inversores_df[inversores_df['nome_inversor'].isin(selecao_inversores)]['id_inversor'].tolist()]
            
        # Filtros de Data
        st.sidebar.markdown("---")
        data_padrao_inicio = datetime.now().date() - timedelta(days=15)
        data_inicio = st.sidebar.date_input("Data de Início", data_padrao_inicio)
        data_fim = st.sidebar.date_input("Data de Fim", datetime.now().date())

        # --- ABAS ---
        tab_historico, tab_comparacao, tab_status_dia = st.tabs(["📈 Análise Histórica", "📊 Comparação de Dias", "⚡ Status Real-Time"])

        # ==============================================================================
        # ABA 1: ANÁLISE HISTÓRICA (REFORMULADA)
        # ==============================================================================
        with tab_historico:
            st.title(f"Análise de Performance")
            
            # --- Controles da Aba ---
            c1, c2, c3 = st.columns([1, 1, 2])
            with c1:
                # Seletor de Visão Temporal (Diário, Mensal, Anual)
                visao_temporal = st.selectbox("Visão Temporal:", ["Diário", "Mensal", "Anual"])
            with c2:
                agrupar_por = st.selectbox("Agrupar por:", ["Usina", "Inversor"])
            with c3:
                st.write("")
                st.write("")
                exibir_meta = st.checkbox("Comparar com Meta (Target)", value=True)

            # Busca dados
            df = get_dados_historicos(ids_inversores_finais, data_inicio, data_fim)

            if df.empty:
                st.info("Nenhum dado encontrado para esta seleção no período.")
            else:
                # Prepara DataFrame
                df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
                df['timestamp_local'] = df['timestamp_utc'].dt.tz_convert('America/Sao_Paulo')
                
                # Define coluna de agrupamento (Usina ou Inversor)
                if agrupar_por == "Usina":
                    group_col = 'nome_usina'
                else:
                    df['nome_inversor_unico'] = df['nome_usina'] + ' - ' + df['nome_inversor']
                    group_col = 'nome_inversor_unico'

                # --- LÓGICA DE CONSOLIDAÇÃO TEMPORAL (Diário / Mensal / Anual) ---
                if visao_temporal == "Diário":
                    rule = 'D'
                    format_data = '%d/%m/%Y'
                    hover_format = '%d/%m/%Y'
                elif visao_temporal == "Mensal":
                    rule = 'ME' # Month End
                    format_data = '%m/%Y'
                    hover_format = '%B %Y'
                else: # Anual
                    rule = 'YE' # Year End
                    format_data = '%Y'
                    hover_format = '%Y'

                df = df.set_index('timestamp_local')
                
                # Agrupa dados pela visão temporal escolhida e pelo grupo (Usina/Inversor)
                # Soma a energia (kWh)
                df_resampled = df.groupby([pd.Grouper(freq=rule), group_col])['energia_intervalo_wh'].sum().unstack(level=group_col).fillna(0)
                df_resampled = df_resampled / 1000.0 # Converte Wh para kWh
                
                # Formata o índice para ficar bonito no gráfico (remove horas)
                df_resampled.index = df_resampled.index.strftime(format_data)

                # --- CÁLCULO DA META (TARGET) ---
                # Meta estimada: Potência de Pico (kWp) * HSP (Horas de Sol Pleno - ex: 5h)
                # Ajustada para o período (Dia=1, Mês=30, Ano=365)
                capacidade_total_selecionada = usinas_sel_df['potencia_pico_kwp'].sum()
                
                fator_tempo = 1
                if visao_temporal == "Mensal": fator_tempo = 30
                if visao_temporal == "Anual": fator_tempo = 365
                
                meta_energia = float(capacidade_total_selecionada or 0) * 5.0 * fator_tempo
                
                # --- GRÁFICO 1: GERAÇÃO DE ENERGIA (BARRAS AGRUPADAS) ---
                st.subheader(f"Geração de Energia ({visao_temporal})")
                
                fig_bar = go.Figure()

                # Adiciona uma barra para cada Usina/Inversor
                for col in df_resampled.columns:
                    fig_bar.add_trace(go.Bar(
                        x=df_resampled.index,
                        y=df_resampled[col],
                        name=col
                    ))
                
                # Adiciona Linha de Meta
                if exibir_meta:
                    fig_bar.add_trace(go.Scatter(
                        x=df_resampled.index,
                        y=[meta_energia] * len(df_resampled),
                        mode='lines',
                        name='Meta (Estimada)',
                        line=dict(color='red', width=2, dash='dash')
                    ))

                fig_bar.update_layout(
                    xaxis_title="Período",
                    yaxis_title="Energia (kWh)",
                    barmode='group', # Isso faz as barras ficarem lado a lado!
                    hovermode="x unified",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    height=500
                )
                st.plotly_chart(fig_bar, use_container_width=True)

                # --- GRÁFICO 2: CURVA DE POTÊNCIA (Apenas se for Diário) ---
                # Não faz sentido ver curva de potência média em visão mensal/anual
                if visao_temporal == "Diário":
                    st.subheader("Curva de Potência (Detalhada)")
                    
                    # Pivot para potência (granularidade original)
                    df_potencia = df.pivot_table(index='timestamp_local', columns=group_col, values='potencia_ativa_kw', aggfunc='sum')
                    
                    fig_line = go.Figure()
                    for col in df_potencia.columns:
                        fig_line.add_trace(go.Scatter(
                            x=df_potencia.index,
                            y=df_potencia[col],
                            mode='lines',
                            name=col
                        ))
                    
                    fig_line.update_layout(
                        xaxis_title="Horário",
                        yaxis_title="Potência (kW)",
                        hovermode="x unified",
                        height=400,
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                    )
                    # Permite zoom no eixo X nativo do Plotly
                    st.plotly_chart(fig_line, use_container_width=True)

                # Tabela de Dados
                with st.expander(f"Ver dados consolidados ({visao_temporal})"):
                    st.dataframe(df_resampled.style.format("{:.2f}"))

        # ==============================================================================
        # ABA 2: COMPARAÇÃO DE DIAS
        # ==============================================================================
        with tab_comparacao:
            st.title("Comparação Detalhada")
            
            c1, c2 = st.columns([1, 3])
            with c1:
                st.subheader("Configuração")
                num_datas = st.number_input("Qtd Dias:", 2, 5, 2)
                datas_comp = []
                for i in range(num_datas):
                    d = st.date_input(f"Dia {i+1}", value=datetime.now().date() - timedelta(days=(i*7)+1), key=f"dc_{i}")
                    datas_comp.append(d)
                
                metrica = st.selectbox("Métrica:", ["Potência Ativa (kW)", "Energia Acumulada (kWh)"])
                if st.button("Comparar"): st.session_state['run_comp'] = True

            with c2:
                if st.session_state.get('run_comp'):
                    fig_comp = go.Figure()
                    stats = []
                    colors = px.colors.qualitative.Plotly

                    for idx, dt in enumerate(datas_comp):
                        df_d = get_dados_historicos(ids_inversores_finais, dt, dt)
                        if df_d.empty: continue
                        
                        # Processamento
                        df_d['timestamp_local'] = pd.to_datetime(df_d['timestamp_utc']).dt.tz_convert('America/Sao_Paulo')
                        df_agg = df_d.groupby('timestamp_local').agg({'potencia_ativa_kw':'sum', 'energia_intervalo_wh':'sum'}).reset_index()
                        
                        # Eixo X normalizado (Hora decimal)
                        df_agg['hora'] = df_agg['timestamp_local'].dt.hour + df_agg['timestamp_local'].dt.minute/60
                        df_agg['energia_acc'] = df_agg['energia_intervalo_wh'].cumsum() / 1000.0

                        y_val = df_agg['potencia_ativa_kw'] if metrica == "Potência Ativa (kW)" else df_agg['energia_acc']
                        
                        fig_comp.add_trace(go.Scatter(
                            x=df_agg['hora'], y=y_val, mode='lines', name=dt.strftime('%d/%m/%Y'),
                            line=dict(width=2, color=colors[idx % len(colors)])
                        ))
                        
                        stats.append({
                            "Data": dt.strftime('%d/%m/%Y'),
                            "Energia (kWh)": df_agg['energia_acc'].max(),
                            "Pico (kW)": df_agg['potencia_ativa_kw'].max()
                        })

                    fig_comp.update_layout(title=f"Comparativo: {metrica}", xaxis_title="Hora do Dia", hovermode="x unified", height=500)
                    st.plotly_chart(fig_comp, use_container_width=True)
                    
                    if stats:
                        st.dataframe(pd.DataFrame(stats).set_index("Data").style.format("{:.2f}"), use_container_width=True)

        # ==============================================================================
        # ABA 3: STATUS
        # ==============================================================================
        with tab_status_dia:
            st.header("Monitoramento em Tempo Real")
            if ids_inversores_finais:
                q = """
                SELECT i.nome_inversor, u.nome_usina, MAX(l.timestamp_utc) as ultimo_sinal
                FROM tbl_leituras l
                JOIN tbl_inversores i ON l.id_inversor = i.id_inversor
                JOIN tbl_usinas u ON i.id_usina = u.id_usina
                WHERE l.id_inversor = ANY(:ids)
                GROUP BY i.nome_inversor, u.nome_usina
                ORDER BY ultimo_sinal DESC
                """
                df_s = conn.query(q, params={"ids": ids_inversores_finais}, ttl=60)
                if not df_s.empty:
                    df_s['ultimo_sinal'] = pd.to_datetime(df_s['ultimo_sinal']).dt.tz_convert('America/Sao_Paulo')
                    
                    # Status check (Vermelho se > 30 min sem dados)
                    now = datetime.now(pytz.timezone('America/Sao_Paulo'))
                    df_s['status'] = df_s['ultimo_sinal'].apply(lambda x: "🔴 OFFLINE" if (now - x).total_seconds() > 1800 else "🟢 ONLINE")
                    
                    st.dataframe(df_s.style.format({"ultimo_sinal": lambda t: t.strftime("%d/%m %H:%M")}))
                else:
                    st.info("Sem dados recentes.")
            else:
                st.warning("Nenhum inversor selecionado.")