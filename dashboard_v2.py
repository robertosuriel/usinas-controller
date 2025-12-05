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
def init_connection():
    try:
        # Conecta usando os segredos configurados no .streamlit/secrets.toml ou no Cloud
        conn = st.connection("postgresql", type="sql")
        return conn
    except Exception as e:
        st.error(f"ERRO DE CONEXÃO: {e}")
        return None

conn = init_connection()

# --- FUNÇÕES DE BUSCA DE DADOS ---
@st.cache_data(ttl=600) 
def get_usinas():
    if conn:
        df = conn.query("SELECT id_usina, nome_usina, fabricante_api, potencia_pico_kwp FROM tbl_usinas ORDER BY nome_usina", ttl=600)
        return df
    return pd.DataFrame()

@st.cache_data(ttl=600)
def get_inversores_por_usinas(lista_ids_usinas: list):
    if not lista_ids_usinas or not conn: return pd.DataFrame()
    query = "SELECT id_inversor, nome_inversor, id_usina FROM tbl_inversores WHERE id_usina = ANY(:lista_ids) ORDER BY nome_inversor"
    df = conn.query(query, params={"lista_ids": lista_ids_usinas}, ttl=600)
    return df

@st.cache_data(ttl=300) 
def get_dados_historicos(lista_ids_inversores: list, data_inicio: date, data_fim: date):
    if not lista_ids_inversores or not conn: return pd.DataFrame()
    
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
    st.warning("Aguardando conexão com o banco...")
else:
    usinas_df = get_usinas()

    if usinas_df.empty:
        st.sidebar.error("Nenhuma usina encontrada.")
    else:
        # --- FILTRO 1: USINAS ---
        nomes_usinas_selecionadas = st.sidebar.multiselect(
            "Selecione as Usinas:",
            options=usinas_df['nome_usina'].unique(),
            default=usinas_df['nome_usina'].iloc[0] 
        )
        
        if not nomes_usinas_selecionadas:
            st.warning("Selecione ao menos uma usina para começar.")
            st.stop() 
            
        usinas_sel_df = usinas_df[usinas_df['nome_usina'].isin(nomes_usinas_selecionadas)]
        ids_usinas_selecionadas = [int(x) for x in usinas_sel_df['id_usina'].tolist()]
        
        # --- FILTRO 2: INVERSORES ---
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
            
        # --- FILTRO 3: DATAS ---
        st.sidebar.markdown("---")
        # Default: Início do ano até hoje
        data_padrao_inicio = date(datetime.now().year, 1, 1)
        data_inicio = st.sidebar.date_input("Data de Início", data_padrao_inicio)
        data_fim = st.sidebar.date_input("Data de Fim", datetime.now().date())

        # --- ABAS ---
        tab_resumo, tab_historico, tab_comparacao, tab_status_dia = st.tabs(["📊 Resumo Gerencial", "📈 Análise Histórica", "🔎 Comparação de Dias", "⚡ Status Real-Time"])

        # ==============================================================================
        # ABA 0: RESUMO GERENCIAL
        # ==============================================================================
        with tab_resumo:
            st.title("Resumo Gerencial")
            
            df = get_dados_historicos(ids_inversores_finais, data_inicio, data_fim)

            if df.empty:
                st.info("Nenhum dado encontrado para esta seleção no período.")
            else:
                df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
                df['timestamp_local'] = df['timestamp_utc'].dt.tz_convert('America/Sao_Paulo')
                df = df.set_index('timestamp_local')
                
                capacidade_total_sel = usinas_sel_df['potencia_pico_kwp'].sum()
                meta_diaria_base = float(capacidade_total_sel or 0) * 5.0

                total_gerado_periodo = df['energia_intervalo_wh'].sum() / 1000.0
                dias_periodo = (data_fim - data_inicio).days + 1
                meta_periodo = meta_diaria_base * dias_periodo
                perf_ratio = (total_gerado_periodo / meta_periodo * 100) if meta_periodo > 0 else 0

                kpi1, kpi2, kpi3 = st.columns(3)
                kpi1.metric("Energia Gerada Total", f"{total_gerado_periodo:,.0f} kWh")
                kpi2.metric("Meta Estimada (Período)", f"{meta_periodo:,.0f} kWh")
                kpi3.metric("Performance vs Meta", f"{perf_ratio:.1f}%", delta_color="normal")
                
                st.divider()

                col_ano, col_mes = st.columns(2)

                # 1. Dados Anuais
                df_ano = df.resample('YE')['energia_intervalo_wh'].sum() / 1000.0
                df_ano.index = df_ano.index.strftime('%Y')
                
                fig_ano = go.Figure()
                fig_ano.add_trace(go.Bar(
                    x=df_ano.index, y=df_ano.values, name='Realizado', marker_color='#2E86C1',
                    text=df_ano.values, texttemplate='%{text:.0f}', textposition='auto'
                ))
                # CORREÇÃO 1: Forçar eixo X a ser categoria (evita 2,024.8)
                fig_ano.update_layout(title="Energia por Ano (kWh)", height=300, xaxis=dict(type='category'))
                col_ano.plotly_chart(fig_ano, use_container_width=True)

                # 2. Dados Mensais
                df_mes = df.resample('ME')['energia_intervalo_wh'].sum() / 1000.0
                metas_mensais = [meta_diaria_base * 30] * len(df_mes) 
                
                fig_mes = go.Figure()
                fig_mes.add_trace(go.Bar(
                    x=df_mes.index.strftime('%b/%y'), y=df_mes.values, name='Realizado', marker_color='#1ABC9C'
                ))
                fig_mes.add_trace(go.Scatter(
                    x=df_mes.index.strftime('%b/%y'), y=metas_mensais, mode='lines+markers', name='Meta Mensal',
                    line=dict(color='red', width=2, dash='dash')
                ))
                # Mensal já é string formatada, então é categoria por padrão
                fig_mes.update_layout(title="Energia por Mês (kWh)", height=300, hovermode="x unified")
                col_mes.plotly_chart(fig_mes, use_container_width=True)

                # --- GRÁFICO MICRO (DIÁRIO) ---
                st.subheader("Energia Diária (Detalhada)")
                df_dia = df.resample('D')['energia_intervalo_wh'].sum() / 1000.0
                metas_diarias = [meta_diaria_base] * len(df_dia)

                fig_dia = go.Figure()
                fig_dia.add_trace(go.Bar(
                    x=df_dia.index.strftime('%d/%m'), y=df_dia.values, name='Realizado', marker_color='#9B59B6'
                ))
                fig_dia.add_trace(go.Scatter(
                    x=df_dia.index.strftime('%d/%m'), y=metas_diarias, mode='lines', name='Meta Diária',
                    line=dict(color='#F1C40F', width=3)
                ))
                fig_dia.update_layout(
                    title=f"Evolução Diária ({data_inicio.strftime('%d/%m')} a {data_fim.strftime('%d/%m')})",
                    xaxis_title="Dia", yaxis_title="kWh", hovermode="x unified", height=400,
                    xaxis=dict(type='category') # Garante que dias não virem números estranhos
                )
                st.plotly_chart(fig_dia, use_container_width=True)

        # ==============================================================================
        # ABA 1: ANÁLISE HISTÓRICA
        # ==============================================================================
        with tab_historico:
            st.title(f"Análise Técnica")
            
            c1, c2, c3 = st.columns([1, 1, 2])
            with c1:
                visao_temporal = st.selectbox("Visão Temporal:", ["Diário", "Mensal", "Anual"], key="vt_tec")
            with c2:
                agrupar_por = st.selectbox("Agrupar por:", ["Usina", "Inversor"], key="ag_tec")
            with c3:
                st.write("")
                st.write("")
                exibir_meta = st.checkbox("Comparar com Meta (Target)", value=True, key="cb_meta")

            if not df.empty:
                if agrupar_por == "Usina":
                    group_col = 'nome_usina'
                else:
                    df['nome_inversor_unico'] = df['nome_usina'] + ' - ' + df['nome_inversor']
                    group_col = 'nome_inversor_unico'

                if visao_temporal == "Diário":
                    rule = 'D'; format_data = '%d/%m/%Y'
                elif visao_temporal == "Mensal":
                    rule = 'ME'; format_data = '%m/%Y'
                else: 
                    rule = 'YE'; format_data = '%Y'
                
                df_resampled = df.groupby([pd.Grouper(freq=rule), group_col])['energia_intervalo_wh'].sum().unstack(level=group_col).fillna(0)
                df_resampled = df_resampled / 1000.0 
                df_resampled.index = df_resampled.index.strftime(format_data)

                fator_tempo = 1
                if visao_temporal == "Mensal": fator_tempo = 30
                if visao_temporal == "Anual": fator_tempo = 365
                meta_energia = meta_diaria_base * fator_tempo
                
                st.subheader(f"Geração de Energia ({visao_temporal})")
                fig_bar = go.Figure()
                for col in df_resampled.columns:
                    fig_bar.add_trace(go.Bar(x=df_resampled.index, y=df_resampled[col], name=col))
                
                if exibir_meta:
                    fig_bar.add_trace(go.Scatter(x=df_resampled.index, y=[meta_energia]*len(df_resampled), mode='lines', name='Meta', line=dict(color='red', width=2, dash='dash')))

                # CORREÇÃO 2: Forçar type='category' aqui também para evitar problemas em visão Anual
                fig_bar.update_layout(barmode='group', hovermode="x unified", height=500, xaxis=dict(type='category'))
                st.plotly_chart(fig_bar, use_container_width=True)

                if visao_temporal == "Diário":
                    st.subheader("Curva de Potência")
                    df_potencia = df.pivot_table(index='timestamp_local', columns=group_col, values='potencia_ativa_kw', aggfunc='sum')
                    fig_line = go.Figure()
                    for col in df_potencia.columns:
                        fig_line.add_trace(go.Scatter(x=df_potencia.index, y=df_potencia[col], mode='lines', name=col))
                    fig_line.update_layout(hovermode="x unified", height=450)
                    st.plotly_chart(fig_line, use_container_width=True)

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
                        df_d['timestamp_local'] = pd.to_datetime(df_d['timestamp_utc']).dt.tz_convert('America/Sao_Paulo')
                        df_agg = df_d.groupby('timestamp_local').agg({'potencia_ativa_kw':'sum', 'energia_intervalo_wh':'sum'}).reset_index()
                        df_agg['hora'] = df_agg['timestamp_local'].dt.hour + df_agg['timestamp_local'].dt.minute/60
                        df_agg['energia_acc'] = df_agg['energia_intervalo_wh'].cumsum() / 1000.0
                        y_val = df_agg['potencia_ativa_kw'] if metrica == "Potência Ativa (kW)" else df_agg['energia_acc']
                        
                        fig_comp.add_trace(go.Scatter(x=df_agg['hora'], y=y_val, mode='lines', name=dt.strftime('%d/%m/%Y'), line=dict(width=2, color=colors[idx % len(colors)])))
                        stats.append({"Data": dt.strftime('%d/%m/%Y'), "Energia (kWh)": df_agg['energia_acc'].max(), "Pico (kW)": df_agg['potencia_ativa_kw'].max()})

                    fig_comp.update_layout(title=f"Comparativo: {metrica}", xaxis_title="Hora do Dia", hovermode="x unified", height=500)
                    st.plotly_chart(fig_comp, use_container_width=True)
                    if stats: st.dataframe(pd.DataFrame(stats).set_index("Data").style.format("{:.2f}"), use_container_width=True)

        # ==============================================================================
        # ABA 3: STATUS
        # ==============================================================================
        with tab_status_dia:
            st.header("Monitoramento em Tempo Real")
            if ids_inversores_finais:
                q = """SELECT i.nome_inversor, u.nome_usina, MAX(l.timestamp_utc) as ultimo_sinal FROM tbl_leituras l JOIN tbl_inversores i ON l.id_inversor = i.id_inversor JOIN tbl_usinas u ON i.id_usina = u.id_usina WHERE l.id_inversor = ANY(:ids) GROUP BY i.nome_inversor, u.nome_usina ORDER BY ultimo_sinal DESC"""
                df_s = conn.query(q, params={"ids": ids_inversores_finais}, ttl=60)
                if not df_s.empty:
                    df_s['ultimo_sinal'] = pd.to_datetime(df_s['ultimo_sinal']).dt.tz_convert('America/Sao_Paulo')
                    now = datetime.now(pytz.timezone('America/Sao_Paulo'))
                    df_s['status'] = df_s['ultimo_sinal'].apply(lambda x: "🔴 OFFLINE" if (now - x).total_seconds() > 1800 else "🟢 ONLINE")
                    st.dataframe(df_s.style.format({"ultimo_sinal": lambda t: t.strftime("%d/%m %H:%M")}))
                else: st.info("Sem dados recentes.")
            else: st.warning("Nenhum inversor selecionado.")