import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, date
import plotly.graph_objects as go
import plotly.express as px
import pytz 

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(
    page_title="Dashboard de Usinas | SOL Online",
    page_icon="☀️",
    layout="wide"
)

# ==============================================================================
# ESTILIZAÇÃO CSS (IDENTIDADE VISUAL SOL ONLINE)
# ==============================================================================
st.markdown("""
<style>
    /* Sidebar Rosa */
    [data-testid="stSidebar"] {
        background-color: #E91E63;
    }
    [data-testid="stSidebar"] h1, [data-testid="stSidebar"] h2, [data-testid="stSidebar"] h3, 
    [data-testid="stSidebar"] label, [data-testid="stSidebar"] .stMarkdown p {
        color: white !important;
    }
    [data-testid="stSidebar"] span { color: #f0f0f0; }
    [data-testid="stSidebar"] hr { border-color: rgba(255, 255, 255, 0.4) !important; }

    /* Títulos e KPIs em Rosa */
    h1, h2, h3, [data-testid="stMetricValue"] {
        color: #E91E63 !important;
    }
    :root { --primary-color: #00E676; }
</style>
""", unsafe_allow_html=True)

SOL_COLORS = ['#00E676', '#E91E63', '#FFC107', '#2196F3', '#9C27B0']

# ------------------------------------------------------------------
# 1. FUNÇÕES DE TARGETS (METAS)
# ------------------------------------------------------------------
@st.cache_data(ttl=3600)
def load_targets():
    """Carrega o Excel e cria um mapa DIÁRIO: {(Mes, Dia): Valor}"""
    try:
        mapeamento_targets = {
            'Riachão': {'aba_max': 'Riachão_Max', 'aba_min': 'Riachão_Min', 'potencia_ref': 118},
            'Xique-xique': {'aba_max': 'Xique-xique_Max', 'aba_min': 'Xique-xique_Min', 'potencia_ref': 118},
            'Xique-xique_132': {'aba_max': 'Xique-xique_Max 132kWp', 'aba_min': 'Xique-xique_Min 132kWp', 'potencia_ref': 132}
        }
        
        targets_dict = {}
        
        for usina_key, config in mapeamento_targets.items():
            try:
                df_max = pd.read_excel('Targets das Usinas_add 132.xlsx', sheet_name=config['aba_max'], header=2)
                df_min = pd.read_excel('Targets das Usinas_add 132.xlsx', sheet_name=config['aba_min'], header=2)
            except Exception:
                continue

            diario_max = {}
            diario_min = {}
            
            for mes_idx in range(12): 
                mes_num = mes_idx + 1 
                if mes_idx < len(df_max):
                    for col in df_max.columns:
                        try:
                            dia = int(float(str(col)))
                            if 1 <= dia <= 31:
                                val_max = df_max.iloc[mes_idx, df_max.columns.get_loc(col)]
                                val_min = df_min.iloc[mes_idx, df_min.columns.get_loc(col)]
                                if pd.notna(val_max): diario_max[(mes_num, dia)] = float(val_max)
                                if pd.notna(val_min): diario_min[(mes_num, dia)] = float(val_min)
                        except: continue
            
            targets_dict[usina_key] = {
                'max': diario_max,
                'min': diario_min,
                'potencia_ref': config['potencia_ref']
            }
        return targets_dict
    except Exception as e:
        st.error(f"Erro ao ler targets: {e}")
        return {}

@st.cache_data
def get_target_range_diario(nome_usina, potencia_kwp, data_alvo):
    """
    Retorna o target exato para um DIA específico com as regras atualizadas.
    """
    targets_data = load_targets()
    
    nome_lower = nome_usina.lower()
    usina_key = 'Xique-xique' # Default geral
    
    # --- REGRAS DE MAPEAMENTO ESPECÍFICAS ---
    
    # 1. Blue Solutions e Riachão -> RIACHÃO
    if any(k in nome_lower for k in ['riachão', 'riachao', 'jacuipe', 'blue']):
        usina_key = 'Riachão'
        
    # 2. Usina 10 -> XIQUE-XIQUE 132 (Forçado)
    elif 'usina 10' in nome_lower:
        usina_key = 'Xique-xique_132'
        
    # 3. Usina 11 e Usina 12 -> XIQUE-XIQUE (Normal/118)
    elif any(k in nome_lower for k in ['usina 11', 'usina 12']):
        usina_key = 'Xique-xique'
        
    # 4. Demais Usinas -> Regra de Potência
    else:
        if potencia_kwp and potencia_kwp >= 130:
            usina_key = 'Xique-xique_132'
        else:
            usina_key = 'Xique-xique'
    
    # --- BUSCA DO VALOR ---
    target_min, target_max = 0, 0
    if usina_key in targets_data:
        t_info = targets_data[usina_key]
        chave_dia = (data_alvo.month, data_alvo.day)
        
        t_max = t_info['max'].get(chave_dia, 500)
        t_min = t_info['min'].get(chave_dia, 450)
        
        # Escala pela potência
        if potencia_kwp and t_info['potencia_ref']:
            fator = potencia_kwp / t_info['potencia_ref']
            target_min *= fator
            target_max *= fator
    else:
        # Fallback
        p_def = potencia_kwp or 100
        target_min, target_max = p_def * 4.5, p_def * 5.5
            
    return target_min, target_max

def calcular_targets_periodo(usinas_df, timestamps, fator_tempo=1):
    """Calcula listas de targets somados para plotagem"""
    targets_min = []
    targets_max = []
    
    for ts in timestamps:
        data_ref = ts.date() if isinstance(ts, pd.Timestamp) or isinstance(ts, datetime) else ts
        t_total_min = 0
        t_total_max = 0
        
        for _, row in usinas_df.iterrows():
            t_min, t_max = get_target_range_diario(row['nome_usina'], row['potencia_pico_kwp'], data_ref)
            
            if fator_tempo == 'mes_real':
                import calendar
                _, dias_no_mes = calendar.monthrange(data_ref.year, data_ref.month)
                fator = dias_no_mes
            else:
                fator = fator_tempo
                
            t_total_min += t_min * fator
            t_total_max += t_max * fator
            
        targets_min.append(t_total_min)
        targets_max.append(t_total_max)
        
    return targets_min, targets_max

# ------------------------------------------------------------------
# 2. CONEXÃO E DADOS
# ------------------------------------------------------------------
@st.cache_resource
def init_connection():
    try:
        return st.connection("postgresql", type="sql")
    except Exception as e:
        st.error(f"Erro BD: {e}")
        return None

conn = init_connection()

@st.cache_data(ttl=600)
def get_usinas():
    if conn: return conn.query("SELECT id_usina, nome_usina, fabricante_api, potencia_pico_kwp FROM tbl_usinas ORDER BY nome_usina", ttl=600)
    return pd.DataFrame()

@st.cache_data(ttl=600)
def get_inversores_por_usinas(lista_ids):
    if not lista_ids or not conn: return pd.DataFrame()
    return conn.query("SELECT id_inversor, nome_inversor, id_usina FROM tbl_inversores WHERE id_usina = ANY(:ids) ORDER BY nome_inversor", params={"ids": lista_ids}, ttl=600)

@st.cache_data(ttl=300)
def get_dados_historicos(lista_ids, data_ini, data_fim):
    if not lista_ids or not conn: return pd.DataFrame()
    query = """
    SELECT l.timestamp_utc, l.potencia_ativa_kw, l.energia_intervalo_wh, i.nome_inversor, u.nome_usina, u.id_usina
    FROM tbl_leituras l
    JOIN tbl_inversores i ON l.id_inversor = i.id_inversor
    JOIN tbl_usinas u ON i.id_usina = u.id_usina
    WHERE l.id_inversor = ANY(:ids) AND l.timestamp_utc >= :d_ini AND l.timestamp_utc < :d_fim
    ORDER BY l.timestamp_utc
    LIMIT 100000;
    """
    try:
        df = conn.query(query, params={"ids": lista_ids, "d_ini": data_ini, "d_fim": data_fim + timedelta(days=1)}, ttl=300)
        if len(df) >= 100000: st.toast("⚠️ Dados truncados. Reduza o período.", icon="📉")
        return df
    except Exception as e:
        st.error(f"Erro dados: {e}")
        return pd.DataFrame()

# ------------------------------------------------------------------
# 3. INTERFACE DO USUÁRIO
# ------------------------------------------------------------------

try:
    st.sidebar.image("solonline-foto.png", use_container_width=True)
except:
    st.sidebar.markdown("<h2 style='color: white; text-align: center;'>🌞 SOL ONLINE</h2>", unsafe_allow_html=True)

st.sidebar.title("Filtros")

if conn:
    usinas_df = get_usinas()
    if not usinas_df.empty:
        sel_usinas = st.sidebar.multiselect("Usinas:", usinas_df['nome_usina'].unique(), default=usinas_df['nome_usina'].iloc[0])
        if not sel_usinas: st.stop()
        
        usinas_sel_df = usinas_df[usinas_df['nome_usina'].isin(sel_usinas)]
        ids_usinas = [int(x) for x in usinas_sel_df['id_usina']]
        
        inv_df = get_inversores_por_usinas(ids_usinas)
        sel_inv = st.sidebar.multiselect("Inversores:", ["-- TODOS --"] + inv_df['nome_inversor'].unique().tolist(), default="-- TODOS --")
        ids_inv_finais = [int(x) for x in inv_df['id_inversor']] if "-- TODOS --" in sel_inv or not sel_inv else [int(x) for x in inv_df[inv_df['nome_inversor'].isin(sel_inv)]['id_inversor']]

        st.sidebar.markdown("---")
        hoje = datetime.now().date()
        d_ini = st.sidebar.date_input("Início", hoje - timedelta(days=7))
        d_fim = st.sidebar.date_input("Fim", hoje)
        if (d_fim - d_ini).days > 31: st.sidebar.info("💡 Períodos longos podem ser lentos.")

        with st.spinner("Atualizando dados..."):
            main_df = get_dados_historicos(ids_inv_finais, d_ini, d_fim)
            if not main_df.empty:
                main_df['ts'] = pd.to_datetime(main_df['timestamp_utc']).dt.tz_convert('America/Sao_Paulo')
                main_df_indexed = main_df.set_index('ts')
            else:
                main_df_indexed = pd.DataFrame()

        tab_resumo, tab_hist, tab_comp, tab_status = st.tabs(["📊 Resumo Gerencial", "📈 Análise Técnica", "🔎 Comparação", "⚡ Status"])

        # === ABA 1: RESUMO ===
        with tab_resumo:
            st.title("Resumo Gerencial")
            if not main_df_indexed.empty:
                total_gerado = main_df_indexed['energia_intervalo_wh'].sum() / 1000.0
                meta_kpi_min, meta_kpi_max = calcular_targets_periodo(usinas_sel_df, pd.date_range(d_ini, d_fim), fator_tempo=1)
                meta_periodo_avg = (sum(meta_kpi_min) + sum(meta_kpi_max)) / 2
                perf = (total_gerado / meta_periodo_avg * 100) if meta_periodo_avg > 0 else 0

                c1, c2, c3 = st.columns(3)
                c1.metric("Gerado Total", f"{total_gerado:,.0f} kWh".replace(',', '.'))
                c2.metric("Meta Estimada", f"{meta_periodo_avg:,.0f} kWh".replace(',', '.'))
                c3.metric("Performance", f"{perf:.1f}%")
                st.divider()

                c_ano, c_mes = st.columns(2)
                
                # Anual
                df_ano = main_df_indexed.resample('YE')['energia_intervalo_wh'].sum() / 1000.0
                df_ano.index = df_ano.index.strftime('%Y')
                fig_ano = go.Figure(go.Bar(x=df_ano.index, y=df_ano.values, text=[f"{v:,.0f}".replace(',','.') for v in df_ano.values], textposition='auto', marker_color='#00E676', name='Realizado'))
                fig_ano.update_layout(title="Energia/Ano (kWh)", height=300, xaxis=dict(type='category'), yaxis=dict(tickformat=',.0f'))
                c_ano.plotly_chart(fig_ano, use_container_width=True)

                # Mensal
                df_mes = main_df_indexed.resample('ME')['energia_intervalo_wh'].sum() / 1000.0
                t_mes_min, t_mes_max = calcular_targets_periodo(usinas_sel_df, df_mes.index, fator_tempo='mes_real')
                
                fig_mes = go.Figure()
                fig_mes.add_trace(go.Bar(x=df_mes.index.strftime('%b/%y'), y=df_mes.values, marker_color='#00E676', name='Realizado'))
                fig_mes.add_trace(go.Scatter(x=df_mes.index.strftime('%b/%y'), y=t_mes_max, mode='lines', line=dict(width=0), showlegend=False, hoverinfo='skip'))
                fig_mes.add_trace(go.Scatter(
                    x=df_mes.index.strftime('%b/%y'), y=t_mes_min, mode='lines', fill='tonexty', 
                    fillcolor='rgba(233, 30, 99, 0.2)', line=dict(color='rgba(233, 30, 99, 0.8)', width=1, dash='dot'),
                    name='Meta (Faixa)', hovertemplate='Meta: %{y:,.0f} - %{customdata:,.0f} kWh', customdata=t_mes_max
                ))
                fig_mes.update_layout(title="Energia/Mês (kWh)", height=300, hovermode="x unified", yaxis=dict(tickformat=',.0f'))
                c_mes.plotly_chart(fig_mes, use_container_width=True)

                # Diário
                st.subheader("Evolução Diária")
                df_dia = main_df_indexed.resample('D')['energia_intervalo_wh'].sum() / 1000.0
                t_dia_min, t_dia_max = calcular_targets_periodo(usinas_sel_df, df_dia.index, fator_tempo=1)

                fig_dia = go.Figure()
                fig_dia.add_trace(go.Bar(
                    x=df_dia.index.strftime('%d/%m'), y=df_dia.values, marker_color='#00E676', name='Realizado',
                    text=[f"{v:,.0f}".replace(',','.') for v in df_dia.values], textposition='auto'
                ))
                fig_dia.add_trace(go.Scatter(x=df_dia.index.strftime('%d/%m'), y=t_dia_max, mode='lines', line=dict(width=0), showlegend=False, hoverinfo='skip'))
                fig_dia.add_trace(go.Scatter(
                    x=df_dia.index.strftime('%d/%m'), y=t_dia_min, mode='lines', fill='tonexty',
                    fillcolor='rgba(233, 30, 99, 0.2)', line=dict(color='rgba(233, 30, 99, 0.8)', width=2),
                    name='Meta (Faixa)', hovertemplate='Meta: %{y:,.0f} - %{customdata:,.0f} kWh', customdata=t_dia_max
                ))
                fig_dia.update_layout(height=400, hovermode="x unified", xaxis=dict(type='category'), yaxis=dict(tickformat=',.0f'))
                st.plotly_chart(fig_dia, use_container_width=True)
            else: st.info("Sem dados no período.")

        # === ABA 2: TÉCNICA ===
        with tab_hist:
            st.title("Análise Técnica")
            c1, c2, c3 = st.columns([1,1,2])
            visao = c1.selectbox("Visão:", ["Diário", "Mensal", "Anual"])
            agrup = c2.selectbox("Agrupar:", ["Usina", "Inversor"])
            meta_on = c3.checkbox("Exibir Meta (Individual)", True)

            if not main_df_indexed.empty:
                grp = 'nome_usina' if agrup == "Usina" else 'nome_inversor' 
                df_tec = main_df_indexed.copy()
                if agrup == "Inversor": df_tec['nome_inversor'] = df_tec['nome_usina'] + ' - ' + df_tec['nome_inversor']

                rule = 'D' if visao == "Diário" else ('ME' if visao == "Mensal" else 'YE')
                fmt = '%d/%m/%Y' if visao == "Diário" else ('%m/%Y' if visao == "Mensal" else '%Y')

                df_res = df_tec.groupby([pd.Grouper(freq=rule), grp])['energia_intervalo_wh'].sum().unstack(level=grp).fillna(0) / 1000.0
                df_res.index = df_res.index.strftime(fmt)

                fig = go.Figure()
                for i, col_name in enumerate(df_res.columns):
                    color = SOL_COLORS[i % len(SOL_COLORS)]
                    fig.add_trace(go.Bar(x=df_res.index, y=df_res[col_name], name=col_name, marker_color=color))

                    if meta_on and agrup == "Usina":
                        usina_info = usinas_sel_df[usinas_sel_df['nome_usina'] == col_name]
                        if not usina_info.empty:
                            ts_idx = [datetime.strptime(d, fmt) for d in df_res.index]
                            ft = 1
                            if visao == "Mensal": ft = 'mes_real'
                            if visao == "Anual": ft = 365
                            
                            t_min, t_max = calcular_targets_periodo(usina_info, pd.DatetimeIndex(ts_idx), fator_tempo=ft)
                            
                            fig.add_trace(go.Scatter(x=df_res.index, y=t_max, mode='lines', line=dict(color=color, width=2, dash='dot'), showlegend=False, hoverinfo='skip'))
                            fig.add_trace(go.Scatter(
                                x=df_res.index, y=t_min, mode='lines',
                                line=dict(color=color, width=2, dash='dot'),
                                name=f'Meta Min {col_name}',
                                hovertemplate=f'{col_name} Meta: %{{y:,.0f}} - {t_max[0]:,.0f} kWh<extra></extra>'
                            ))

                fig.update_layout(barmode='group', hovermode="x unified", height=500, xaxis=dict(type='category'))
                st.plotly_chart(fig, use_container_width=True)

                if visao == "Diário":
                    st.subheader("Curva de Potência")
                    df_pot = df_tec.pivot_table(index='ts', columns=grp, values='potencia_ativa_kw', aggfunc='sum')
                    fig_l = go.Figure()
                    for i, col in enumerate(df_pot.columns): 
                        fig_l.add_trace(go.Scatter(x=df_pot.index, y=df_pot[col], mode='lines', name=col, line=dict(color=SOL_COLORS[i % len(SOL_COLORS)])))
                    fig_l.update_layout(hovermode="x unified", height=450)
                    st.plotly_chart(fig_l, use_container_width=True)

        # === ABA 3: COMPARAÇÃO ===
        with tab_comp:
            st.title("Comparação")
            c1, c2 = st.columns([1,3])
            with c1:
                qtd = st.number_input("Dias:", 2, 5, 2)
                datas = [st.date_input(f"Dia {i+1}", hoje - timedelta(days=(i*7)+1)) for i in range(qtd)]
                metr = st.selectbox("Métrica:", ["Potência (kW)", "Energia (kWh)"])
            
            with c2:
                fig_c = go.Figure()
                has_data = False
                for i, d in enumerate(datas):
                    dd = get_dados_historicos(ids_inv_finais, d, d)
                    if dd.empty: continue
                    has_data = True
                    dd['ts'] = pd.to_datetime(dd['timestamp_utc']).dt.tz_convert('America/Sao_Paulo')
                    agg = dd.groupby('ts').agg({'potencia_ativa_kw':'sum', 'energia_intervalo_wh':'sum'}).reset_index()
                    agg['h'] = agg['ts'].dt.hour + agg['ts'].dt.minute/60
                    agg['h_str'] = agg['ts'].dt.strftime('%H:%M')
                    agg['e'] = agg['energia_intervalo_wh'].cumsum()/1000
                    y = agg['potencia_ativa_kw'] if metr == "Potência (kW)" else agg['e']
                    
                    color = SOL_COLORS[i % len(SOL_COLORS)]
                    fig_c.add_trace(go.Scatter(
                        x=agg['h'], y=y, name=d.strftime('%d/%m'), 
                        customdata=agg['h_str'], hovertemplate='<b>%{customdata}</b>: %{y:.2f}',
                        line=dict(color=color, width=3)
                    ))
                
                if has_data:
                    fig_c.update_layout(title=f"Comparativo: {metr}", xaxis_title="Hora", hovermode="x unified", height=500, xaxis=dict(tickmode='array', tickvals=[0,6,12,18,24], ticktext=['00h','06h','12h','18h','24h']))
                    st.plotly_chart(fig_c, use_container_width=True)
                else: st.info("Selecione datas com dados.")

        # === ABA 4: STATUS ===
        with tab_status:
            st.header("Status Real-Time")
            if ids_inv_finais:
                q = "SELECT i.nome_inversor, u.nome_usina, MAX(l.timestamp_utc) as u_sinal FROM tbl_leituras l JOIN tbl_inversores i ON l.id_inversor = i.id_inversor JOIN tbl_usinas u ON i.id_usina = u.id_usina WHERE l.id_inversor = ANY(:ids) GROUP BY 1, 2 ORDER BY 3 DESC"
                ds = conn.query(q, params={"ids": ids_inv_finais}, ttl=60)
                if not ds.empty:
                    ds['u_sinal'] = pd.to_datetime(ds['u_sinal']).dt.tz_convert('America/Sao_Paulo')
                    ds['status'] = ds['u_sinal'].apply(lambda x: "🔴 OFF" if (datetime.now(pytz.timezone('America/Sao_Paulo')) - x).total_seconds() > 1800 else "🟢 ON")
                    st.dataframe(ds.style.format({"u_sinal": lambda t: t.strftime("%d/%m %H:%M")}))
                    
