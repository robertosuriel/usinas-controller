import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, date, time
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
# ESTILIZAÇÃO CSS
# ==============================================================================
st.markdown("""
<style>
    [data-testid="stSidebar"] { background-color: #E91E63; }
    [data-testid="stSidebar"] h1, [data-testid="stSidebar"] h2, [data-testid="stSidebar"] label, [data-testid="stSidebar"] p { color: white !important; }
    [data-testid="stSidebar"] span { color: #f0f0f0; }
    [data-testid="stSidebar"] hr { border-color: rgba(255, 255, 255, 0.4) !important; }
    h1, h2, h3, [data-testid="stMetricValue"] { color: #E91E63 !important; }
    [data-testid="stMetricLabel"] { color: #555; }
    :root { --primary-color: #00E676; }
</style>
""", unsafe_allow_html=True)

SOL_COLORS = ['#00E676', '#E91E63', '#FFC107', '#2196F3', '#9C27B0']

# ------------------------------------------------------------------
# CONEXÃO E DADOS
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
    if conn: 
        return conn.query("""
            SELECT id_usina, nome_usina, fabricante_api, potencia_pico_kwp, target_profile 
            FROM tbl_usinas 
            ORDER BY nome_usina
        """, ttl=600)
    return pd.DataFrame()

@st.cache_data(ttl=600)
def get_inversores_por_usinas(lista_ids):
    if not lista_ids or not conn: return pd.DataFrame()
    return conn.query("SELECT id_inversor, nome_inversor, id_usina FROM tbl_inversores WHERE id_usina = ANY(:ids) ORDER BY nome_inversor", params={"ids": lista_ids}, ttl=600)

@st.cache_data(ttl=300)
def get_dados_completos(lista_ids_inv, lista_perfis, dt_ini_utc, dt_fim_utc, date_ini_br, date_fim_br):
    if not lista_ids_inv or not conn: return pd.DataFrame(), pd.DataFrame()
    
    # 1. Geração
    q_gen = """
    SELECT l.timestamp_utc, l.potencia_ativa_kw, l.energia_intervalo_wh, i.nome_inversor, u.nome_usina, u.id_usina, u.target_profile, u.potencia_pico_kwp
    FROM tbl_leituras l
    JOIN tbl_inversores i ON l.id_inversor = i.id_inversor
    JOIN tbl_usinas u ON i.id_usina = u.id_usina
    WHERE l.id_inversor = ANY(:ids) AND l.timestamp_utc >= :d_ini AND l.timestamp_utc < :d_fim
    ORDER BY l.timestamp_utc
    LIMIT 150000;
    """
    
    # 2. Targets
    q_target = """
    SELECT target_profile, data_referencia, val_min, val_max
    FROM tbl_targets
    WHERE target_profile = ANY(:perfis) 
      AND data_referencia >= :d_ini_date 
      AND data_referencia <= :d_fim_date
    """
    
    try:
        df_gen = conn.query(q_gen, params={"ids": lista_ids_inv, "d_ini": dt_ini_utc, "d_fim": dt_fim_utc}, ttl=300)
        
        if not df_gen.empty:
            perfis = [p for p in df_gen['target_profile'].unique() if p]
            if perfis:
                df_targets = conn.query(q_target, params={"perfis": perfis, "d_ini_date": date_ini_br, "d_fim_date": date_fim_br}, ttl=3600)
            else:
                df_targets = pd.DataFrame()
        else:
            df_targets = pd.DataFrame()
            
        return df_gen, df_targets
    except Exception as e:
        st.error(f"Erro dados: {e}")
        return pd.DataFrame(), pd.DataFrame()

# ------------------------------------------------------------------
# CÁLCULO DE META
# ------------------------------------------------------------------
def get_meta_periodo(df_targets, usinas_df, data_range):
    if df_targets.empty or usinas_df.empty:
        return [0]*len(data_range), [0]*len(data_range)
    
    df_targets['data_str'] = pd.to_datetime(df_targets['data_referencia']).dt.date
    target_map = df_targets.set_index(['target_profile', 'data_str'])[['val_min', 'val_max']].to_dict('index')
    usinas_map = usinas_df.set_index('nome_usina')[['target_profile', 'potencia_pico_kwp']].to_dict('index')
    
    lista_min, lista_max = [], []
    
    for dia in data_range:
        dia_date = dia.date() if isinstance(dia, pd.Timestamp) else dia
        soma_min, soma_max = 0, 0
        
        for nome_usina, info in usinas_map.items():
            perfil = info.get('target_profile', 'Xique-xique')
            vals = target_map.get((perfil, dia_date))
            if vals:
                soma_min += vals['val_min']
                soma_max += vals['val_max']
            
        lista_min.append(soma_min)
        lista_max.append(soma_max)
        
    return lista_min, lista_max

# ------------------------------------------------------------------
# INTERFACE
# ------------------------------------------------------------------

try:
    st.sidebar.image("solonline-foto.png", use_container_width=True)
except:
    st.sidebar.markdown("### 🌞 SOL ONLINE")

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

        # Timezone Correction
        tz_br = pytz.timezone('America/Sao_Paulo')
        dt_ini_br = datetime.combine(d_ini, time.min)
        dt_ini_br = tz_br.localize(dt_ini_br)
        dt_ini_utc = dt_ini_br.astimezone(pytz.utc)
        
        dt_fim_br = datetime.combine(d_fim + timedelta(days=1), time.min)
        dt_fim_br = tz_br.localize(dt_fim_br)
        dt_fim_utc = dt_fim_br.astimezone(pytz.utc)

        with st.spinner("Carregando dados..."):
            main_df, targets_db_df = get_dados_completos(
                ids_inv_finais, 
                usinas_sel_df['target_profile'].unique().tolist(), 
                dt_ini_utc, 
                dt_fim_utc,
                d_ini,
                d_fim
            )
            
            if not main_df.empty:
                main_df['ts'] = pd.to_datetime(main_df['timestamp_utc']).dt.tz_convert('America/Sao_Paulo')
                main_df_indexed = main_df.set_index('ts')
            else:
                main_df_indexed = pd.DataFrame()

        # === NOMES DAS ABAS COMPLETOS (RECUPERADO) ===
        tab1, tab2, tab3, tab4 = st.tabs(["📊 Resumo Gerencial", "📈 Análise Técnica", "🔎 Comparação", "⚡ Status"])

        # === ABA 1: RESUMO ===
        with tab1:
            st.title("Resumo Gerencial")
            if not main_df_indexed.empty:
                total_gerado = main_df_indexed['energia_intervalo_wh'].sum() / 1000.0
                dias_range = pd.date_range(d_ini, d_fim)
                
                lista_meta_min, lista_meta_max = get_meta_periodo(targets_db_df, usinas_sel_df, dias_range)
                total_meta_min = sum(lista_meta_min)
                total_meta_max = sum(lista_meta_max)
                meta_media = (total_meta_min + total_meta_max) / 2
                perf = (total_gerado / meta_media * 100) if meta_media > 0 else 0

                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Gerado Total", f"{total_gerado:,.0f} kWh".replace(',', '.'))
                c2.metric("Meta Mínima", f"{total_meta_min:,.0f} kWh".replace(',', '.'))
                c3.metric("Meta Máxima", f"{total_meta_max:,.0f} kWh".replace(',', '.'))
                c4.metric("Performance", f"{perf:.1f}%")
                
                st.divider()

                col_a, col_m = st.columns(2)
                
                # ANUAL
                df_ano = main_df_indexed.resample('YE')['energia_intervalo_wh'].sum() / 1000.0
                df_ano.index = df_ano.index.strftime('%Y')
                fig_a = go.Figure(go.Bar(x=df_ano.index, y=df_ano.values, text=[f"{v:,.0f}" for v in df_ano.values], textposition='auto', marker_color='#00E676', name='Realizado'))
                fig_a.update_layout(title="Energia Anual", height=300)
                col_a.plotly_chart(fig_a, use_container_width=True)

                # MENSAL
                df_mes = main_df_indexed.resample('ME')['energia_intervalo_wh'].sum() / 1000.0
                fig_m = go.Figure(go.Bar(x=df_mes.index.strftime('%b/%y'), y=df_mes.values, marker_color='#00E676', name='Realizado'))
                
                if not df_mes.empty:
                    m_start = df_mes.index.min() - pd.offsets.MonthBegin(1)
                    m_end = df_mes.index.max() + pd.offsets.MonthEnd(0)
                    r_mes = pd.date_range(m_start, m_end)
                    dm_min, dm_max = get_meta_periodo(targets_db_df, usinas_sel_df, r_mes)
                    
                    df_m_temp = pd.DataFrame({'min': dm_min, 'max': dm_max}, index=r_mes).resample('ME').sum()
                    df_m_final = df_m_temp.loc[df_m_temp.index.intersection(df_mes.index)]
                    
                    fig_m.add_trace(go.Scatter(x=df_m_final.index.strftime('%b/%y'), y=df_m_final['max'], mode='lines', line=dict(color='rgba(233,30,99,0.6)', width=1, dash='dash'), name='Meta Máxima', showlegend=True))
                    fig_m.add_trace(go.Scatter(x=df_m_final.index.strftime('%b/%y'), y=df_m_final['min'], mode='lines', fill='tonexty', fillcolor='rgba(233,30,99,0.1)', line=dict(color='rgba(233,30,99,0.6)', width=1, dash='dash'), name='Meta Mínima', showlegend=True))

                fig_m.update_layout(title="Energia Mensal", height=300, hovermode="x unified")
                col_m.plotly_chart(fig_m, use_container_width=True)

                # DIÁRIO
                st.subheader("Evolução Diária")
                df_d = main_df_indexed.resample('D')['energia_intervalo_wh'].sum() / 1000.0
                t_d_min, t_d_max = get_meta_periodo(targets_db_df, usinas_sel_df, df_d.index)
                
                fig_d = go.Figure()
                fig_d.add_trace(go.Bar(x=df_d.index.strftime('%d/%m'), y=df_d.values, marker_color='#00E676', name='Realizado'))
                fig_d.add_trace(go.Scatter(x=df_d.index.strftime('%d/%m'), y=t_d_max, mode='lines', line=dict(color='rgba(233,30,99,0.8)', width=1, dash='dash'), name='Meta Máxima', showlegend=True))
                fig_d.add_trace(go.Scatter(x=df_d.index.strftime('%d/%m'), y=t_d_min, mode='lines', fill='tonexty', fillcolor='rgba(233,30,99,0.1)', line=dict(color='rgba(233,30,99,0.8)', width=1, dash='dash'), name='Meta Mínima', showlegend=True))
                
                fig_d.update_layout(height=400, hovermode="x unified")
                st.plotly_chart(fig_d, use_container_width=True)
            else: st.info("Sem dados.")

        # === ABA 2: TÉCNICA ===
        with tab2:
            st.title("Análise Técnica")
            c1, c2, c3 = st.columns([1,1,2])
            visao = c1.selectbox("Visão", ["Diário", "Mensal"])
            agrup = c2.selectbox("Agrupar", ["Usina", "Inversor"])
            meta_on = c3.checkbox("Exibir Metas", True)

            if not main_df_indexed.empty:
                grp = 'nome_usina' if agrup == "Usina" else 'nome_inversor'
                rule = 'D' if visao == "Diário" else 'ME'
                fmt = '%d/%m' if visao == "Diário" else '%b/%y'
                
                df_tec = main_df_indexed.copy()
                if agrup == "Inversor": df_tec['nome_inversor'] = df_tec['nome_usina'] + ' - ' + df_tec['nome_inversor']
                
                # Gráfico de Barras (Energia)
                df_res = df_tec.groupby([pd.Grouper(freq=rule), grp])['energia_intervalo_wh'].sum().unstack(level=grp).fillna(0) / 1000.0
                df_res.index = df_res.index.strftime(fmt)

                fig = go.Figure()
                idx_dates = df_tec.groupby([pd.Grouper(freq=rule)]).first().index

                for i, col_name in enumerate(df_res.columns):
                    color = SOL_COLORS[i % len(SOL_COLORS)]
                    fig.add_trace(go.Bar(x=df_res.index, y=df_res[col_name], name=col_name, marker_color=color))
                    
                    if meta_on and agrup == "Usina":
                        u_info = usinas_sel_df[usinas_sel_df['nome_usina'] == col_name]
                        if not u_info.empty:
                            if visao == "Diário":
                                tm_min, tm_max = get_meta_periodo(targets_db_df, u_info, idx_dates)
                            else:
                                full_range = pd.date_range(idx_dates.min(), idx_dates.max() + pd.offsets.MonthEnd(0))
                                d_min, d_max = get_meta_periodo(targets_db_df, u_info, full_range)
                                df_t = pd.DataFrame({'min': d_min, 'max': d_max}, index=full_range).resample('ME').sum()
                                df_t = df_t.loc[df_t.index.intersection(idx_dates)]
                                tm_min, tm_max = df_t['min'], df_t['max']

                            fig.add_trace(go.Scatter(x=df_res.index, y=tm_max, mode='lines', line=dict(color=color, width=1, dash='dash'), name=f"{col_name} (Max)", showlegend=True))
                            fig.add_trace(go.Scatter(x=df_res.index, y=tm_min, mode='lines', line=dict(color=color, width=1, dash='dot'), fill='tonexty', fillcolor=f"rgba{tuple(int(color.lstrip('#')[i:i+2], 16) for i in (0, 2, 4)) + (0.1,)}", name=f"{col_name} (Min)", showlegend=True))

                fig.update_layout(barmode='group', height=500, title="Energia (kWh)")
                st.plotly_chart(fig, use_container_width=True)

                # === CURVA DE POTÊNCIA (RECUPERADO) ===
                if visao == "Diário":
                    st.divider()
                    st.subheader("Curva de Potência (kW)")
                    
                    # Pivot para ter colunas por usina/inversor e índice por timestamp
                    df_pot = df_tec.pivot_table(index='ts', columns=grp, values='potencia_ativa_kw', aggfunc='sum')
                    
                    fig_l = go.Figure()
                    for i, col in enumerate(df_pot.columns): 
                        fig_l.add_trace(go.Scatter(
                            x=df_pot.index, 
                            y=df_pot[col], 
                            mode='lines', 
                            name=col, 
                            line=dict(color=SOL_COLORS[i % len(SOL_COLORS)])
                        ))
                    
                    fig_l.update_layout(hovermode="x unified", height=450, xaxis_title="Horário", yaxis_title="Potência (kW)")
                    st.plotly_chart(fig_l, use_container_width=True)

        # === ABA 3: COMPARAÇÃO ===
        with tab3:
            st.title("Comparação")
            c1, c2 = st.columns([1,3])
            qtd = c1.number_input("Dias", 2, 5, 2)
            datas = [c1.date_input(f"Dia {i+1}", hoje - timedelta(days=(i*7)+1)) for i in range(qtd)]
            metr = c1.selectbox("Métrica", ["Potência (kW)", "Energia (kWh)"])
            
            fig_c = go.Figure()
            for i, d in enumerate(datas):
                # Conversão Timezone manual
                d_br = datetime.combine(d, time.min)
                d_ini_utc = tz_br.localize(d_br).astimezone(pytz.utc)
                d_fim_utc = tz_br.localize(datetime.combine(d + timedelta(days=1), time.min)).astimezone(pytz.utc)
                
                dd, _ = get_dados_completos(ids_inv_finais, [], d_ini_utc, d_fim_utc, d, d)
                if dd.empty: continue
                dd['ts'] = pd.to_datetime(dd['timestamp_utc']).dt.tz_convert('America/Sao_Paulo')
                agg = dd.groupby('ts').agg({'potencia_ativa_kw':'sum', 'energia_intervalo_wh':'sum'}).reset_index()
                agg['h'] = agg['ts'].dt.hour + agg['ts'].dt.minute/60
                y = agg['potencia_ativa_kw'] if metr == "Potência (kW)" else agg['energia_intervalo_wh'].cumsum()/1000
                fig_c.add_trace(go.Scatter(x=agg['h'], y=y, name=d.strftime('%d/%m'), line=dict(color=SOL_COLORS[i%5], width=3)))
            
            fig_c.update_layout(height=500, xaxis=dict(tickvals=[0,6,12,18,24], ticktext=['0h','6h','12h','18h','24h']))
            c2.plotly_chart(fig_c, use_container_width=True)

        # === ABA 4: STATUS ===
        with tab4:
            st.header("Status")
            q = "SELECT i.nome_inversor, u.nome_usina, MAX(l.timestamp_utc) as u_sinal FROM tbl_leituras l JOIN tbl_inversores i ON l.id_inversor = i.id_inversor JOIN tbl_usinas u ON i.id_usina = u.id_usina WHERE l.id_inversor = ANY(:ids) GROUP BY 1, 2 ORDER BY 3 DESC"
            ds = conn.query(q, params={"ids": ids_inv_finais}, ttl=60)
            if not ds.empty:
                ds['u_sinal'] = pd.to_datetime(ds['u_sinal']).dt.tz_convert('America/Sao_Paulo')
                ds['status'] = ds['u_sinal'].apply(lambda x: "🔴 OFF" if (datetime.now(pytz.timezone('America/Sao_Paulo')) - x).total_seconds() > 1800 else "🟢 ON")
                st.dataframe(ds.style.format({"u_sinal": lambda t: t.strftime("%d/%m %H:%M")}))
