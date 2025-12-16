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
    [data-testid="stSidebar"] label, [data-testid="stSidebar"] .stMarkdown p,
    [data-testid="stSidebar"] .stRadio label {
        color: white !important;
    }
    [data-testid="stSidebar"] span { color: #f0f0f0; }
    [data-testid="stSidebar"] hr { border-color: rgba(255, 255, 255, 0.4) !important; }

    /* Títulos e KPIs em Rosa */
    h1, h2, h3, [data-testid="stMetricValue"] {
        color: #E91E63 !important;
    }
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
        # Traz também o perfil de target para usar nos cálculos
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
def get_dados_completos(lista_ids_inv, lista_perfis, data_ini, data_fim):
    """
    Busca dados de geração E dados de meta em uma única ida ao banco (ou duas otimizadas).
    """
    if not lista_ids_inv or not conn: return pd.DataFrame(), pd.DataFrame()
    
    # 1. Dados de Geração (Leituras)
    q_gen = """
    SELECT l.timestamp_utc, l.potencia_ativa_kw, l.energia_intervalo_wh, i.nome_inversor, u.nome_usina, u.id_usina, u.target_profile, u.potencia_pico_kwp
    FROM tbl_leituras l
    JOIN tbl_inversores i ON l.id_inversor = i.id_inversor
    JOIN tbl_usinas u ON i.id_usina = u.id_usina
    WHERE l.id_inversor = ANY(:ids) AND l.timestamp_utc >= :d_ini AND l.timestamp_utc < :d_fim
    ORDER BY l.timestamp_utc
    LIMIT 150000;
    """
    
    # 2. Dados de Meta (Targets)
    # Busca apenas os perfis envolvidos no período selecionado
    q_target = """
    SELECT target_profile, data_referencia, val_min, val_max
    FROM tbl_targets
    WHERE target_profile = ANY(:perfis) 
      AND data_referencia >= :d_ini 
      AND data_referencia <= :d_fim
    """
    
    try:
        df_gen = conn.query(q_gen, params={"ids": lista_ids_inv, "d_ini": data_ini, "d_fim": data_fim + timedelta(days=1)}, ttl=300)
        
        # Se tiver geração, busca os targets correspondentes
        if not df_gen.empty:
            perfis_unicos = df_gen['target_profile'].unique().tolist()
            # Remove None/Null se houver
            perfis_unicos = [p for p in perfis_unicos if p]
            
            if perfis_unicos:
                df_targets = conn.query(q_target, params={"perfis": perfis_unicos, "d_ini": data_ini, "d_fim": data_fim}, ttl=3600)
            else:
                df_targets = pd.DataFrame()
        else:
            df_targets = pd.DataFrame()
            
        return df_gen, df_targets
        
    except Exception as e:
        st.error(f"Erro ao buscar dados: {e}")
        return pd.DataFrame(), pd.DataFrame()

# ------------------------------------------------------------------
# LÓGICA DE CÁLCULO DE META (USANDO O DATAFRAME DO BANCO)
# ------------------------------------------------------------------
def get_meta_periodo(df_targets, usinas_df, data_range, fator_tempo=1):
    """
    Calcula as metas min/max somadas para um intervalo de datas,
    usando o DataFrame de targets carregado do banco.
    """
    if df_targets.empty or usinas_df.empty:
        return [0]*len(data_range), [0]*len(data_range)
    
    # Pré-processamento: Indexar targets por (perfil, data) para busca rápida
    # Converte data_referencia para string ou date para garantir match
    df_targets['data_str'] = pd.to_datetime(df_targets['data_referencia']).dt.date
    target_map = df_targets.set_index(['target_profile', 'data_str'])[['val_min', 'val_max']].to_dict('index')
    
    # Dicionário de Usinas: {Nome: {Perfil, Potencia}}
    usinas_map = usinas_df.set_index('nome_usina')[['target_profile', 'potencia_pico_kwp']].to_dict('index')
    
    # Referências de Potência (Hardcoded pois são fixas do projeto)
    ref_pot = {
        'Riachão': 118, 'Xique-xique': 118, 'Xique-xique_132': 132
    }
    
    lista_min, lista_max = [], []
    
    for dia in data_range:
        dia_date = dia.date() if isinstance(dia, pd.Timestamp) else dia
        soma_min, soma_max = 0, 0
        
        for nome_usina, info in usinas_map.items():
            perfil = info.get('target_profile', 'Xique-xique') # Default
            pot_usina = info.get('potencia_pico_kwp', 100)
            
            # Busca target base do banco
            vals = target_map.get((perfil, dia_date))
            
            if vals:
                t_min_base = vals['val_min']
                t_max_base = vals['val_max']
                
                # Escala pela potência
                pot_ref = ref_pot.get(perfil, 118)
                fator_escala = pot_usina / pot_ref if pot_ref > 0 else 1
                
                t_min = t_min_base * fator_escala
                t_max = t_max_base * fator_escala
            else:
                # Fallback se não tiver meta cadastrada no dia
                t_min, t_max = 0, 0 
            
            # Fator de tempo (ex: mensal = multiplica por dias do mês? Não, aqui já somamos dia a dia)
            # A lógica de "fator_tempo" era para quando não tínhamos dados diários.
            # Agora temos. Se o gráfico for mensal, o resample vai somar.
            # Se a função for chamada para um dia, retorna o valor do dia.
            
            soma_min += t_min
            soma_max += t_max
            
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
        # Filtros
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

        # LOAD DATA
        with st.spinner("Carregando dados..."):
            # Busca Geração E Targets do banco
            main_df, targets_db_df = get_dados_completos(ids_inv_finais, usinas_sel_df['target_profile'].unique().tolist(), d_ini, d_fim)
            
            if not main_df.empty:
                main_df['ts'] = pd.to_datetime(main_df['timestamp_utc']).dt.tz_convert('America/Sao_Paulo')
                main_df_indexed = main_df.set_index('ts')
            else:
                main_df_indexed = pd.DataFrame()

        tab1, tab2, tab3, tab4 = st.tabs(["📊 Resumo", "📈 Técnica", "🔎 Comparação", "⚡ Status"])

        # === ABA 1: RESUMO ===
        with tab1:
            st.title("Resumo Gerencial")
            if not main_df_indexed.empty:
                total_gerado = main_df_indexed['energia_intervalo_wh'].sum() / 1000.0
                
                # Calcula meta para o período todo (soma diária)
                dias_range = pd.date_range(d_ini, d_fim)
                kpi_min, kpi_max = get_meta_periodo(targets_db_df, usinas_sel_df, dias_range)
                meta_total = (sum(kpi_min) + sum(kpi_max)) / 2
                
                perf = (total_gerado / meta_total * 100) if meta_total > 0 else 0

                c1, c2, c3 = st.columns(3)
                c1.metric("Gerado", f"{total_gerado:,.0f} kWh".replace(',', '.'))
                c2.metric("Meta", f"{meta_total:,.0f} kWh".replace(',', '.'))
                c3.metric("Perf.", f"{perf:.1f}%")
                st.divider()

                col_a, col_m = st.columns(2)
                
                # Anual
                df_ano = main_df_indexed.resample('YE')['energia_intervalo_wh'].sum() / 1000.0
                df_ano.index = df_ano.index.strftime('%Y')
                fig_a = go.Figure(go.Bar(x=df_ano.index, y=df_ano.values, text=[f"{v:,.0f}" for v in df_ano.values], textposition='auto', marker_color='#00E676'))
                fig_a.update_layout(title="Energia Anual", height=300)
                col_a.plotly_chart(fig_a, use_container_width=True)

                # Mensal
                df_mes = main_df_indexed.resample('ME')['energia_intervalo_wh'].sum() / 1000.0
                # Meta Mensal: Re-agrega os dados diários por mês
                # Truque: Criar um DF temporário com as metas diárias e resamplear
                df_meta_diaria = pd.DataFrame({'data': dias_range, 'min': kpi_min, 'max': kpi_max}).set_index('data')
                df_meta_mes = df_meta_diaria.resample('ME').sum()
                
                # Alinha índices (apenas meses que existem na geração ou no range)
                # (Simplificação: usa o índice do df_mes gerado)
                # Para garantir alinhamento visual, plotamos metas apenas onde tem mês
                
                fig_m = go.Figure()
                fig_m.add_trace(go.Bar(x=df_mes.index.strftime('%b/%y'), y=df_mes.values, marker_color='#00E676', name='Realizado'))
                
                # Plota Meta Mensal (Somada)
                # Precisa recalcular meta para os meses do gráfico
                m_min_lista, m_max_lista = [], []
                for m_date in df_mes.index:
                    # Pega o último dia do mês ou o range
                    # Forma mais segura: filtrar df_meta_mes pelo índice
                    try:
                        # Encontra o mês correspondente no df_meta_mes (aproximação por ano/mês)
                        # Como ambos são 'ME' (Month End), devem bater
                        val = df_meta_mes.loc[m_date]
                        m_min_lista.append(val['min'])
                        m_max_lista.append(val['max'])
                    except:
                        m_min_lista.append(0); m_max_lista.append(0)

                fig_m.add_trace(go.Scatter(x=df_mes.index.strftime('%b/%y'), y=m_max_lista, mode='lines', line=dict(width=0), showlegend=False))
                fig_m.add_trace(go.Scatter(x=df_mes.index.strftime('%b/%y'), y=m_min_lista, mode='lines', fill='tonexty', fillcolor='rgba(233,30,99,0.2)', line=dict(color='rgba(233,30,99,0.8)', dash='dot'), name='Meta'))
                fig_m.update_layout(title="Energia Mensal", height=300)
                col_m.plotly_chart(fig_m, use_container_width=True)

                # Diário
                st.subheader("Diário")
                df_d = main_df_indexed.resample('D')['energia_intervalo_wh'].sum() / 1000.0
                # Meta Diária já calculada em kpi_min/max, basta filtrar para os dias com dados ou range
                # Vamos re-calcular para o índice exato do gráfico para garantir alinhamento
                t_d_min, t_d_max = get_meta_periodo(targets_db_df, usinas_sel_df, df_d.index)
                
                fig_d = go.Figure()
                fig_d.add_trace(go.Bar(x=df_d.index.strftime('%d/%m'), y=df_d.values, marker_color='#00E676', name='Realizado'))
                fig_d.add_trace(go.Scatter(x=df_d.index.strftime('%d/%m'), y=t_d_max, mode='lines', line=dict(width=0), showlegend=False))
                fig_d.add_trace(go.Scatter(x=df_d.index.strftime('%d/%m'), y=t_d_min, mode='lines', fill='tonexty', fillcolor='rgba(233,30,99,0.2)', line=dict(color='rgba(233,30,99,0.8)', dash='dot'), name='Meta'))
                fig_d.update_layout(height=400, hovermode="x unified")
                st.plotly_chart(fig_d, use_container_width=True)
            else: st.info("Sem dados.")

        # === ABA 2: TÉCNICA ===
        with tab2:
            st.title("Análise Técnica")
            c1, c2, c3 = st.columns([1,1,2])
            visao = c1.selectbox("Visão", ["Diário", "Mensal"])
            agrup = c2.selectbox("Agrupar", ["Usina", "Inversor"])
            meta_on = c3.checkbox("Meta Individual", True)

            if not main_df_indexed.empty:
                grp = 'nome_usina' if agrup == "Usina" else 'nome_inversor'
                rule = 'D' if visao == "Diário" else 'ME'
                fmt = '%d/%m' if visao == "Diário" else '%b/%y'
                
                df_tec = main_df_indexed.copy()
                if agrup == "Inversor": df_tec['nome_inversor'] = df_tec['nome_usina'] + ' - ' + df_tec['nome_inversor']
                
                df_res = df_tec.groupby([pd.Grouper(freq=rule), grp])['energia_intervalo_wh'].sum().unstack(level=grp).fillna(0) / 1000.0
                df_res.index = df_res.index.strftime(fmt) # String para plot

                fig = go.Figure()
                
                # Índice original de datas para calcular meta
                datas_originais = df_tec.groupby([pd.Grouper(freq=rule)]).first().index

                for i, col_name in enumerate(df_res.columns):
                    color = SOL_COLORS[i % len(SOL_COLORS)]
                    fig.add_trace(go.Bar(x=df_res.index, y=df_res[col_name], name=col_name, marker_color=color))
                    
                    if meta_on and agrup == "Usina":
                        # Calcula meta para ESTA usina
                        u_info = usinas_sel_df[usinas_sel_df['nome_usina'] == col_name]
                        if not u_info.empty:
                            # Se for diário, pega meta do dia. Se mensal, soma do mês.
                            # get_meta_periodo retorna meta DIÁRIA para uma lista de dias.
                            # Se a visão é MENSAL, precisamos somar.
                            
                            if visao == "Diário":
                                tm_min, tm_max = get_meta_periodo(targets_db_df, u_info, datas_originais)
                            else:
                                # Lógica para mensal: Calcular dia a dia de cada mês e somar
                                # Complexo? Simplificação:
                                # Cria range de todos os dias do período, calcula meta dia a dia, depois resample
                                full_range = pd.date_range(datas_originais.min(), datas_originais.max() + pd.offsets.MonthEnd(0))
                                d_min, d_max = get_meta_periodo(targets_db_df, u_info, full_range)
                                df_temp = pd.DataFrame({'min': d_min, 'max': d_max}, index=full_range)
                                df_temp_res = df_temp.resample('ME').sum()
                                # Filtra apenas os meses que estão no gráfico
                                # (Assumindo alinhamento pelo índice de datas_originais que são ME)
                                tm_min = df_temp_res.loc[datas_originais]['min'].values
                                tm_max = df_temp_res.loc[datas_originais]['max'].values

                            fig.add_trace(go.Scatter(x=df_res.index, y=tm_max, mode='lines', line=dict(color=color, width=0), showlegend=False, hoverinfo='skip'))
                            fig.add_trace(go.Scatter(x=df_res.index, y=tm_min, mode='lines', line=dict(color=color, width=2, dash='dot'), fill='tonexty', fillcolor=f"rgba{tuple(int(color.lstrip('#')[i:i+2], 16) for i in (0, 2, 4)) + (0.2,)}", name=f"Meta {col_name}"))

                fig.update_layout(barmode='group', height=500)
                st.plotly_chart(fig, use_container_width=True)

        # === ABA 3: COMPARAÇÃO ===
        with tab3:
            st.title("Comparação")
            c1, c2 = st.columns([1,3])
            qtd = c1.number_input("Dias", 2, 5, 2)
            datas = [c1.date_input(f"Dia {i+1}", hoje - timedelta(days=(i*7)+1)) for i in range(qtd)]
            metr = c1.selectbox("Métrica", ["Potência (kW)", "Energia (kWh)"])
            
            fig_c = go.Figure()
            # Loop simples para buscar e plotar
            for i, d in enumerate(datas):
                dd, _ = get_dados_completos(ids_inv_finais, [], d, d) # Targets vazio aqui
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
