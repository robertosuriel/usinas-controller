# Sistema de Monitoramento de Usinas Solares

Sistema completo de gerenciamento e monitoramento de usinas solares com integração às APIs da Fronius e Sungrow.

## 🚀 Funcionalidades

### Dashboard Principal (`dashboard_v2.py`)
- **Autenticação SHA256** para acesso seguro
- **7 abas de visualização** com análises completas
- **Gráficos interativos** com Plotly
- **Conversão automática** de fuso horário (UTC → America/Sao_Paulo)
- **Interface responsiva** desenvolvida em Streamlit

### ETL Automatizado
- **Integração Fronius Solar.web** - API totalmente funcional
- **Integração Sungrow iSolarCloud** - Sistema de autenticação resolvido
- **Extração automática** de metadados das usinas e inversores
- **Conexão AWS RDS PostgreSQL** para armazenamento

## 📊 Estrutura do Banco de Dados

- **tbl_usinas**: Metadados das usinas solares
- **tbl_inversores**: Informações detalhadas dos inversores
- **Potências corretas**: Sistema de extração baseado em modelos

## 🔧 Tecnologias

- **Python**: Linguagem principal
- **Streamlit**: Interface web
- **PostgreSQL**: Banco de dados (AWS RDS)
- **Plotly**: Visualizações interativas
- **Pandas**: Manipulação de dados
- **Requests**: Integração com APIs

## 📦 Instalação

```bash
pip install streamlit pandas plotly psycopg2-binary requests
```

## ⚡ Uso

```bash
streamlit run dashboard_v2.py
```

## 🏭 Usinas Monitoradas

- **13 usinas** no total
- **1 Fronius** (Blue Solutions)
- **12 Sungrow** (Usina01 até Usina12)
- **25 inversores** com potências corretas

## 🔐 Configuração

As credenciais estão configuradas nos arquivos ETL para:
- AWS RDS PostgreSQL
- Fronius Solar.web API  
- Sungrow iSolarCloud API

## 📈 Status do Projeto

✅ Dashboard completo e funcional  
✅ ETL para ambas as APIs integrado  
✅ Banco de dados populado  
✅ Potências dos inversores corrigidas  
✅ Sistema pronto para produção  

---

**Desenvolvido por Blue Solutions** 🌞