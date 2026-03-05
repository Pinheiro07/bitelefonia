# 📊 BI Telefonia Web – Planilha Inteligente de Ligações

Sistema web simples e profissional para análise de chamadas internacionais, alimentado automaticamente pelo processamento dos logs.

---

# 🎯 Objetivo

Transformar logs brutos de chamadas em uma **planilha web interativa**, com:

* Filtros por período
* Filtro por país
* Filtro por número de origem (From)
* KPIs automáticos
* Ordenação e paginação
* Atualização mensal simples

Sem Excel. Sem Power BI. Tudo via navegador.

---

# 🏗 Arquitetura do Projeto

```
BI TELEFONIA/
│
├── data/
│   ├── raw/
│   │   └── voice_calls.csv              ← Arquivo bruto mensal
│   │
│   └── processed/
│       └── voice_calls_tratado.csv      ← Gerado automaticamente
│
├── src/
│   └── transform.py                     ← ETL (tratamento do log)
│
├── web/
│   ├── backend/
│   │   ├── app.py                       ← API FastAPI
│   │   └── __init__.py
│   │
│   └── frontend/
│       └── index.html                   ← Planilha web
│
├── .venv/
└── requirements.txt
```

---

# ⚙️ Tecnologias Utilizadas

* Python 3.10+
* FastAPI
* Pandas
* Uvicorn
* HTML + CSS
* Tabulator.js (planilha web)

---

# 🚀 Como Executar o Sistema

## 1️⃣ Ativar o ambiente virtual

### CMD:

```
.\.venv\Scripts\activate.bat
```

### PowerShell:

```
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

---

## 2️⃣ Gerar o arquivo tratado

Coloque o arquivo bruto em:

```
data/raw/voice_calls.csv
```

Depois execute:

```
python src\transform.py
```

Isso irá gerar:

```
data/processed/voice_calls_tratado.csv
```

---

## 3️⃣ Iniciar o servidor web

```
python -m uvicorn web.backend.app:app --reload
```

Acesse no navegador:

```
http://127.0.0.1:8000
```

---

# 🔎 Funcionalidades da Planilha Web

✔ Filtro por data (início e fim)
✔ Filtro por país
✔ Busca por número de origem (From)
✔ KPIs automáticos:

* Total gasto (R$)
* Total de chamadas
* Total de minutos
* Custo médio por chamada
* Custo por minuto

✔ Ordenação por coluna
✔ Paginação automática
✔ Atualização automática após novo processamento

---

# 🔄 Atualização Mensal (Fluxo Oficial)

Todo mês:

1. Substituir:

   ```
   data/raw/voice_calls.csv
   ```

2. Rodar:

   ```
   python src\transform.py
   ```

3. Atualizar o navegador (F5)

Pronto.

---

# 🛠 Endpoints Disponíveis

* `/` → Planilha Web
* `/calls` → Dados + KPIs
* `/filters` → Lista de países
* `/debug` → Verificação do caminho do CSV
* `/docs` → Documentação automática da API

---

# 📈 Próximas Evoluções Possíveis

* Exportar Excel
* Exportar CSV filtrado
* Banco SQLite ao invés de CSV
* Deploy em servidor interno
* Deploy em VPS
* Autenticação de usuários
* Dashboard com gráficos adicionais

---

# 🔒 Segurança

* Dados não são enviados para terceiros
* Sistema roda localmente ou na rede interna
* Pode ser hospedado em servidor privado

---

# 🧠 Resumo

Você construiu um mini-BI web próprio:

ETL → CSV Tratado → API → Planilha Interativa

Sistema simples, eficiente e totalmente controlado por você.

---

Projeto desenvolvido para análise estratégica de custos de telefonia internacional.
