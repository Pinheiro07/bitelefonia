🏗 Arquitetura do projeto

BI TELEFONIA
│
├─ data
│  ├─ raw
│  │  └─ national_rsw/        # arquivos .cdr baixados do FTP
│  │
│  └─ processed
│     ├─ voice_calls.db      # banco SQLite (não versionado)
│     └─ voice_calls_tratado.csv
│
├─ src
│  ├─ fetch_ftp_rsw.py       # baixa arquivos do FTP
│  ├─ parse_cdr.py           # parser de arquivos CDR
│  ├─ transform.py           # trata CSV internacional
│  ├─ sqlite_ingest.py       # insere dados no SQLite
│
├─ web
│  ├─ backend
│  │  └─ app.py              # API FastAPI
│  │
│  └─ frontend
│     └─ index.html          # dashboard web
│
├─ requirements.txt
└─ README.md


⚙️ Tecnologias utilizadas

Python 3.12

FastAPI

SQLite

Pandas

Tabulator.js

Uvicorn

📊 Funcionalidades

✔ Ingestão automática de chamadas nacionais via FTP
✔ Importação de chamadas internacionais via CSV
✔ Deduplicação automática de chamadas
✔ Banco de dados SQLite otimizado para consultas
✔ Dashboard web com:

KPIs de chamadas

Filtros por data

Filtro por origem

Filtro por país

Paginação

Tabela dinâmica

🚀 Como rodar o projeto
1️⃣ Clonar o repositório
git clone <repo>
cd BI-TELEFONIA
2️⃣ Criar ambiente virtual
python -m venv .venv
Windows
.\.venv\Scripts\Activate.ps1
3️⃣ Instalar dependências
pip install -r requirements.txt
🔑 Configurar acesso ao FTP

Antes de rodar o sistema, configure as variáveis:

$env:FTP_HOST="168.227.139.254"
$env:FTP_USER="cdr"
$env:FTP_PASS="SENHA"
📥 Baixar chamadas nacionais
python src/fetch_ftp_rsw.py

Os arquivos serão salvos em:

data/raw/national_rsw
🌍 Processar chamadas internacionais

Coloque o CSV exportado em:

data/processed/voice_calls.csv

Depois rode:

python src/transform.py

Isso gera:

voice_calls_tratado.csv
🗄 Inserir dados no banco
python src/sqlite_ingest.py

Isso cria/atualiza:

data/processed/voice_calls.db

O sistema evita duplicação usando:

call_hash

ingested_files

🌐 Rodar o BI
python -m uvicorn web.backend.app:app --host 0.0.0.0 --port 8000

Abrir no navegador:

http://localhost:8000
🔄 Atualização de dados

Fluxo padrão:

python src/fetch_ftp_rsw.py
python src/transform.py
python src/sqlite_ingest.py

Depois apenas recarregar o navegador.

🔐 Segurança

O banco SQLite não é versionado no Git.

Arquivos ignorados:

data/raw/
data/processed/*.db
data/processed/downloaded_files.txt
.venv/
📈 Estrutura do banco

Tabela principal:

calls

Campos principais:

Connect time
Disconnect time
From
To
Country
DurationSeconds
Amount, BRL
CallType
SourceFile

Tipos de chamada:

National
International
👨‍💻 Autor

Projeto desenvolvido para análise de tráfego telefônico e custos de chamadas.