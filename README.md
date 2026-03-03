# 📊 BI Telefonia - ETL de Ligações Internacionais

Projeto para tratamento de logs de chamadas internacionais e geração de base estruturada para uso no Power BI.

---

## 🎯 Objetivo

Automatizar a transformação de logs brutos de chamadas em um dataset limpo e estruturado para análise financeira e operacional no Power BI.

---

## 📁 Estrutura do Projeto


BI TELEFONIA/
│
├── src/
│ └── transform.py
│
├── data/
│ ├── raw/
│ │ └── voice_calls.csv ← Colocar o arquivo bruto aqui
│ │
│ └── processed/
│ └── voice_calls_tratado.csv ← Arquivo gerado automaticamente
│
├── requirements.txt
└── .gitignore


---

## ⚙️ Requisitos

- Python 3.10+
- Pandas

Instalar dependências:

```bash
pip install -r requirements.txt
🚀 Como usar
1️⃣ Coloque o arquivo bruto em:
data/raw/voice_calls.csv
2️⃣ Execute o script:
python src/transform.py
3️⃣ O arquivo tratado será gerado em:
data/processed/voice_calls_tratado.csv
📈 Como conectar no Power BI

Abrir Power BI

Obter Dados → Texto/CSV

Selecionar:

data/processed/voice_calls_tratado.csv

Carregar

📊 Métricas sugeridas no Power BI

Total Gasto

Total de Chamadas

Total de Minutos

Custo Médio por Chamada

Custo Médio por Minuto

Gasto por Dia

Gasto por Mês

Top Números Mais Ligados

🔒 Observações

A pasta data/raw e data/processed estão no .gitignore

Dados sensíveis não são versionados

Apenas código sobe para o GitHub

🛠 Próximos Passos

Automatizar execução diária

Criar banco de dados

Deploy em servidor

Criar pipeline CI/CD