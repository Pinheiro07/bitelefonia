from __future__ import annotations
import sqlite3
from pathlib import Path
import pandas as pd

BASE = Path(__file__).resolve().parents[1]
CSV = BASE / "data" / "processed" / "voice_calls_tratado.csv"
DB  = BASE / "data" / "processed" / "voice_calls.db"

TABLE = "calls"

def main():
    if not CSV.exists():
        raise SystemExit(f"CSV não encontrado: {CSV}")

    df = pd.read_csv(CSV)

    # Normaliza tipos básicos
    for c in ["Amount, BRL", "DurationSeconds"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    # Connect time como texto ISO (mais fácil filtrar no SQLite)
    if "Connect time" in df.columns:
        df["Connect time"] = pd.to_datetime(df["Connect time"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")

    if "Disconnect time" in df.columns:
        df["Disconnect time"] = pd.to_datetime(df["Disconnect time"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")

    # Cria DB
    DB.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB)

    # Recria tabela do zero (mais simples pro fluxo mensal)
    df.to_sql(TABLE, con, if_exists="replace", index=False)

    # Índices pra acelerar filtro/ordenação
    cur = con.cursor()
    cur.execute("CREATE INDEX IF NOT EXISTS idx_calls_connect_time ON calls([Connect time]);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_calls_country ON calls(Country);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_calls_from ON calls([From]);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_calls_type ON calls(CallType);")
    con.commit()
    con.close()

    print(f"OK: SQLite gerado em {DB} com {len(df)} linhas")

if __name__ == "__main__":
    main()