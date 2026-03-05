from __future__ import annotations
import pandas as pd
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]

INTL = BASE / "data" / "processed" / "voice_calls_tratado.csv"          # seu internacional tratado (já existente)
NAT  = BASE / "data" / "processed" / "voice_calls_national_tratado.csv" # nacional tratado (novo)
OUT  = BASE / "data" / "processed" / "voice_calls_tratado.csv"          # final (sobrescreve)

def align(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "From", "To", "Country", "Description",
        "Connect time", "Disconnect time",
        "Charged time, hour:min:sec",
        "DurationSeconds", "Amount, BRL",
        "Date", "Hour", "CallType"
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    return df[cols]

def main():
    if not INTL.exists():
        raise SystemExit(f"Arquivo internacional não encontrado: {INTL}")

    df_int = pd.read_csv(INTL)
    # se o internacional ainda não tiver CallType, adiciona
    if "CallType" not in df_int.columns:
        df_int["CallType"] = "International"
    df_int = align(df_int)

    dfs = [df_int]

    if NAT.exists():
        df_nat = pd.read_csv(NAT)
        if "CallType" not in df_nat.columns:
            df_nat["CallType"] = "National"
        df_nat = align(df_nat)
        dfs.append(df_nat)
    else:
        print("Aviso: não achei o nacional tratado, seguindo só com internacional.")

    df_all = pd.concat(dfs, ignore_index=True)

    # ordena pela data/hora se existir
    if "Connect time" in df_all.columns:
        df_all["Connect time"] = pd.to_datetime(df_all["Connect time"], errors="coerce")
        df_all = df_all.sort_values("Connect time", ascending=False)
        df_all["Connect time"] = df_all["Connect time"].dt.strftime("%Y-%m-%d %H:%M:%S")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    df_all.to_csv(OUT, index=False)
    print(f"OK: dataset final gerado em {OUT} com {len(df_all)} linhas")

if __name__ == "__main__":
    main()