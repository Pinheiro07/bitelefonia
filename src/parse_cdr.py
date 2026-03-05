from __future__ import annotations
from pathlib import Path
import pandas as pd
import re

BASE = Path(__file__).resolve().parents[1]
CDR_DIR = BASE / "data" / "raw" / "national_rsw"
OUT = BASE / "data" / "processed" / "voice_calls_national_tratado.csv"

DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+$")  # 2025-12-31 23:58:51.833
TZ_RE = re.compile(r"^[+-]\d{4}$")
PHONE_RE = re.compile(r"^\d{10,15}$")  # 10-15 dígitos

from datetime import datetime

def extract_datetimes(fields):
    dts = []

    for s in fields:
        s = s.strip()

        if len(s) == 23 and s[4] == "-" and s[7] == "-":
            try:
                ts = datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f")
                dts.append(ts)
            except:
                pass

    if not dts:
        return None, None

    return dts[0], dts[-1]

    # no CDR costuma ter 3 timestamps; pegamos o primeiro e o último
    return dts[0], dts[-1]

def extract_phones(fields: list[str]) -> tuple[str | None, str | None]:
    candidates = []
    for f in fields:
        s = f.strip()
        if PHONE_RE.match(s):
            candidates.append(s)

    # remove duplicados mantendo ordem
    seen = set()
    uniq = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            uniq.append(c)

    if len(uniq) == 0:
        return None, None
    if len(uniq) == 1:
        return uniq[0], None
    return uniq[0], uniq[1]

def main():
    files = sorted(list(CDR_DIR.glob("*.cdr")))
    if not files:
        raise SystemExit(f"Nenhum .cdr encontrado em {CDR_DIR}")

    rows = []
    for fp in files:
        with fp.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                fields = line.split(";")
                connect, disconnect = extract_datetimes(fields)
                from_n, to_n = extract_phones(fields)

                duration_sec = 0

            if connect and disconnect:
                
                rows.append({
                    "From": from_n,
                    "To": to_n,
                    "Country": "Brazil",
                    "Description": "National",
                    "Connect time": connect.strftime("%Y-%m-%d %H:%M:%S") if not pd.isna(connect) else None,
                    "Disconnect time": disconnect.strftime("%Y-%m-%d %H:%M:%S") if not pd.isna(disconnect) else None,
                    "Charged time, hour:min:sec": None,
                    "DurationSeconds": duration_sec,
                    "Amount, BRL": 0.0,  # se tiver tarifa no CDR, depois mapeamos
                    "Date": connect.strftime("%Y-%m-%d") if connect is not None else None,
                    "Hour": int(connect.hour) if isinstance(connect, pd.Timestamp) else None,
                    "CallType": "National",
                    "_source_file": fp.name,
                })

    df = pd.DataFrame(rows)
    df["DurationSeconds"] = pd.to_numeric(df["DurationSeconds"], errors="coerce").fillna(0).astype(int)
    df["Amount, BRL"] = pd.to_numeric(df["Amount, BRL"], errors="coerce").fillna(0.0)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT, index=False)

    print(f"OK: gerado {OUT} com {len(df)} linhas")
    print(df[["From","To","Connect time","Disconnect time","DurationSeconds","_source_file"]].head(5).to_string(index=False))

if __name__ == "__main__":
    main()