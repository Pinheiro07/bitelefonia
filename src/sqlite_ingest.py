from __future__ import annotations
import sqlite3
from pathlib import Path
from datetime import datetime
import hashlib
import re
import pandas as pd

BASE = Path(__file__).resolve().parents[1]

DB_PATH = BASE / "data" / "processed" / "voice_calls.db"
TABLE_CALLS = "calls"
TABLE_FILES = "ingested_files"

# Fontes
NAT_DIR = BASE / "data" / "raw" / "national_rsw"          # onde ficam os .cdr baixados do FTP
INTL_CSV = BASE / "data" / "processed" / "voice_calls_tratado.csv"  # seu internacional tratado (do transform.py)

DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+$")
PHONE_RE = re.compile(r"^\d{10,15}$")

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()

def ensure_schema(con: sqlite3.Connection) -> None:
    cur = con.cursor()

    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_FILES} (
        file_key TEXT PRIMARY KEY,
        ingested_at TEXT NOT NULL
    );
    """)

    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_CALLS} (
        call_hash TEXT PRIMARY KEY,
        "Connect time" TEXT,
        "Disconnect time" TEXT,
        "From" TEXT,
        "To" TEXT,
        Country TEXT,
        Description TEXT,
        "Charged time, hour:min:sec" TEXT,
        DurationSeconds INTEGER,
        "Amount, BRL" REAL,
        Date TEXT,
        Hour INTEGER,
        CallType TEXT,
        SourceFile TEXT
    );
    """)

    # índices úteis
    cur.execute(f'CREATE INDEX IF NOT EXISTS idx_calls_connect ON {TABLE_CALLS} ("Connect time");')
    cur.execute(f'CREATE INDEX IF NOT EXISTS idx_calls_country ON {TABLE_CALLS} (Country);')
    cur.execute(f'CREATE INDEX IF NOT EXISTS idx_calls_from ON {TABLE_CALLS} ("From");')
    cur.execute(f'CREATE INDEX IF NOT EXISTS idx_calls_type ON {TABLE_CALLS} (CallType);')

    con.commit()

def already_ingested(con: sqlite3.Connection, file_key: str) -> bool:
    cur = con.cursor()
    cur.execute(f"SELECT 1 FROM {TABLE_FILES} WHERE file_key = ? LIMIT 1;", (file_key,))
    return cur.fetchone() is not None

def mark_ingested(con: sqlite3.Connection, file_key: str) -> None:
    cur = con.cursor()
    cur.execute(
        f"INSERT OR IGNORE INTO {TABLE_FILES} (file_key, ingested_at) VALUES (?, ?);",
        (file_key, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    )
    con.commit()

def extract_datetimes(fields: list[str]) -> tuple[str | None, str | None]:
    # extremamente rápido: parse só strings com cara de timestamp
    dts = []
    for s in fields:
        s = s.strip()
        if DT_RE.match(s):
            try:
                ts = datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f")
                dts.append(ts)
            except:
                pass
    if not dts:
        return None, None
    connect = dts[0].strftime("%Y-%m-%d %H:%M:%S")
    disconnect = dts[-1].strftime("%Y-%m-%d %H:%M:%S")
    return connect, disconnect

def extract_phones(fields: list[str]) -> tuple[str | None, str | None]:
    candidates = []
    for f in fields:
        s = f.strip()
        if PHONE_RE.match(s):
            candidates.append(s)
    # remove duplicados
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

def insert_calls(con: sqlite3.Connection, rows: list[tuple]) -> None:
    cur = con.cursor()
    cur.executemany(
        f"""
        INSERT OR IGNORE INTO {TABLE_CALLS} (
          call_hash, "Connect time", "Disconnect time", "From", "To", Country, Description,
          "Charged time, hour:min:sec", DurationSeconds, "Amount, BRL", Date, Hour, CallType, SourceFile
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows
    )
    con.commit()

def ingest_national(con: sqlite3.Connection) -> None:
    nat_files = sorted(NAT_DIR.glob("*.cdr"))
    if not nat_files:
        print("Nenhum .cdr encontrado para nacional.")
        return

    processed = 0
    skipped = 0
    errors = 0

    for idx, fp in enumerate(nat_files, start=1):
        name = fp.name.split("__", 1)[-1]  # remove timestamp se existir
        file_key = f"national:{name}"

        if already_ingested(con, file_key):
            skipped += 1
            continue

        try:
            rows = []
            with fp.open("r", encoding="utf-8", errors="ignore") as f:
                for line_no, line in enumerate(f, start=1):
                    line = line.strip()
                    if not line:
                        continue

                    fields = line.split(";")
                    connect, disconnect = extract_datetimes(fields)
                    from_n, to_n = extract_phones(fields)

                    duration = 0
                    if connect and disconnect:
                        try:
                            t1 = datetime.strptime(connect, "%Y-%m-%d %H:%M:%S")
                            t2 = datetime.strptime(disconnect, "%Y-%m-%d %H:%M:%S")
                            ds = int((t2 - t1).total_seconds())
                            duration = ds if ds > 0 else 0
                        except:
                            duration = 0

                    date = connect[:10] if connect else None
                    hour = int(connect[11:13]) if connect else None
                    amount = 0.0

                    call_hash = sha1(f"NAT|{from_n}|{to_n}|{connect}|{disconnect}|{amount}|{duration}")

                    rows.append((
                        call_hash, connect, disconnect, from_n, to_n,
                        "Brazil", "National", None, duration, amount, date, hour,
                        "National", name
                    ))

            insert_calls(con, rows)
            mark_ingested(con, file_key)
            processed += 1

            if processed % 200 == 0:
                print(f"[OK] processados={processed} | pulados={skipped} | erros={errors} | último={name}")

        except Exception as e:
            errors += 1
            print(f"[ERRO] arquivo={name} -> {e}")
            # não marca como ingerido; dá pra tentar de novo depois
            continue

    print(f"Resumo nacional: processados={processed} | pulados={skipped} | erros={errors}")


def ingest_international(con: sqlite3.Connection) -> None:
    if not INTL_CSV.exists():
        print(f"Internacional não encontrado: {INTL_CSV} (rode transform.py antes)")
        return

    # se o CSV é re-gerado todo mês, podemos reprocessar sempre com INSERT OR IGNORE (não duplica)
    df = pd.read_csv(INTL_CSV)

    # garante colunas
    if "CallType" not in df.columns:
        df["CallType"] = "International"

    # normaliza
    if "Amount, BRL" in df.columns:
        df["Amount, BRL"] = pd.to_numeric(df["Amount, BRL"], errors="coerce").fillna(0.0)
    else:
        df["Amount, BRL"] = 0.0

    if "DurationSeconds" in df.columns:
        df["DurationSeconds"] = pd.to_numeric(df["DurationSeconds"], errors="coerce").fillna(0).astype(int)
    else:
        df["DurationSeconds"] = 0

    # file_key baseado em tamanho+mtime (se mudar, reprocessa; mas sem duplicar)
    stat = INTL_CSV.stat()
    file_key = f"intl:{INTL_CSV.name}:{stat.st_size}:{int(stat.st_mtime)}"
    if already_ingested(con, file_key):
        print("Internacional: CSV já ingerido (mesmo tamanho/mtime).")
        return

    rows = []
    for _, r in df.iterrows():
        connect = str(r.get("Connect time") or "")
        disconnect = str(r.get("Disconnect time") or "")
        from_n = str(r.get("From") or "")
        to_n = str(r.get("To") or "")
        country = str(r.get("Country") or "")
        desc = str(r.get("Description") or "")
        charged = r.get("Charged time, hour:min:sec")
        duration = int(r.get("DurationSeconds") or 0)
        amount = float(r.get("Amount, BRL") or 0.0)
        calltype = str(r.get("CallType") or "International")

        date = connect[:10] if connect else None
        hour = int(connect[11:13]) if len(connect) >= 13 else None

        call_hash = sha1(f"INT|{from_n}|{to_n}|{connect}|{disconnect}|{amount}|{duration}|{calltype}")

        rows.append((
            call_hash, connect or None, disconnect or None, from_n or None, to_n or None,
            country or None, desc or None, charged if pd.notna(charged) else None,
            duration, amount, date, hour, calltype, INTL_CSV.name
        ))

    insert_calls(con, rows)
    mark_ingested(con, file_key)

    print(f"OK internacional: {INTL_CSV.name} -> {len(rows)} linhas ingeridas (sem duplicar)")

def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    ensure_schema(con)

    ingest_national(con)
    ingest_international(con)
    print(">>> Iniciando ingest internacional:", INTL_CSV)

    con.close()
    print(f"\n✅ SQLite atualizado: {DB_PATH}")

if __name__ == "__main__":
    main()