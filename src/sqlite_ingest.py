from __future__ import annotations

import hashlib
import re
import sqlite3
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pandas as pd

BASE = Path(__file__).resolve().parents[1]

DB_PATH = BASE / "data" / "processed" / "voice_calls.db"
NAT_DIR = BASE / "data" / "raw" / "national_rsw"
INTL_CSV = BASE / "data" / "processed" / "voice_calls_tratado.csv"

TABLE_CALLS = "calls"
TABLE_FILES = "ingested_files"

# -------- regex --------
DT_RE = re.compile(r"\b\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+\b")

# Regex mais tolerante:
# captura qualquer conteúdo entre ;...; desde que não atravesse o próximo ';'
# depois a limpeza decide o que é telefone válido
PAIR_RE = re.compile(r";([^;]{3,40});\d+;;([^;]{3,40});\d+;")


# -------- utils --------
def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()


def to_text(value) -> str | None:
    if value is None:
        return None

    s = str(value).strip()
    if not s or s.lower() in {"none", "null", "nan"}:
        return None

    return s


def normalize_phone(value) -> str:
    """
    Normaliza telefones vindos de CDR / CSV / SQLite.

    Regras:
    - aceita int, float, str, None
    - corrige ".0" e notação científica
    - remove caracteres não numéricos
    - remove zeros à esquerda excedentes
    - remove prefixo 55 quando sobrar
    - remove prefixos de rota/tronco comuns quando fizer sentido
    - mantém no final os 10 ou 11 dígitos mais prováveis
    """
    if value is None:
        return ""

    s = str(value).strip()

    if not s or s.lower() in {"none", "null", "nan"}:
        return ""

    # Corrige casos como 28270383201.0 ou 2.8270383201E10
    try:
        if any(ch in s.lower() for ch in [".", "e"]):
            d = Decimal(s)
            s = format(d, "f")
            if "." in s:
                s = s.rstrip("0").rstrip(".")
    except (InvalidOperation, ValueError):
        pass

    # Mantém apenas dígitos
    s = re.sub(r"\D", "", s)

    if not s:
        return ""

    # Remove zeros à esquerda sobrando
    while s.startswith("0") and len(s) > 11:
        s = s[1:]

    # Remove código do Brasil se ainda estiver sobrando
    if s.startswith("55") and len(s) > 11:
        s = s[2:]

    # Remove prefixos comuns de rota/tronco quando o restante ainda parecer telefone
    prefixes = ["600", "300", "0300", "0800", "90", "9090"]
    changed = True
    while changed:
        changed = False
        for p in prefixes:
            if s.startswith(p) and len(s) - len(p) >= 10:
                s = s[len(p):]
                changed = True
                break

    # Se ainda vier grande demais, prioriza os últimos 11 ou 10 dígitos
    if len(s) > 11:
        tail11 = s[-11:]
        tail10 = s[-10:]

        # prioriza 11 dígitos por ser o caso mais comum no BR (DDD + 9)
        if len(tail11) == 11 and not tail11.startswith("00"):
            s = tail11
        else:
            s = tail10

    return s


def extract_datetimes_from_line(line: str) -> tuple[str | None, str | None]:
    """
    Pega todos timestamps com milissegundos na linha.
    Primeiro = connect, último = disconnect.
    """
    hits = DT_RE.findall(line)
    if not hits:
        return None, None

    try:
        t1 = datetime.strptime(hits[0], "%Y-%m-%d %H:%M:%S.%f")
    except Exception:
        t1 = None

    try:
        t2 = datetime.strptime(hits[-1], "%Y-%m-%d %H:%M:%S.%f")
    except Exception:
        t2 = None

    connect = t1.strftime("%Y-%m-%d %H:%M:%S") if t1 else None
    disconnect = t2.strftime("%Y-%m-%d %H:%M:%S") if t2 else None
    return connect, disconnect


def extract_from_to_rsw(line: str) -> tuple[str | None, str | None]:
    """
    Captura TODOS os pares no formato ;FROM;<n>;;TO;<n>;
    usando regex tolerante, e escolhe o par mais provável.
    """
    matches = list(PAIR_RE.finditer(line))
    if not matches:
        return None, None

    def score_pair(a: str, b: str) -> int:
        score = 0

        # números de telefone BR mais comuns
        if len(a) in (10, 11):
            score += 4
        elif 8 <= len(a) <= 13:
            score += 2

        if len(b) in (10, 11):
            score += 4
        elif 8 <= len(b) <= 13:
            score += 2

        # favorece destino com 11 dígitos
        if len(b) == 11:
            score += 2

        # penaliza curtos demais
        if len(a) <= 5:
            score -= 5
        if len(b) <= 5:
            score -= 5

        return score

    best = None
    best_score = -10**9

    for m in matches:
        raw_a = m.group(1)
        raw_b = m.group(2)

        a = normalize_phone(raw_a)
        b = normalize_phone(raw_b)

        if not a or not b:
            continue

        sc = score_pair(a, b)

        # em empate, prefere o último
        if sc > best_score or (sc == best_score and best is not None):
            best = (a, b)
            best_score = sc

    return best if best else (None, None)


# -------- db helpers --------
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

    cur.execute(f'CREATE INDEX IF NOT EXISTS idx_calls_connect ON {TABLE_CALLS} ("Connect time");')
    cur.execute(f'CREATE INDEX IF NOT EXISTS idx_calls_country ON {TABLE_CALLS} (Country);')
    cur.execute(f'CREATE INDEX IF NOT EXISTS idx_calls_from ON {TABLE_CALLS} ("From");')
    cur.execute(f'CREATE INDEX IF NOT EXISTS idx_calls_to ON {TABLE_CALLS} ("To");')
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
        (file_key, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    )
    con.commit()


def insert_calls_batch(con: sqlite3.Connection, rows: list[tuple]) -> None:
    if not rows:
        return

    cur = con.cursor()
    cur.executemany(
        f"""
        INSERT OR IGNORE INTO {TABLE_CALLS} (
          call_hash, "Connect time", "Disconnect time", "From", "To", Country, Description,
          "Charged time, hour:min:sec", DurationSeconds, "Amount, BRL", Date, Hour, CallType, SourceFile
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    con.commit()


# -------- ingest national (streaming) --------
def ingest_national_streaming(con: sqlite3.Connection, batch_size: int = 5000) -> None:
    files = sorted(NAT_DIR.glob("*.cdr"))
    if not files:
        print("Nenhum .cdr encontrado para nacional.")
        return

    processed_files = 0
    skipped_files = 0
    error_files = 0
    inserted_rows_total = 0

    for fp in files:
        name = fp.name.split("__", 1)[-1]
        file_key = f"national:{name}"

        if already_ingested(con, file_key):
            skipped_files += 1
            continue

        try:
            batch: list[tuple] = []

            with fp.open("r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    connect, disconnect = extract_datetimes_from_line(line)
                    from_n, to_n = extract_from_to_rsw(line)

                    if not connect or not from_n or not to_n:
                        continue

                    duration = 0
                    if disconnect:
                        try:
                            t1 = datetime.strptime(connect, "%Y-%m-%d %H:%M:%S")
                            t2 = datetime.strptime(disconnect, "%Y-%m-%d %H:%M:%S")
                            ds = int((t2 - t1).total_seconds())
                            duration = ds if ds > 0 else 0
                        except Exception:
                            duration = 0

                    date = connect[:10]
                    hour = int(connect[11:13])
                    amount = 0.0

                    call_hash = sha1(f"NAT|{from_n}|{to_n}|{connect}|{disconnect}|{duration}|{amount}")

                    batch.append((
                        call_hash,
                        connect,
                        disconnect,
                        from_n,
                        to_n,
                        "Brazil",
                        "National",
                        None,
                        duration,
                        amount,
                        date,
                        hour,
                        "National",
                        name,
                    ))

                    if len(batch) >= batch_size:
                        insert_calls_batch(con, batch)
                        inserted_rows_total += len(batch)
                        batch.clear()

            if batch:
                insert_calls_batch(con, batch)
                inserted_rows_total += len(batch)
                batch.clear()

            mark_ingested(con, file_key)
            processed_files += 1

            if processed_files % 200 == 0:
                print(
                    f"[OK] arquivos={processed_files} | pulados={skipped_files} | "
                    f"erros={error_files} | rows_total={inserted_rows_total}"
                )

        except Exception as e:
            error_files += 1
            print(f"[ERRO] arquivo={name} -> {e}")
            continue

    print(
        f"Resumo nacional: processados={processed_files} | pulados={skipped_files} | "
        f"erros={error_files} | rows_inseridas={inserted_rows_total}"
    )


# -------- ingest international --------
def ingest_international(con: sqlite3.Connection) -> None:
    if not INTL_CSV.exists():
        print(f"Internacional não encontrado: {INTL_CSV} (rode transform.py antes)")
        return

    stat = INTL_CSV.stat()
    file_key = f"intl:{INTL_CSV.name}:{stat.st_size}:{int(stat.st_mtime)}"

    if already_ingested(con, file_key):
        print("Internacional: CSV já ingerido (mesmo tamanho/mtime).")
        return

    df = pd.read_csv(INTL_CSV)

    df["Amount, BRL"] = pd.to_numeric(df.get("Amount, BRL", 0), errors="coerce").fillna(0.0)
    df["DurationSeconds"] = pd.to_numeric(df.get("DurationSeconds", 0), errors="coerce").fillna(0).astype(int)

    rows: list[tuple] = []

    for _, r in df.iterrows():
        connect = to_text(r.get("Connect time"))
        disconnect = to_text(r.get("Disconnect time"))
        from_n = normalize_phone(r.get("From")) or None
        to_n = normalize_phone(r.get("To")) or None
        country = to_text(r.get("Country"))
        desc = to_text(r.get("Description"))
        charged = r.get("Charged time, hour:min:sec")
        duration = int(r.get("DurationSeconds") or 0)
        amount = float(r.get("Amount, BRL") or 0.0)
        calltype = "International"

        if not connect or not from_n or not to_n:
            continue

        date = connect[:10] if connect else None
        hour = int(connect[11:13]) if connect and len(connect) >= 13 else None

        call_hash = sha1(f"INT|{from_n}|{to_n}|{connect}|{disconnect}|{amount}|{duration}")

        rows.append((
            call_hash,
            connect,
            disconnect,
            from_n,
            to_n,
            country,
            desc,
            charged if pd.notna(charged) else None,
            duration,
            amount,
            date,
            hour,
            calltype,
            INTL_CSV.name,
        ))

    insert_calls_batch(con, rows)
    mark_ingested(con, file_key)
    print(f"OK internacional: {INTL_CSV.name} -> {len(rows)} linhas ingeridas (sem duplicar)")


# -------- main --------
def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    ensure_schema(con)

    ingest_national_streaming(con, batch_size=5000)
    ingest_international(con)

    con.close()
    print(f"\n✅ SQLite atualizado: {DB_PATH}")


if __name__ == "__main__":
    main()