from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
import re

import pandas as pd

BASE = Path(__file__).resolve().parents[1]
CDR_DIR = BASE / "data" / "raw" / "national_rsw"
OUT = BASE / "data" / "processed" / "voice_calls_national_tratado.csv"

DT_RE = re.compile(r"\b\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+\b")
PAIR_RE = re.compile(r";([^;]{3,40});\d+;;([^;]{3,40});\d+;")


def normalize_phone(value) -> str:
    if value is None:
        return ""

    s = str(value).strip()

    if not s or s.lower() in {"none", "null", "nan"}:
        return ""

    try:
        if any(ch in s.lower() for ch in [".", "e"]):
            d = Decimal(s)
            s = format(d, "f")
            if "." in s:
                s = s.rstrip("0").rstrip(".")
    except (InvalidOperation, ValueError):
        pass

    s = re.sub(r"\D", "", s)

    if not s:
        return ""

    while s.startswith("0") and len(s) > 11:
        s = s[1:]

    if s.startswith("55") and len(s) > 11:
        s = s[2:]

    prefixes = ["600", "300", "0300", "0800", "90", "9090"]
    changed = True
    while changed:
        changed = False
        for p in prefixes:
            if s.startswith(p) and len(s) - len(p) >= 10:
                s = s[len(p):]
                changed = True
                break

    if len(s) > 11:
        tail11 = s[-11:]
        tail10 = s[-10:]
        if len(tail11) == 11 and not tail11.startswith("00"):
            s = tail11
        else:
            s = tail10

    return s


def extract_datetimes_from_line(line: str) -> tuple[datetime | None, datetime | None]:
    hits = DT_RE.findall(line)
    if not hits:
        return None, None

    try:
        first_dt = datetime.strptime(hits[0], "%Y-%m-%d %H:%M:%S.%f")
    except Exception:
        first_dt = None

    try:
        last_dt = datetime.strptime(hits[-1], "%Y-%m-%d %H:%M:%S.%f")
    except Exception:
        last_dt = None

    return first_dt, last_dt


def extract_from_to(line: str) -> tuple[str | None, str | None]:
    matches = list(PAIR_RE.finditer(line))
    if not matches:
        return None, None

    def score_pair(a: str, b: str) -> int:
        score = 0

        if len(a) in (10, 11):
            score += 4
        elif 8 <= len(a) <= 13:
            score += 2

        if len(b) in (10, 11):
            score += 4
        elif 8 <= len(b) <= 13:
            score += 2

        if len(b) == 11:
            score += 2

        if len(a) <= 5:
            score -= 5
        if len(b) <= 5:
            score -= 5

        return score

    best = None
    best_score = -10**9

    for m in matches:
        a = normalize_phone(m.group(1))
        b = normalize_phone(m.group(2))

        if not a or not b:
            continue

        sc = score_pair(a, b)
        if sc > best_score or (sc == best_score and best is not None):
            best = (a, b)
            best_score = sc

    return best if best else (None, None)


def main():
    files = sorted(CDR_DIR.glob("*.cdr"))
    if not files:
        raise SystemExit(f"Nenhum .cdr encontrado em {CDR_DIR}")

    rows = []

    for fp in files:
        with fp.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                connect, disconnect = extract_datetimes_from_line(line)
                from_n, to_n = extract_from_to(line)

                if not connect or not from_n or not to_n:
                    continue

                duration_sec = 0
                if disconnect:
                    try:
                        ds = int((disconnect - connect).total_seconds())
                        duration_sec = ds if ds > 0 else 0
                    except Exception:
                        duration_sec = 0

                rows.append({
                    "From": from_n,
                    "To": to_n,
                    "Country": "Brazil",
                    "Description": "National",
                    "Connect time": connect.strftime("%Y-%m-%d %H:%M:%S") if connect else None,
                    "Disconnect time": disconnect.strftime("%Y-%m-%d %H:%M:%S") if disconnect else None,
                    "Charged time, hour:min:sec": None,
                    "DurationSeconds": duration_sec,
                    "Amount, BRL": 0.0,
                    "Date": connect.strftime("%Y-%m-%d") if connect else None,
                    "Hour": int(connect.hour) if connect else None,
                    "CallType": "National",
                    "_source_file": fp.name,
                })

    df = pd.DataFrame(rows)

    if not df.empty:
        df["DurationSeconds"] = pd.to_numeric(df["DurationSeconds"], errors="coerce").fillna(0).astype(int)
        df["Amount, BRL"] = pd.to_numeric(df["Amount, BRL"], errors="coerce").fillna(0.0)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT, index=False)

    print(f"OK: gerado {OUT} com {len(df)} linhas")
    if not df.empty:
        print(
            df[["From", "To", "Connect time", "Disconnect time", "DurationSeconds", "_source_file"]]
            .head(5)
            .to_string(index=False)
        )


if __name__ == "__main__":
    main()