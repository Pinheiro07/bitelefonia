import sqlite3
import re
from decimal import Decimal, InvalidOperation
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from typing import Optional

BASE = Path(__file__).resolve().parents[2]
DB_PATH = BASE / "data" / "processed" / "voice_calls.db"
FRONT_DIR = BASE / "web" / "frontend"
INDEX_HTML = FRONT_DIR / "index.html"

app = FastAPI(title="Voice Calls Web Sheet API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=str(FRONT_DIR)), name="static")


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
        s = s[-11:]

    return s


def build_number_conditions(field_name: str, raw_value: Optional[str], params: dict, prefix: str) -> list[str]:
    """
    Aceita:
    - 210121%
    - 2821015656
    - 2821015656,2835219010
    - múltiplas linhas
    """
    if not raw_value:
        return []

    raw_parts = re.split(r"[,\n;\r\t]+", raw_value)
    conditions = []

    for i, part in enumerate(raw_parts):
        part = part.strip()
        if not part:
            continue

        # Busca com prefixo, ex: 210121%
        if "%" in part:
            normalized = re.sub(r"[^\d%]", "", part)

            # garante que só aceite % no final
            if normalized.endswith("%"):
                normalized = normalized.rstrip("%")
                normalized = normalize_phone(normalized)
                if normalized:
                    key = f"{prefix}_{i}"
                    conditions.append(f'"{field_name}" LIKE :{key}')
                    params[key] = f"{normalized}%"
            continue

        # Busca normal parcial
        normalized = normalize_phone(part)
        if normalized:
            key = f"{prefix}_{i}"
            conditions.append(f'"{field_name}" LIKE :{key}')
            params[key] = f"%{normalized}%"

    return conditions


@app.get("/")
def home():
    return FileResponse(str(INDEX_HTML))


@app.get("/debug")
def debug():
    return {"db_path": str(DB_PATH), "exists": DB_PATH.exists()}


@app.get("/filters")
def filters():
    if not DB_PATH.exists():
        return {"countries": []}

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        SELECT DISTINCT Country
        FROM calls
        WHERE Country IS NOT NULL AND Country <> ''
        ORDER BY Country;
    """)
    countries = [r[0] for r in cur.fetchall()]
    con.close()
    return {"countries": countries}


@app.get("/calls")
def calls(
    start: Optional[str] = Query(default=None, description="YYYY-MM-DD"),
    end: Optional[str] = Query(default=None, description="YYYY-MM-DD"),
    country: Optional[str] = Query(default=None),
    from_number: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=200, ge=50, le=5000),
    to_number: Optional[str] = Query(default=None),
):
    if not DB_PATH.exists():
        return {
            "kpis": {
                "total_spend": 0,
                "total_calls": 0,
                "total_minutes": 0,
                "avg_cost_per_call": 0,
                "cost_per_minute": 0,
            },
            "pagination": {"page": page, "page_size": page_size, "total_rows": 0},
            "rows": [],
        }

    where = []
    params = {}

    if start:
        where.append('"Connect time" >= :start_dt')
        params["start_dt"] = f"{start} 00:00:00"

    if end:
        where.append('"Connect time" <= :end_dt')
        params["end_dt"] = f"{end} 23:59:59"

    if country:
        where.append("Country = :country")
        params["country"] = country

    from_conditions = build_number_conditions("From", from_number, params, "from_like")
    if from_conditions:
        where.append("(" + " OR ".join(from_conditions) + ")")

    to_conditions = build_number_conditions("To", to_number, params, "to_like")
    if to_conditions:
        where.append("(" + " OR ".join(to_conditions) + ")")

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    offset = (page - 1) * page_size

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute(f'SELECT COUNT(*) as c FROM calls {where_sql}', params)
    total_rows = int(cur.fetchone()["c"])

    cur.execute(f'''
        SELECT
            COALESCE(SUM("Amount, BRL"), 0) as total_spend,
            COALESCE(SUM(DurationSeconds), 0) as total_seconds,
            COUNT(*) as total_calls
        FROM calls
        {where_sql}
    ''', params)
    k = cur.fetchone()

    total_spend = float(k["total_spend"])
    total_calls = int(k["total_calls"])
    total_seconds = int(k["total_seconds"])
    total_minutes = total_seconds / 60 if total_seconds else 0.0
    avg_per_call = total_spend / total_calls if total_calls else 0.0
    cost_per_min = total_spend / total_minutes if total_minutes else 0.0

    cur.execute(f'''
        SELECT
            "Connect time", "Disconnect time", "From", "To", Country, Description,
            DurationSeconds, "Amount, BRL", CallType, SourceFile
        FROM calls
        {where_sql}
        ORDER BY "Connect time" DESC
        LIMIT :limit OFFSET :offset
    ''', {**params, "limit": page_size, "offset": offset})

    rows = [dict(r) for r in cur.fetchall()]
    con.close()

    for row in rows:
        row["From"] = normalize_phone(row.get("From"))
        row["To"] = normalize_phone(row.get("To"))

    return {
        "kpis": {
            "total_spend": total_spend,
            "total_calls": total_calls,
            "total_minutes": total_minutes,
            "avg_cost_per_call": avg_per_call,
            "cost_per_minute": cost_per_min,
        },
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total_rows": total_rows,
        },
        "rows": rows,
    }