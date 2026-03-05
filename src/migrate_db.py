import sqlite3
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
DB = BASE / "data" / "processed" / "voice_calls.db"

def main():
    con = sqlite3.connect(DB)
    cur = con.cursor()

    # verifica se calls já tem call_hash
    cur.execute("PRAGMA table_info(calls);")
    cols = [r[1] for r in cur.fetchall()]
    if "call_hash" in cols:
        print("DB já está no schema novo (call_hash existe).")
        con.close()
        return

    # renomeia tabela antiga
    cur.execute("ALTER TABLE calls RENAME TO calls_old;")

    # cria tabela nova
    cur.execute("""
    CREATE TABLE IF NOT EXISTS calls (
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

    # cria ingested_files se não existir
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ingested_files (
        file_key TEXT PRIMARY KEY,
        ingested_at TEXT NOT NULL
    );
    """)

    con.commit()
    con.close()
    print("Migração feita: calls_old criado e calls novo criado.")
    print("Agora rode: python src/sqlite_ingest.py (ele vai preencher sem duplicar).")

if __name__ == "__main__":
    main()