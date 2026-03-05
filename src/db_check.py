import sqlite3
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
DB = BASE / "data" / "processed" / "voice_calls.db"

con = sqlite3.connect(DB)
cur = con.cursor()

cur.execute("SELECT COUNT(*) FROM ingested_files;")
print("ingested_files:", cur.fetchone()[0])

cur.execute("SELECT COUNT(*) FROM calls;")
print("calls:", cur.fetchone()[0])

cur.execute("SELECT file_key, ingested_at FROM ingested_files ORDER BY ingested_at DESC LIMIT 10;")
print("\nÚltimos 10 arquivos ingeridos:")
for r in cur.fetchall():
    print(r[0], r[1])

con.close()