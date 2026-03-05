import sqlite3
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
DB = BASE / "data" / "processed" / "voice_calls.db"

con = sqlite3.connect(DB)
cur = con.cursor()

print("\n=== Calls por CallType ===")
cur.execute("SELECT CallType, COUNT(*) FROM calls GROUP BY CallType;")
for r in cur.fetchall():
    print(r)

print("\n=== Top 10 Countries ===")
cur.execute("""
SELECT Country, COUNT(*)
FROM calls
GROUP BY Country
ORDER BY COUNT(*) DESC
LIMIT 10;
""")
for r in cur.fetchall():
    print(r)

print("\n=== Keys intl no controle de ingestão ===")
cur.execute("SELECT COUNT(*) FROM ingested_files WHERE file_key LIKE 'intl:%';")
print(cur.fetchone()[0])

con.close()