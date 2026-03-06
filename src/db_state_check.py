import sqlite3
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
DB_PATH = BASE / "data" / "processed" / "voice_calls.db"

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

print("\nEstados encontrados nas chamadas nacionais:\n")

cur.execute("""
SELECT Description, COUNT(*)
FROM calls
WHERE CallType='National'
GROUP BY Description
ORDER BY COUNT(*) DESC
LIMIT 20
""")

for row in cur.fetchall():
    print(row)

con.close()