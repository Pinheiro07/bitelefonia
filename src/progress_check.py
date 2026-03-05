import sqlite3
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
DB = BASE / "data" / "processed" / "voice_calls.db"
NAT_DIR = BASE / "data" / "raw" / "national_rsw"

con = sqlite3.connect(DB)
cur = con.cursor()

# total arquivos locais
files = list(NAT_DIR.glob("*.cdr"))
total_local = len(files)

# total ingeridos (só nacional)
cur.execute("SELECT COUNT(*) FROM ingested_files WHERE file_key LIKE 'national:%';")
total_ing = cur.fetchone()[0]

print("Arquivos locais (.cdr):", total_local)
print("Arquivos ingeridos (national):", total_ing)
print("Faltando:", total_local - total_ing)

con.close()