from __future__ import annotations
import os
from ftplib import FTP
from pathlib import Path
from datetime import datetime

RAW_DIR = Path(__file__).resolve().parents[1] / "data" / "raw" / "national"
RAW_DIR.mkdir(parents=True, exist_ok=True)

FTP_HOST = os.getenv("FTP_HOST")
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS")
FTP_DIR  = os.getenv("FTP_DIR", "/")  # pasta no FTP
FTP_PATTERN = os.getenv("FTP_PATTERN", ".csv")  # filtra arquivos

def main():
    if not FTP_HOST or not FTP_USER or not FTP_PASS:
        raise SystemExit("Defina FTP_HOST, FTP_USER e FTP_PASS nas variáveis de ambiente.")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    downloaded = 0

    with FTP(FTP_HOST) as ftp:
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_DIR)

        files = ftp.nlst()
        targets = [f for f in files if FTP_PATTERN in f.lower()]

        if not targets:
            print("Nenhum arquivo encontrado no FTP com o filtro:", FTP_PATTERN)
            return

        for fname in targets:
            out = RAW_DIR / f"{ts}__{Path(fname).name}"
            with open(out, "wb") as fp:
                ftp.retrbinary(f"RETR {fname}", fp.write)
            downloaded += 1
            print("Baixado:", out)

    print(f"OK: {downloaded} arquivo(s) baixado(s) para {RAW_DIR}")

if __name__ == "__main__":
    main()