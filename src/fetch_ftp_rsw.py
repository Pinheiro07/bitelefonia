from __future__ import annotations
import os
from ftplib import FTP, FTP_TLS, all_errors
from pathlib import Path
from datetime import datetime

BASE = Path(__file__).resolve().parents[1]
OUT_DIR = BASE / "data" / "raw" / "national_rsw"
OUT_DIR.mkdir(parents=True, exist_ok=True)

CONTROL_FILE = BASE / "data" / "processed" / "downloaded_files.txt"
CONTROL_FILE.parent.mkdir(parents=True, exist_ok=True)

FTP_HOST = os.getenv("FTP_HOST")
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS")
FTP_DIR  = os.getenv("FTP_DIR", "/")

FTP_PORT = int(os.getenv("FTP_PORT", "21"))
FTP_PASSIVE = os.getenv("FTP_PASSIVE", "1") != "0"   # 1 = passive on
FTP_USE_TLS = os.getenv("FTP_TLS", "0") == "1"        # 1 = FTPS

PREFIX = os.getenv("FTP_PREFIX", "rsw.")
SUFFIX = os.getenv("FTP_SUFFIX", ".cdr")

def main():
    if not FTP_HOST or not FTP_USER or not FTP_PASS:
        raise SystemExit("Defina FTP_HOST, FTP_USER e FTP_PASS antes de rodar.")

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        ftp = FTP_TLS() if FTP_USE_TLS else FTP()
        ftp.connect(FTP_HOST, FTP_PORT, timeout=20)
        ftp.login(FTP_USER, FTP_PASS)

        if FTP_USE_TLS:
            ftp.prot_p()

        ftp.set_pasv(FTP_PASSIVE)

        if FTP_DIR:
            ftp.cwd(FTP_DIR)

        files = ftp.nlst()

        already = set()
        if CONTROL_FILE.exists():
            with open(CONTROL_FILE, "r", encoding="utf-8", errors="ignore") as f:
                already = set(line.strip() for line in f if line.strip())

        targets = [
            f for f in files
            if f.lower().startswith(PREFIX.lower())
            and f.lower().endswith(SUFFIX.lower())
            and f not in already
        ]

        print(f"Conectado em {FTP_HOST}:{FTP_PORT} | dir={FTP_DIR} | passive={FTP_PASSIVE} | tls={FTP_USE_TLS}")
        print(f"[{ts}] encontrados={len(files)} | novos={len(targets)} ({PREFIX}*{SUFFIX})")

        if not targets:
            print("Nada novo para baixar.")
            ftp.quit()
            return

        downloaded_count = 0
        for remote_name in targets:
            safe_name = Path(remote_name).name
            out = OUT_DIR / safe_name  # ✅ SEM timestamp
            print("Baixando:", remote_name, "->", out)

            with open(out, "wb") as fp:
                ftp.retrbinary(f"RETR {remote_name}", fp.write)

            with open(CONTROL_FILE, "a", encoding="utf-8") as f:
                f.write(remote_name + "\n")

            downloaded_count += 1

        ftp.quit()
        print(f"OK: {downloaded_count} arquivo(s) novo(s) baixado(s) em {OUT_DIR}")

    except all_errors as e:
        raise SystemExit(f"Erro FTP: {e}")

if __name__ == "__main__":
    main()