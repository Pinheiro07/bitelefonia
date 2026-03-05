import subprocess
import sys

def run(cmd):
    print("\n>>", cmd)
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        print("Erro ao executar:", cmd)
        sys.exit(result.returncode)

def main():

    # baixar arquivos do FTP
    run("python src/fetch_ftp_rsw.py")

    # processar CDR nacionais
    run("python src/parse_cdr.py")

    # tratar chamadas internacionais
    run("python src/transform.py")

    run("python src/sqlite_ingest.py")

    # juntar datasets
    run("python src/build_final_dataset.py")

    # gerar banco SQLite
    run("python src/build_sqlite.py")

    print("\nPipeline finalizado com sucesso!")

if __name__ == "__main__":
    main()