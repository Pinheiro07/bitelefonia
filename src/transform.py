import pandas as pd
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
RAW = BASE / "data" / "raw" / "voice_calls.csv"
OUT = BASE / "data" / "processed" / "voice_calls_tratado.csv"

def main():
    df = pd.read_csv(RAW, skiprows=1)
    df["Connect time"] = pd.to_datetime(df["Connect time"])
    df["Disconnect time"] = pd.to_datetime(df["Disconnect time"])

    df["DurationSeconds"] = pd.to_timedelta(
        df["Charged time, hour:min:sec"]
    ).dt.total_seconds().astype(int)

    df["Date"] = df["Connect time"].dt.date
    df["Hour"] = df["Connect time"].dt.hour
    df["Month"] = df["Connect time"].dt.to_period("M").astype(str)
    df["DayOfWeek"] = df["Connect time"].dt.day_name()

    df["Amount, BRL"] = df["Amount, BRL"].astype(float)
    df = df.drop(columns=["Account"], errors="ignore")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT, index=False)

    print(f"OK! Gerado: {OUT}")

if __name__ == "__main__":
    main()