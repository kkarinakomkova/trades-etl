# підключення до Google Drive
from google.colab import drive
drive.mount('/content/drive')

import pandas as pd
import sqlite3
from datetime import datetime, timedelta

# функції
def extract(file_path):
    # зчитування CSV файлу з Google Drive
    df = pd.read_csv(file_path) # Removed parse_dates here
    return df

def get_week_start_date(date_obj):
    # повернення понеділка для будь-якої дати
    weekday = date_obj.weekday()
    monday = date_obj - timedelta(days=weekday)
    return monday.date()

def calculate_pnl(row):
    # обчислення прибутку або витрат
    if row["side"].lower() == "sell":
        return row["quantity"] * row["price"]
    else:
        return -row["quantity"] * row["price"]

def transform(df):
    # готую дані: додаю тиждень і PnL, агрегую їх

    # Convert timestamp to datetime objects, coercing errors
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors='coerce')

    # Drop rows where timestamp could not be parsed
    df.dropna(subset=["timestamp"], inplace=True)

    # додаю стовпець з понеділком для кожного запису
    df["week_start_date"] = df["timestamp"].apply(get_week_start_date)

    # додаю колонку з PnL
    df["pnl"] = df.apply(calculate_pnl, axis=1)

    # агрегація
    agg_df = df.groupby(
        ["week_start_date", "client_type", "user_id", "symbol"],
        as_index=False
    ).agg(
        total_volume=("quantity", "sum"),
        total_pnl=("pnl", "sum"),
        trade_count=("timestamp", "count")
    )

    return agg_df

def load(df, db_path, table_name="agg_trades_weekly"):
    # зберігаю агреговані дані у SQLite-базу
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()

def main():
    csv_path = "/content/drive/MyDrive/Test/trades.csv"
    db_path = "/content/drive/MyDrive/Test/agg_result.db"

    print("Дані записані в:", db_path)

main()
