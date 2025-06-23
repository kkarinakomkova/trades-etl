import pandas as pd
import sqlite3
from datetime import datetime, timedelta

def extract(file_path):
    df = pd.read_csv(file_path)
    return df

def get_week_start_date(date_obj):
    weekday = date_obj.weekday()
    monday = date_obj - timedelta(days=weekday)
    return monday.date()

def calculate_pnl(row):
    if row["side"].lower() == "sell":
        return row["quantity"] * row["price"]
    else:
        return -row["quantity"] * row["price"]

def transform(df):
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors='coerce')
    df.dropna(subset=["timestamp"], inplace=True)
    df["week_start_date"] = df["timestamp"].apply(get_week_start_date)
    df["pnl"] = df.apply(calculate_pnl, axis=1)

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
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()

def main():
    csv_path = "data/trades.csv"
    db_path = "agg_result.db"

    df = extract(csv_path)
    agg_df = transform(df)
    load(agg_df, db_path)

    print("Дані записані в:", db_path)

main()
