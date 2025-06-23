import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
import os

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

    os.makedirs("output", exist_ok=True)

    # топ-3 bronze-клієнти за total_volume
    top_clients_volume = agg_df[agg_df["client_type"] == "bronze"] \
        .groupby("user_id", as_index=False) \
        .agg(total_volume=("total_volume", "sum")) \
        .sort_values("total_volume", ascending=False) \
        .head(3)

    # топ-3 за total_pnl
    top_clients_pnl = agg_df[agg_df["client_type"] == "bronze"] \
        .groupby("user_id", as_index=False) \
        .agg(total_pnl=("total_pnl", "sum")) \
        .sort_values("total_pnl", ascending=False) \
        .head(3)

    # зберігаю у CSV і Excel
    top_clients_volume.to_csv("output/top_clients.csv", index=False)
    with pd.ExcelWriter("output/top_clients.xlsx") as writer:
        top_clients_volume.to_excel(writer, sheet_name="ByVolume", index=False)
        top_clients_pnl.to_excel(writer, sheet_name="ByPnL", index=False)

    # побудова графіків
    plt.figure(figsize=(10, 5))
    sns.lineplot(data=agg_df, x="week_start_date", y="total_volume", hue="client_type")
    plt.title("Обсяг торгів по тижнях за типом клієнта")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("output/volume_by_week.png")
    plt.close()

    plt.figure(figsize=(10, 5))
    sns.lineplot(data=agg_df, x="week_start_date", y="total_pnl", hue="client_type")
    plt.title("Прибуток/Збиток по тижнях за типом клієнта")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("output/pnl_by_week.png")
    plt.close()

    print("Збережено в папку output/")

main()
