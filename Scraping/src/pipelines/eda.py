import pandas as pd
import sqlite3 as sql
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import numpy as np


def fetch_data(db_path):
    conn = sql.connect(db_path)
    query = 'SELECT * FROM content'
    df = pd.read_sql_query(query, conn)
    return df


def info(df):
    # Resumen de la base de datos
    print(df.info())

    # Estadísticas descriptivas de las columnas numéricas
    print(df.describe())


    # Ensure 'publish_date' is in datetime format
    df["publish_date"] = pd.to_datetime(df["publish_date"])

    # Extract the year from 'publish_date'
    df["year"] = df["publish_date"].dt.year

    # Create separate columns for each keyword
    df["mentions_inmigracion"] = df["text"].str.contains("inmigrantes", case=False).astype(int)
    df["mentions_refugiados"] = df["text"].str.contains("refugiados", case=False).astype(int)
    df["mentions_asilo"] = df["text"].str.contains("asilo", case=False).astype(int)
    df["mentions_racismo"] = df["text"].str.contains("racismo", case=False).astype(int)
    # Group by year and sum mentions
    mentions_per_year = df.groupby("year")[["mentions_inmigracion", "mentions_refugiados", "mentions_asilo","mentions_racismo"]].sum()

    # Plot multiple lines for each keyword
    plt.figure(figsize=(15, 5))
    plt.plot(mentions_per_year.index, mentions_per_year["mentions_inmigracion"], marker="o", label="Inmigración")
    plt.plot(mentions_per_year.index, mentions_per_year["mentions_refugiados"], marker="s", label="Refugiados")
    plt.plot(mentions_per_year.index, mentions_per_year["mentions_asilo"], marker="^", label="Asilo")
    plt.plot(mentions_per_year.index, mentions_per_year["mentions_racismo"], marker="_", label="Racismo")

    # Labels and title
    plt.xlabel("Año")
    plt.ylabel("Número de menciones")
    plt.title("Evolución de la Mención de 'Inmigración', 'Refugiados' y 'Asilo' en Noticias")
    plt.legend()  # Add legend to distinguish lines
    plt.grid(True)

    # Show plot
    plt.show()

    




def main(db_path):
    df = fetch_data(db_path)
    info(df)

    



if __name__ == '__main__':
    db_path = 'data/processed/articles.db'
    main(db_path)