import psycopg2
import config
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from loguru import logger

def get_airport_country_data():
    try:
        conn = psycopg2.connect(
            host=config.DB_HOST,
            port=config.DB_PORT,
            database=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD
        )
        query = """
            SELECT country, COUNT(*) AS count
            FROM airports_lookup
            GROUP BY country
            ORDER BY count DESC;
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as err:
        logger.error("Failed to fetch data from PostgreSQL: {}", err)
        return None

def plot_airport_country_counts(df, top_n=20):
    if top_n and top_n < len(df):
        df = df.head(top_n)

    sns.set(style="whitegrid", font_scale=1.1)
    plt.figure(figsize=(12, 8))

    ax = sns.barplot(
        y='country',
        x='count',
        data=df,
        palette="viridis",
        orient='h'
    )
    plt.title("Record Count per Country (Top {})".format(top_n if top_n else "All"))
    plt.xlabel("Number of Records")
    plt.ylabel("Country")
    plt.tight_layout()

    for bar in ax.patches:
        width = bar.get_width()
        ax.text(
            width + 3,
            bar.get_y() + bar.get_height() / 2,
            f"{int(width)}",
            ha="left",
            va="center"
        )

    plt.show()

if __name__ == "__main__":
    data = get_airport_country_data()
    if data is not None and not data.empty:
        logger.info("Fetched data:\n{}", data)
        plot_airport_country_counts(data, top_n=20)
    else:
        logger.error("No data available to visualize.")
