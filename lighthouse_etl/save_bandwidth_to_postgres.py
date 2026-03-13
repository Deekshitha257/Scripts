import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timezone
import logging
import re




DATABASE_URL = "postgresql+psycopg2://search_sandbox:search_sandbox@172.168.168.232:5432/Dashboard"
TABLE_NAME = "bandwidth_usage"
CSV_FILE_PATH = "TopBandwidthConsumingRequests.csv"




logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)




def get_engine():
    return create_engine(DATABASE_URL, pool_pre_ping=True)




def create_table_if_not_exists(engine):
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        url TEXT NOT NULL,
        dataset VARCHAR(100),
        tag VARCHAR(100),
        sum_response_size_gb NUMERIC,
        avg_response_size_mb NUMERIC,
        request_count BIGINT,
        created_timestamp TIMESTAMP WITH TIME ZONE
    );
    """

    with engine.connect() as conn:
        conn.execute(text(create_table_query))
        conn.commit()

    logging.info("Table checked/created successfully.")




def clean_avg_response_size(value):
   
    if pd.isna(value):
        return None

    value = str(value).strip().replace(",", "")

    numbers = re.findall(r"[\d.]+", value)
    if not numbers:
        return None

    number = float(numbers[0])
    value_upper = value.upper()

    if "KB" in value_upper:
        return number / 1024
    elif "GB" in value_upper:
        return number * 1024
    else:
        
        return number




def transform_data(df):

    logging.info("Initial rows count: %s", len(df))

    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

    df = df.rename(columns={
        "URL": "url",
        "Dataset": "dataset",
        "Tag": "tag",
        "Sum Response Size": "sum_response_size_gb",
        "Avg Response Size": "avg_response_size_mb",
        "Count": "request_count"
    })

    if "sum_response_size_gb" in df.columns:
        df["sum_response_size_gb"] = (
            df["sum_response_size_gb"]
            .astype(str)
            .str.replace(" GB", "", regex=False)
            .str.replace(",", "", regex=False)
            .str.strip()
        )
        df["sum_response_size_gb"] = pd.to_numeric(df["sum_response_size_gb"], errors="coerce")

    
    df["avg_response_size_mb"] = df["avg_response_size_mb"].apply(clean_avg_response_size)

   
    df["request_count"] = (
        df["request_count"]
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.strip()
    )

    df["request_count"] = pd.to_numeric(df["request_count"], errors="coerce")

    
    null_avg = df[df["avg_response_size_mb"].isna()]
    null_count = df[df["request_count"].isna()]

    if not null_avg.empty:
        logging.warning("Rows with NULL avg_response_size_mb: %s", len(null_avg))
        logging.warning(null_avg.head())

    if not null_count.empty:
        logging.warning("Rows with NULL request_count: %s", len(null_count))
        logging.warning(null_count.head())

   
    df["created_timestamp"] = datetime.now(timezone.utc)

    logging.info("Transformation complete.")

    return df




def load_to_postgres(df, engine):
    df.to_sql(
        TABLE_NAME,
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000
    )

    logging.info("Data appended successfully.")




def main():
    try:
        logging.info("Connecting to database...")
        engine = get_engine()

        logging.info("Ensuring table exists...")
        create_table_if_not_exists(engine)

        logging.info("Reading CSV file...")
        df = pd.read_csv(CSV_FILE_PATH, index_col=0)

        logging.info("Transforming data...")
        df = transform_data(df)

        logging.info("Loading data to PostgreSQL...")
        load_to_postgres(df, engine)

        logging.info("ETL process completed successfully.")

    except Exception as e:
        logging.error("ETL Failed: %s", e)
        raise


if __name__ == "__main__":
    main()