import os
import requests
import logging
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from tenacity import retry, wait_fixed,stop_after_attempt, retry_if_exception_type
from psycopg2.extras import execute_values
from multiprocessing import Pool, cpu_count

load_dotenv()

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler("etl_pipeline.log"), logging.StreamHandler()]
                    )

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT")

def extract_data(api_url):
    logging.info("(New run) Starting data extraction from API: %s", api_url)
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data)
        return df
    except requests.exceptions.RequestException as e:
        logging.error("Error extracting from API: %s", exc_info=True)
        raise
    
def transform_data(df):
    try:
        logging.info("Starting data transformation")
        df = df[['id', 'name', 'symbol', 'current_price', 'market_cap', 'total_volume', 'last_updated']]
    
        df = df.dropna()
    
        df['loaded_at'] = pd.Timestamp.now()
    
        return df
    except Exception as e:
        logging.error("Error transforming data: %s", exc_info=True)
        raise

def validate_data(df):
    required_cols = {
        "id": str,
        "symbol": str,
        "current_price": float,
        "market_cap": int,
        "total_volume": int,
        "last_updated": "datetime64[ns]",
        "loaded_at": "datetime64[ns]"
    }
    
    for col, dtype in required_cols.items():
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")
        
        if dtype == str:
            df[col] = df[col].astype(str)
        elif dtype == int:
            df[col] = df[col].astype("Int64")
        elif dtype == float:
            df[col] = df[col].astype(float)
        else:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            
    
    return df


def chunk_data(df, chunk_size):
    for i in range(0, len(df), chunk_size):
        yield df.iloc[i:i+chunk_size]


@retry(
    retry = retry_if_exception_type(psycopg2.OperationalError),
    wait = wait_fixed(2),
    stop = stop_after_attempt(5),    
)  
def connect_pgsql():
    logging.info("Connecting to PostgreSQL")
    conn = psycopg2.connect(
        host = DB_HOST,
        dbname = DB_NAME,
        user = DB_USER,
        password = DB_PASS,
        port = DB_PORT
    )
    return conn
    

def upsert_batch(batch_data):
    batch, upsert_query, pg_size = batch_data
    try:
        conn = connect_pgsql()
        curr = conn.cursor()
        execute_values(curr, upsert_query, batch, page_size=pg_size)
        conn.commit()
        curr.close()
        logging.info(f"Inserted batch of size {len(batch)}")
        
        
    except Exception as e:
        logging.error(f"(Batch insert failed: {e})")
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        
        
def parallel_insert(df, upsert_query, pg_size=100, chunk_size=1000, workers=None):
    if workers is None:
        workers = max(2, cpu_count() // 2)

    logging.info(f"Running parallel insert with {workers} workers...")
    
    batches = []
    for chunk in chunk_data(df, chunk_size):
        batch_data = (chunk.values.tolist(), upsert_query, pg_size)
        batches.append(batch_data)

    with Pool(workers) as pool:
        pool.map(upsert_batch(), batches)
        

def load_and_update_pgsql(df):
    logging.info("Starting data loading to PostgreSQL")
    try:
        conn = connect_pgsql()
        
        curr = conn.cursor()

        logging.info("Creating a table if it doesn't exist..")
        table_query = """ 
        CREATE TABLE IF NOT EXISTS crypto_price (
            id TEXT PRIMARY KEY, 
            symbol TEXT, 
            name TEXT, 
            current_price DECIMAL(20, 2), 
            market_cap BIGINT, 
            total_volume BIGINT, 
            last_updated TIMESTAMP,
            loaded_at TIMESTAMP
        ) 
        """
        
        curr.execute(table_query)
        conn.commit()
        
        logging.info("Inserting data after table checking and creation..")
        insert = """
            INSERT INTO crypto_price (id, symbol, name, current_price, market_cap, total_volume, last_updated, loaded_at) VALUES %s
        """
        
        execute_values(curr, insert, df.values.tolist())
        conn.commit()
        
        
        curr.close()
        conn.close()
        
        logging.info("Data loaded successfully, ETL completed!")
        
    except Exception as e:
        logging.error("Error loading data: %s", e, exc_info=True)
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        raise
    
    finally:
        if 'curr' in locals(): curr.close()
        if 'conn' in locals(): conn.close()
        logging.info("Database connection closed.")


if __name__ == "__main__":
    # For checks if env file has been loaded
    print("=== Environment Variables Debug ===")
    print(f"DB_HOST: {DB_HOST}")
    print(f"DB_NAME: {DB_NAME}")
    print(f"DB_USER: {DB_USER}")
    print(f"DB_PORT: {DB_PORT}")
    print(f"DB_PASS exists: {os.getenv('DB_PASS') is not None}")
    print(f"DB_PASS length: {len(os.getenv('DB_PASS') or '')}")
    
    
    UPSERT_QUERY = """
        INSERT INTO crypto_price (
            id, symbol, name, current_price, market_cap, total_volume, last_updated, loaded_at
        )
        VALUES %s
        ON CONFLICT (id, last_updated) DO UPDATE SET
            symbol = EXCLUDED.symbol,
            name = EXCLUDED.name,
            current_price = EXCLUDED.current_price,
            market_cap = EXCLUDED.market_cap,
            total_volume = EXCLUDED.total_volume,
            loaded_at = EXCLUDED.loaded_at;
    """
    
        
    API_URL = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd"
    df1 = extract_data(API_URL)
    df2 = transform_data(df1)
    print(df2)
    load_pgsql(df2)
            

        





