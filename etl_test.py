import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os
import logging

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("etl_pipeline.log"), logging.StreamHandler()
                    ])

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PORT = os.getenv("DB_PORT")
DB_PASS = os.getenv("DB_PASS")

def extract_data(api_url):
    logging.info("Starting data extraction from API: %s", api_url)
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data)
        return df
    except requests.exceptions.RequestException as e:
        logging.error("Error extracting data from API: %s", exc_info=True)
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

# For checks if env file has been loaded
print("=== Environment Variables Debug ===")
print(f"DB_HOST: {DB_HOST}")
print(f"DB_NAME: {DB_NAME}")
print(f"DB_USER: {DB_USER}")
print(f"DB_PORT: {DB_PORT}")
print(f"DB_PASS exists: {os.getenv('DB_PASS') is not None}")
print(f"DB_PASS length: {len(os.getenv('DB_PASS') or '')}")


def load_pgsql(df):
    logging.info("Starting data loading to PostgreSQL")
    try:
        conn = psycopg2.connect(
            host = DB_HOST,
            dbname = DB_NAME,
            user = DB_USER,
            password = DB_PASS,
            port = DB_PORT
        )
        
        curr = conn.cursor()

        table_query = """ 
        CREATE TABLE IF NOT EXISTS crypto_price (
            id TEXT, 
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
        
        insert = """
            INSERT INTO crypto_price (id, symbol, name, current_price, market_cap, total_volume, last_updated, loaded_at) VALUES %s
        """
        
        execute_values(curr, insert, df.values.tolist())
        conn.commit()
        
        curr.close()
        conn.close()
        
        logging.info("Data loaded successfully!")
        
    except Exception as e:
        logging.error("Error loading data: %s", e, exc_info=True)
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        raise

API_URL = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd"
df1 = extract_data(API_URL)
df2 = transform_data(df1)
print(df2)
load_pgsql(df2)