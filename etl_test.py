import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PORT = os.getenv("DB_PORT")

def extract_data():
    url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd"
    response = requests.get(url)
    data = response.json()
    df = pd.DataFrame(data)
    return df


def transform_data(df):
    
    df = df[['id', 'symbol', 'current_price', 'market_cap', 'total_volume', 'last_updated']]
    
    df = df.dropna()
    
    df['loaded_at'] = pd.Timestamp.now()
    
    return df


df1 = extract_data()
df2 = transform_data(df1)
print(df2)