import pandas as pd

def validate_and_cast(df):
    required = ["id","symbol","name","current_price","market_cap","total_volume","last_updated"]
    for c in required:
        if c not in df.columns:
            raise ValueError(f"Missing column: {c}")

    df = df.copy()
    df["id"] = df["id"].astype(str)
    df["symbol"] = df["symbol"].astype(str)
    df["name"] = df["name"].astype(str)
    df["current_price"] = df["current_price"].astype(float)
    df["market_cap"] = pd.to_numeric(df["market_cap"], errors="coerce").fillna(0).astype("Int64")
    df["total_volume"] = pd.to_numeric(df["total_volume"], errors="coerce").fillna(0).astype(int).astype("Int64")
    df["last_updated"] = pd.to_datetime(df["last_updated"], errors="coerce")
    df["loaded_at"] = pd.Timestamp.utcnow()
    
    return df
