from sqlalchemy import Column, String, BigInteger, Numeric, TIMESTAMP, text
from etl.models.base import Base

class CryptoPrice(Base):
    __tablename__ = "crypto_price"

    id = Column(String, primary_key=True)
    symbol = Column(String)
    name = Column(String)
    current_price = Column(Numeric(20, 2))
    market_cap = Column(BigInteger)
    total_volume = Column(BigInteger)
    last_updated = Column(TIMESTAMP)
    loaded_at = Column(TIMESTAMP, server_default=text("NOW()"))
