from sqlalchemy.dialects.postgresql import insert
from app.etl.core.db import SessionLocal
from app.etl.models.crypto_model import CryptoPrice
from app.etl.core.logging_config import logger

class Loader:
    def load(self, df):
        session = SessionLocal()
        try:
            data = df.to_dict(orient="records")
            if not data:
                logger.info("No data to load")
                return

            stmt = insert(CryptoPrice).values(data)
            stmt = stmt.on_conflict_do_update(
                index_elements=[CryptoPrice.id],
                set_={
                    "symbol": stmt.excluded.symbol,
                    "name": stmt.excluded.name,
                    "current_price": stmt.excluded.current_price,
                    "market_cap": stmt.excluded.market_cap,
                    "total_volume": stmt.excluded.total_volume,
                    "last_updated": stmt.excluded.last_updated,
                    "loaded_at": stmt.excluded.loaded_at
                }
            )
            session.execute(stmt)
            session.commit()
            logger.info(f"Loaded {len(data)} records to database")
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to load data: {e}")
            raise
        finally:
            session.close()

loader = Loader()
