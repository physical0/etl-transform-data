from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.etl.core.config import settings

def get_engine():
    return create_engine(settings.DATABASE_URL, pool_size=10, max_overflow=20, future=True)

engine = get_engine()
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
