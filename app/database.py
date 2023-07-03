from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from app.schemas import AppSettings

CONFIG = AppSettings()
SQLALCHEMY_DATABASE_URL = (
    "postgresql://{user}:{passwd}@{host}:{port}/{db}".format(
        user=CONFIG.postgres_user,
        passwd=CONFIG.postgres_password,
        host=CONFIG.postgres_host,
        port="5432",
        db=CONFIG.postgres_db,
    )
)

engine = create_engine(SQLALCHEMY_DATABASE_URL)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()
