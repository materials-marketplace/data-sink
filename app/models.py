from sqlalchemy import Column, LargeBinary, String

from app.database import Base


class Dataset(Base):
    __tablename__ = "dataset"

    dataset_id = Column(String(length=36), primary_key=True)
    data = Column(LargeBinary, nullable=False)
