from sqlalchemy.orm import Session

from app import models, schemas
import hashlib


def by_dataset_id(db: Session, dataset_id: str):
    return (
        db.query(models.Dataset)
        .filter(models.Dataset.dataset_id == dataset_id)
        .first()
    )


def create_dataset(db: Session, dataset: schemas.BinaryDataset):
    md5_hash = hashlib.md5(dataset.data).hexdigest()
    dataset = models.Dataset(dataset_id=dataset.dataset_id, data=dataset.data, hash=md5_hash)
    db.add(dataset)
    db.commit()
    db.refresh(dataset)
    return dataset.dataset_id


def update_dataset(db: Session, dataset: schemas.BinaryDataset):
    dataset_db = (
        db.query(models.Dataset)
        .filter(models.Dataset.dataset_id == dataset.dataset_id)
        .first()
    )
    md5_hash = hashlib.md5(dataset.data).hexdigest()
    dataset_db.data = dataset.data
    dataset.hash = md5_hash
    db.commit()


def delete_dataset(db: Session, dataset: schemas.BinaryDataset):
    dataset_db = (
        db.query(models.Dataset)
        .filter(models.Dataset.dataset_id == dataset.dataset_id)
        .first()
    )
    db.delete(dataset_db)
    db.commit()
