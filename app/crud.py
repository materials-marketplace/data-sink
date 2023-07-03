from sqlalchemy.orm import Session

from app import models, schemas


def by_dataset_id(db: Session, dataset_id: str):
    return (
        db.query(models.Dataset)
        .filter(models.Dataset.dataset_id == dataset_id)
        .first()
    )


def create_dataset(db: Session, dataset: schemas.BinaryDataset):
    dataset = models.Dataset(dataset_id=dataset.dataset_id, data=dataset.data)
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
    dataset_db.data = dataset.data
    db.commit()


def delete_dataset(db: Session, dataset: schemas.BinaryDataset):
    dataset_db = (
        db.query(models.Dataset)
        .filter(models.Dataset.dataset_id == dataset.dataset_id)
        .first()
    )
    db.delete(dataset_db)
    db.commit()
