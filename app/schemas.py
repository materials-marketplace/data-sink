from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, BaseSettings, ConstrainedStr, Field


class BinaryDataset(BaseModel):
    dataset_id: str
    data: bytes
    hash: str = None

    class Config:
        orm_mode = True


class Dataset(BaseModel):
    dataset_name: str
    sub_collection_id: str | None = None
    host: str | None = None

    class Config:
        schema_extra = {
            "example": {"dataset_name": "dataset1", "sub_collection_id": None}
        }


class Collection(BaseModel):
    collection_name: str
    sub_collection_id: Optional[str] = None

    class Config:
        schema_extra = {
            "example": {
                "collection_name": "collection1",
                "sub_collection_id": None,
            }
        }


class Query(BaseModel):
    query: str
    meta_data: bool = False

    class Config:
        schema_extra = {
            "example": {
                "query": "SELECT ?subject ?predicate ?object WHERE "
                + "{ ?subject ?predicate ?object . } LIMIT 5",
                "meta_data": "False",
            }
        }


class QueryDataset(BaseModel):
    query: str

    class Config:
        schema_extra = {
            "example": {
                "query": "SELECT ?subject ?predicate ?object "
                + "WHERE { ?subject ?predicate ?object . } LIMIT 5"
            }
        }


class CollectionName(ConstrainedStr):
    min_length = 1
    max_length = 255


class CollectionModel(BaseModel):
    count: Optional[int]
    bytes: Optional[int]
    id: Optional[str]
    name: CollectionName
    last_modified: Optional[datetime]


class CollectionResponseModel(BaseModel):
    items: List[CollectionModel]


class CollectionCreateResponse(BaseModel):
    last_modified: datetime
    collection_id: Optional[str]


class DatasetName(ConstrainedStr):
    min_length = 1


class DatasetCreateResponse(BaseModel):
    last_modified: datetime


class DatasetModel(BaseModel):
    name: DatasetName
    hash: Optional[str]
    bytes: Optional[int]
    content_type: Optional[str]
    last_modified: Optional[datetime]


class DatasetResponseModel(BaseModel):
    items: List[DatasetModel]


class AppSettings(BaseSettings):
    """Datasink dependency settings for authorization"""

    postgres_user: str = Field("", description="Username of Postgres db")
    postgres_password = Field("", description="Password of Postgres db")
    postgres_host = Field("", description="Hostname of Postgres db")
    postgres_db = Field("", description="Database name of Postgres db")
    agraph_super_user = Field("", description="Allegrograh db super username")
    agraph_super_password = Field(
        "", description="Allegrograph super user password"
    )
    agraph_host = Field("", description="Allegrograph db hostname")
    auth_deps = Field(False, description="To enable end poinprotection or not")
    application_url = Field(
        "", description="Service URL of the datasink to download the data"
    )

    class Config:
        """Datasink application configuration."""

        env_file = ".env"
