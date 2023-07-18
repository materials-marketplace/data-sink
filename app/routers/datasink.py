import json
from typing import Annotated

from fastapi import (
    APIRouter,
    Body,
    Depends,
    File,
    Form,
    HTTPException,
    Response,
    UploadFile,
    status,
)
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer
from rdflib import Graph
from sqlalchemy.orm import Session

from app.cuds_dataset import CudsDataset
from app.database import SessionLocal
from app.schemas import (
    CollectionCreateResponse,
    CollectionName,
    CollectionResponseModel,
    DatasetCreateResponse,
    DatasetName,
    DatasetResponseModel,
    Query,
    QueryDataset,
)

security = HTTPBearer()
data_sink_router = APIRouter()


# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@data_sink_router.get(
    "/data",
    status_code=status.HTTP_200_OK,
    operation_id="listCollections",
    response_model=CollectionResponseModel,
)
async def listCollections(
    db: Session = Depends(get_db),
) -> CollectionResponseModel:
    """list_collections

    Returns:
    dict: A list of Collections/Catalogs.
    """
    catalogs = CudsDataset.list_collections(db)
    return {"items": catalogs}


@data_sink_router.put(
    "/data",
    status_code=status.HTTP_201_CREATED,
    operation_id="createCollection",
    response_model=CollectionCreateResponse,
)
async def createCollection(
    collection_name: Annotated[CollectionName, Form()],
    sub_collection_id: Annotated[str, Form()] = None,
) -> CollectionCreateResponse:
    """create_collection

    Creates a Collection/Catalog in datastore with DCAT
    representation of meta-data.

    Parameters:
    - collection_name (str): The path parameter collection name value.
    - sub_collection_id (str): Collection ID to which the collection
      needs to be added. Needed only if you are adding
      it as a nested collection/catalog.

    Returns:
    dict: A dictionary containing the Collection ID created.
    """
    catalog_title = collection_name
    parent_collection_id = sub_collection_id

    parent_catalog_id = None
    if parent_collection_id is not None:
        parent_catalog_id = CudsDataset.by_catalog_id(parent_collection_id)
        # print("parent catalog ID ", parent_catalog_id)

    if parent_catalog_id is None:
        # This means we are creating top level collection and
        # the title should be unique
        print(CudsDataset.by_catalog_title)
        catalog_id = CudsDataset.by_catalog_title(
            catalog_title, root_only=True
        )
        if catalog_id is not None:
            raise HTTPException(
                status_code=400,
                detail="There is already a collection with given name."
                + " Root collection name should be always unique.",
            )
        print("Creating catalog: ", catalog_title)
        response = CudsDataset.create_catalog(catalog_title, parent_catalog_id)
    else:
        print("Creating catalog: ", catalog_title)
        response = CudsDataset.create_catalog(catalog_title, parent_catalog_id)

    return response


@data_sink_router.get(
    "/data/{collection_name}",
    operation_id="listDatasets",
    response_model=DatasetResponseModel,
)
async def listDatasets(
    collection_name: CollectionName, db: Session = Depends(get_db)
) -> DatasetResponseModel:
    """list_datasets

    Parameters:
    - collection_name (str): The path parameter collection name value.

    Returns:
    dict: A list of datasets.
    """
    datasets = CudsDataset.list_datasets(collection_name, db)
    return {"items": datasets}


@data_sink_router.delete(
    "/data/{collection_name}",
    status_code=status.HTTP_204_NO_CONTENT,
    operation_id="deleteCollection",
)
async def deleteCollection(
    collection_name: CollectionName, db: Session = Depends(get_db)
):
    """delete_collection

    Parameters:
    - collection_name (str): The path parameter collection name value.

    Returns:
    No content
    """
    result = CudsDataset.delete_collection(collection_name, db)
    if result is None:
        raise HTTPException(
            status_code=404,
            detail="There is no collection at the given entry.",
        )


@data_sink_router.get(
    "/data/{collection_name}/{dataset_name}", operation_id="getDataset"
)
async def getDataset(
    collection_name: CollectionName,
    dataset_name: DatasetName,
    db: Session = Depends(get_db),
) -> Response:
    """get_dataset

    Parameters:
    - collection_name (str): The path parameter collection name value.
    - dataset_name (str):  The path parameter Dataset name.

    Returns:
    Data payload as binary data.
    """
    # use existing function to fetch single collection as RDF
    response = CudsDataset.get_data(collection_name, dataset_name, db)
    if response is None:
        raise HTTPException(
            status_code=404, detail="There is no dataset at the given entry."
        )

    response = Response(response)
    # Set the appropriate headers
    response.headers[
        "Content-Disposition"
    ] = f'attachment; filename="{dataset_name}"'
    response.headers["Content-Type"] = "application/octet-stream"

    return response


@data_sink_router.put(
    "/data/{collection_name}",
    status_code=status.HTTP_201_CREATED,
    operation_id="createDataset",
    response_model=DatasetCreateResponse,
)
async def createDataset(
    collection_name: CollectionName,
    dataset_name: Annotated[DatasetName, Body()],
    sub_collection_id: Annotated[str, Body()] = None,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
) -> DatasetCreateResponse:
    """create_dataset

    Stores the dataset in the datastore with DCAT representation of meta-data.

    Parameters:
    - collection_name (str): The path parameter collection name value.
    - dataset_name (str): Dataset name.
    - sub_collection_id (str): Collection ID to which the dataset needs
      to be added. Needed only if you are adding the dataset
      to a nested collection/catalog.
    - file (UploadFile): The binary file uploaded as
      part of the multipart form data.

    Returns:
    dict: A dictionary containing the dataset ID created.
    """
    if sub_collection_id == "":
        sub_collection_id = None

    # read the contents of the file
    data = await file.read()

    dataset_title = dataset_name

    parent_catalog_id = None

    # find the root collection
    catalog_id = CudsDataset.by_catalog_title(collection_name, root_only=True)
    if catalog_id is None:
        raise HTTPException(
            status_code=404,
            detail="There is no collection with given " + "collection name.",
        )
    parent_catalog_id = catalog_id

    # find if there is already a dataset with
    # the same name exists in root collection
    datasets = CudsDataset.by_dataset_title(collection_name, dataset_title)
    if len(datasets) > 0:
        raise HTTPException(
            status_code=409,
            detail="There is already a dataset with same "
            + "name in the given collection name",
        )

    # collection_id corresponds to sub folder ID
    # to which the dataset should be atatched
    if sub_collection_id is not None:
        sub_collection = CudsDataset.by_catalog_id(sub_collection_id)
        if sub_collection is None:
            raise HTTPException(
                status_code=404,
                detail="There is no collection found "
                + "with given sub_collection_id",
            )
        parent_catalog_id = sub_collection_id

    response = CudsDataset.create_dataset(
        dataset_title, collection_name, data, parent_catalog_id, db
    )
    return response


@data_sink_router.delete(
    "/data/{collection_name}/{dataset_name}",
    status_code=status.HTTP_204_NO_CONTENT,
    operation_id="deleteDataset",
)
async def deleteDataset(
    collection_name: CollectionName,
    dataset_name: DatasetName,
    db: Session = Depends(get_db),
):
    """delete_dataset

    Parameters:
    - collection_name (str): The path parameter collection name value.
    - dataset_name (str): The path parameter dataset name value.

    Returns:
    No content
    """
    result = CudsDataset.delete_dataset(collection_name, dataset_name, db)
    if result is None:
        raise HTTPException(
            status_code=404, detail="There is no dataset at the given entry."
        )


@data_sink_router.get(
    "/metadata/dcat/{collection_name}",
    operation_id="getCollectionMetadataDcat",
)
async def getCollectionMetadataDcat(
    collection_name: CollectionName,
) -> JSONResponse:
    """get_collection_dcat

    Parameters:
    - collection_name (str): The path parameter collection name value.

    Returns:
    DCAT representation of collection in JSON-ld format.
    """
    # meta data is returned only for Root catalogs.
    # Subdirectories/sub catalogs are not returned.
    catalog_id = CudsDataset.by_catalog_title(collection_name, root_only=True)
    if catalog_id is not None:
        catalog = CudsDataset.export_catalog(catalog_id)
        graph = Graph()
        graph = graph.parse(data=catalog, format="json-ld")
        mime_type = "application/ld+json"
        response = JSONResponse(
            json.loads(graph.serialize(format=mime_type)), media_type=mime_type
        )
        return response
    else:
        raise HTTPException(
            status_code=404,
            detail="There is no Root catalog at the given entry.",
        )


@data_sink_router.get(
    "/metadata/dcat/{collection_name}/{dataset_name}",
    operation_id="getDatasetMetadataDcat",
)
async def getDatasetMetadataDcat(
    collection_name: CollectionName, dataset_name: DatasetName
) -> JSONResponse:
    """get_dataset_dcat

    Parameters:
    - collection_name (str): The path parameter collection name value.
    - dataset_name (str): The path parameter dataset name value.

    Returns:
    DCAT representation of dataset in JSON-ld format.
    """
    dataset = CudsDataset.get_dataset_from_collection(
        collection_name, dataset_name
    )
    if dataset is None:
        raise HTTPException(
            status_code=404, detail="There is no dataset with given name."
        )

    graph = Graph()
    graph = graph.parse(data=dataset, format="json-ld")
    mime_type = "application/ld+json"
    response = JSONResponse(
        json.loads(graph.serialize(format=mime_type)), media_type=mime_type
    )
    return response


@data_sink_router.post("/query", operation_id="query")
async def query(query: Query) -> JSONResponse:
    """query

    Executes a spqrql query on the entire triple store.

    Parameters:
    - query: A string representing sparql query
    - meta_data: Represents whether to query metadata or the actual data.

    Returns:
    List of matching rows as JSON data.
    """
    decoded_query = query.query
    meta_data = query.meta_data
    result = CudsDataset.query(decoded_query, meta_data)
    response = JSONResponse(result, media_type="json")
    return response


@data_sink_router.post(
    "/query/{collection_name}/{dataset_name}", operation_id="queryDataset"
)
async def queryDataset(
    collection_name: CollectionName,
    dataset_name: DatasetName,
    query: Annotated[QueryDataset, Body()] = None,
) -> JSONResponse:
    """query_dataset

    Executes a spqrql query on triples specific to a dataset.

    Parameters:
    - query: A string representing sparql query
    - collection_name (str): The path parameter collection name value.
    - dataset_name (str): The path parameter dataset name value.

    Returns:
    List of matching rows as JSON data.
    """
    decoded_query = query.query
    result = CudsDataset.named_graph_query(
        collection_name, dataset_name, decoded_query
    )
    response = JSONResponse(result, media_type="json")
    return response
