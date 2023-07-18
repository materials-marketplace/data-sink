import io
from datetime import datetime
from io import StringIO
from uuid import uuid4

import rdflib
from fastapi import HTTPException
from rdflib import ConjunctiveGraph, Graph
from simphony_osp.namespaces import dcat3, dcterms, owl
from simphony_osp.ontology import OntologyIndividual
from simphony_osp.tools import export_file, find, import_file, sparql
from simphony_osp.wrappers import AllegroGraph

from app.crud import by_dataset_id, create_dataset, delete_dataset
from app.schemas import (
    AppSettings,
    BinaryDataset,
    CollectionCreateResponse,
    DatasetCreateResponse,
)

CONFIG = AppSettings()

# "json" is an extra support by osp-core
# while the rest formats are supported by rdflib (version 6.0.0)
# https://rdflib.readthedocs.io/en/stable/plugin_serializers.html
# "turtle" and "xml" are more often used and thus arranged first
supported_formats = [
    ("json", "https://www.iana.org/assignments/media-types/application/json"),
    ("turtle", "https://www.iana.org/assignments/media-types/text/turtle"),
    ("xml", "https://www.iana.org/assignments/media-types/text/xml"),
    ("html", "https://www.iana.org/assignments/media-types/text/html"),
    ("hturtle", ""),
    ("mdata", ""),
    ("microdata", ""),
    ("n3", "https://www.iana.org/assignments/media-types/text/n3"),
    ("nquads", ""),
    ("nt", ""),
    ("rdfa", ""),
    ("rdfa1.0", ""),
    ("rdfa1.1", ""),
    ("trix", ""),
]


class CudsDataset:
    @classmethod
    def by_catalog_id(cls, catalog_id: str):
        """by_catalog_id

        Identify a catalog/collection by its ID.

        Parameters:
        - catalog_id (str): Identifier of catalog.

        Returns:
        str: Identifier if found.
        """
        with allegro_graph_session() as session:
            result = sparql(
                f"""
            SELECT ?dataset FROM <{catalog_id}> WHERE {{
                ?dataset rdf:type <{dcat3.Catalog.iri}> .
                ?dataset dcterms:identifier ?identifier
                FILTER (?identifier = "{catalog_id}")
            }}
            """,
                session=session,
            )(dataset=OntologyIndividual)
            catalogs = list(result)
            if len(catalogs) > 0:
                return list(catalogs[0][0][dcterms.identifier])[0]
            return None

    @classmethod
    def by_catalog_title(cls, title: str, root_only=False):
        with allegro_graph_session() as session:
            if not root_only:
                query = f"""
                    SELECT ?dataset WHERE {{
                        ?dataset rdf:type <{dcat3.Catalog.iri}> .
                        ?dataset dcterms:title ?title
                        FILTER (?title = "{title}")
                    }}
                    """
            else:
                query = f"""
                SELECT ?dataset WHERE {{
                    ?dataset rdf:type <{dcat3.Catalog.iri}> .
                    ?dataset dcterms:title ?title .
                    ?dataset dcterms:type ?type
                    FILTER (?title="{title}" \
                        && ?type="http://purl.org/dc/dcmitype/Collection")
                }}
                """
            result = sparql(query, session=session)(dataset=OntologyIndividual)
            catalogs = list(result)
            if len(catalogs) > 0:
                return list(catalogs[0][0][dcterms.identifier])[0]
            return None

    @classmethod
    def by_dataset_id(cls, dataset_id: str):
        """by_dataset_id

        Identify a dataset by its ID.

        Parameters:
        - dataset_id (str): Identifier of dataset.

        Returns:
        str: Identifier if found.
        """
        with allegro_graph_session() as session:
            result = sparql(
                f"""
            SELECT ?dataset FROM <{dataset_id}> WHERE {{
                ?dataset rdf:type <{dcat3.Dataset.iri}> .
                ?dataset dcterms:identifier ?identifier
                FILTER (?identifier = "{dataset_id}")
            }}
            """,
                session=session,
            )(dataset=OntologyIndividual)
            datasets = list(result)
            if len(datasets) > 0:
                return list(datasets[0][0][dcterms.identifier])[0]
            return None

    @classmethod
    def by_dataset_title(cls, collection_name, dataset_name: str):
        """by_dataset_title

        Identify a dataset by its collection name and dataset name.

        Parameters:
        - collection_name (str): name of collection.
        - dataset_name (str): name of dataset.

        Returns:
        list: Matching dataset DCAT objects.
        """
        datasets = []
        with allegro_graph_session() as session:
            query = f"""
                SELECT ?catalog WHERE {{
                    ?catalog rdf:type <{dcat3.Catalog.iri}> .
                    ?catalog dcterms:title ?title.
                    ?catalog dcterms:type ?type
                    FILTER (?type="http://purl.org/dc/dcmitype/Collection" \
                        && ?title="{collection_name}")
                }}
                """
            result = sparql(query, session=session)(catalog=OntologyIndividual)
            catalogs = list(result)
            if len(catalogs) > 0:
                catalog = catalogs[0][0]
                individuals = list(find(catalog, rel=owl.topObjectProperty))
                for individual in individuals:
                    if (
                        not individual.is_a(dcat3.Catalog)
                        and not individual.is_a(dcat3.Distribution)
                        and not individual.is_a(dcat3.DataService)
                    ):
                        dataset = individual
                        dist = list(dataset[dcat3.distribution])[0]
                        if list(dist[dcterms.title])[0] == dataset_name:
                            datasets.append(dataset)
                            break
        return datasets

    @classmethod
    def create_catalog(cls, catalog_title, parent_catalog_id) -> str:
        """create_catalog

        Create a collection/catalog by adding DCAT metadata into triple store.

        Parameters:
        - catalog_title (str): name of collection.
        - parent_catalog_id (str): ID of parent catalog.
          Needed to add sub directory with in a directory.

        Returns:
        dict: Meta data information which includes catalog_id created.
        """
        catalog_uid = str(uuid4())
        parent_catalog = None
        modified_time = str(datetime.now())
        json_data = {
            "@context": {
                "dcat3": "http://www.w3.org/ns/dcat#",
                "dcterms": "http://purl.org/dc/terms/",
                "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            },
            "@graph": [
                {
                    "@id": "http://marketplace-datasink.org/catalogs/"
                    + catalog_uid,
                    "@type": "dcat3:Catalog",
                    "dcterms:title": catalog_title,
                    "dcterms:identifier": catalog_uid,
                    "dcterms:issued": {
                        "@type": "xsd:date",
                        "@value": modified_time,
                    },
                    "dcterms:modified": {
                        "@type": "xsd:date",
                        "@value": modified_time,
                    },
                }
            ],
        }

        with allegro_graph_session() as session:
            if parent_catalog_id is None:
                json_data["@graph"][0][
                    "dcterms:type"
                ] = "http://purl.org/dc/dcmitype/Collection"
            else:
                result = sparql(
                    f"""
                SELECT ?dataset WHERE {{
                    ?dataset rdf:type <{dcat3.Catalog.iri}> .
                    ?dataset dcterms:identifier ?identifier
                    FILTER (?identifier = "{parent_catalog_id}")
                }}
                """,
                    session=session,
                )(dataset=OntologyIndividual)
                parent_catalog = list(result)[0][0]
                json_data["@graph"][0]["dcterms:isPartOf"] = parent_catalog.iri

            cuds = import_file(json_data, format="json-ld", all_triples=True)

            catalog = list(cuds)[0]

            if parent_catalog is not None:
                # Link Catalog to Catalog for nested folders
                parent_catalog[dcat3.catalog] += catalog

            session.commit()
            return CollectionCreateResponse(
                collection_id=catalog_uid, last_modified=modified_time
            )

    @classmethod
    def create_dataset(
        cls, dataset_title, collection_title, data, parent_catalog_id, db
    ) -> str:
        """create_dataset

        Create a dataset by adding DCAT metadata into triple store.

        Parameters:
        - dataset_title (str): name of the dataset.
        - collection_title (str): name of the collection.
        - parent_catalog_id (str): ID of parent catalog.
          Needed to add dataset within a sub directory.
        - db : postgres sql database session.

        Returns:
        dict: Meta data information which includes dataset_id created.
        """
        dataset_uid = str(uuid4())
        named_graph = collection_title + "_" + dataset_title
        dataset_iri = "http://marketplace-datasink.org/datasets/" + dataset_uid
        is_cuds = False
        cuds_format = None
        modified_time = str(datetime.now())
        try:
            decoded_data = data.decode()
            is_cuds, cuds_format = cls._is_cuds(decoded_data)
        except Exception:
            is_cuds = False

        if is_cuds:
            data_type = cuds_format
        else:
            data_type = "raw"

        json_data = {
            "@context": {
                "dcat3": "http://www.w3.org/ns/dcat#",
                "dcterms": "http://purl.org/dc/terms/",
                "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            },
            "@graph": [
                {
                    "@id": dataset_iri,
                    "@type": "dcat3:Dataset",
                    "dcterms:identifier": dataset_uid,
                    "dcterms:issued": {
                        "@type": "xsd:date",
                        "@value": modified_time,
                    },
                    "dcterms:modified": {
                        "@type": "xsd:date",
                        "@value": modified_time,
                    },
                    "dcat3:distribution": {
                        "@id": "http://marketplace-datasink.org/distributions/"
                        + dataset_uid
                        + "/"
                        + dataset_title
                    },
                },
                {
                    "@id": "http://marketplace-datasink.org/distributions/"
                    + dataset_uid
                    + "/"
                    + dataset_title,
                    "@type": "dcat3:Distribution",
                    "dcterms:format": data_type,
                    "dcterms:title": dataset_title,
                    "dcat3:downloadURL": CONFIG.application_url
                    + "/data/"
                    + collection_title
                    + "/"
                    + dataset_title,
                    "dcterms:modified": {
                        "@type": "xsd:date",
                        "@value": modified_time,
                    },
                },
            ],
        }

        with allegro_graph_session() as session:
            try:
                result = sparql(
                    f"""
                SELECT ?catalog WHERE {{
                    ?catalog rdf:type <{dcat3.Catalog.iri}> .
                    ?catalog dcterms:identifier ?identifier
                    FILTER (?identifier = "{parent_catalog_id}")
                }}
                """,
                    session=session,
                )(catalog=OntologyIndividual)
                catalog = list(result)[0][0]

                cuds = import_file(
                    json_data,
                    session=session,
                    format="json-ld",
                    all_triples=True,
                )

                dataset = None
                for object in cuds:
                    if object.is_a(dcat3.Dataset):
                        dataset = object

                # Link dataset to Catalog
                catalog[dcat3.dataset] += dataset
                dataset[dcterms.isPartOf] += catalog.iri
                if is_cuds:
                    g = data_space_session()
                    named_graph = Graph(g.store, named_graph)
                    named_graph.parse(
                        io.StringIO(decoded_data), format=cuds_format
                    )
                    # g.parse(io.StringIO(decoded_data), format=cuds_format)

                    binary_dataset = BinaryDataset(
                        dataset_id=dataset_uid, data=data
                    )
                    dataset = create_dataset(db=db, dataset=binary_dataset)
                    g.commit()
                    g.close()
                else:
                    binary_dataset = BinaryDataset(
                        dataset_id=dataset_uid, data=data
                    )
                    dataset = create_dataset(db=db, dataset=binary_dataset)

                session.commit()

            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        return DatasetCreateResponse(last_modified=modified_time)

    @classmethod
    def query(cls, query_string, meta_data=False):
        """query

        Query triple store to get mathing triples.

        Parameters:
        - query_string (str): SPARQL query.
        - meta_data (str): Whether to query DCAT meta data or
          the content of the datastore.

        Returns:
        list: List of matching triples.
        """
        try:
            if meta_data:
                with allegro_graph_session() as session:
                    result = sparql(query_string, session=session)
            else:
                g = data_space_session()
                result = g.store.query(query_string)
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=str(e),
            )
        return list(result)

    @classmethod
    def named_graph_query(cls, collection_name, dataset_name, query_string):
        """named_graph_query

        Query triple store to get mathing triples within a specific dataset.

        Parameters:
        - collection_name (str): name of the collection.
        - dataset_name (str): name of the dataset.
        - query_string (str): SPARQL query.

        Returns:
        list: List of matching triples.
        """
        try:
            g = data_space_session()
            named_graph = collection_name + "_" + dataset_name
            context = g.get_context(named_graph)
            triples = context.triples((None, None, None))

            g = Graph()
            for triple in triples:
                g.add(triple)
            result = g.query(query_string)
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=str(e),
            )
        return list(result)

    @classmethod
    def delete_collection(cls, collection_name, db=None):
        """delete_collection

        Delete a specific collection/catalog from datastore.

        Parameters:
        - collection_name (str): name of the collection.
        - db : postgres sql database session.

        Returns:
        bool: True if deletion is success.
        """
        catalog_id = cls.by_catalog_title(collection_name, root_only=True)
        if catalog_id is None:
            raise HTTPException(
                status_code=404,
                detail="There is no Root collection " + "with given name.",
            )
        else:
            individual_datasets = cls.list_datasets(collection_name, db)
            has_datasets = False

            for dataset in individual_datasets:
                if dataset["dcat_type"] == "http://www.w3.org/ns/dcat#Dataset":
                    has_datasets = True

            # if it has any datasets or distributions
            # it means the collection is not empty.
            # user has to delete them first.
            if has_datasets:
                raise HTTPException(
                    status_code=409,
                    detail="Collection is not empty. Please "
                    + "remove all the individual datasets "
                    + "and then try again.",
                )
            else:
                with allegro_graph_session() as session:
                    query = f"""
                        SELECT ?catalog WHERE {{
                            ?catalog rdf:type <{dcat3.Catalog.iri}> .
                            ?catalog dcterms:title ?title.
                            ?catalog dcterms:type ?type
                            FILTER (?type=\
                                "http://purl.org/dc/dcmitype/Collection" \
                                && ?title="{collection_name}")
                        }}
                        """
                    result = sparql(query)(catalog=OntologyIndividual)
                    datasets = list(result)
                    if len(datasets) > 0:
                        dataset = datasets[0][0]
                        individuals = list(
                            find(dataset, rel=owl.topObjectProperty)
                        )
                        for individual in individuals:
                            if individual.is_a(dcat3.Catalog):
                                catalog_iri = individual.iri
                                triples = session.graph.triples(
                                    (
                                        (rdflib.term.URIRef(catalog_iri)),
                                        None,
                                        None,
                                    )
                                )
                                for triple in list(triples):
                                    session.graph.remove(triple)
                        session.commit()

        return True

    @classmethod
    def delete_dataset(cls, collection_name, dataset_name, db):
        """delete_dataset

        Delete a specific dataset from datastore.

        Parameters:
        - collection_name (str): name of the collection.
        - dataset_name (str): name of the dataset.
        - db : postgres sql database session.

        Returns:
        bool: True if deletion is success.
        """
        identifier = None
        format = "raw"
        with allegro_graph_session() as session:
            query = f"""
                SELECT ?catalog WHERE {{
                    ?catalog rdf:type <{dcat3.Catalog.iri}> .
                    ?catalog dcterms:title ?title.
                    ?catalog dcterms:type ?type
                    FILTER (?type="http://purl.org/dc/dcmitype/Collection" \
                        && ?title="{collection_name}")
                }}
                """
            result = sparql(query)(catalog=OntologyIndividual)
            catalogs = list(result)
            if len(catalogs) > 0:
                catalog = catalogs[0][0]
                individuals = list(find(catalog, rel=owl.topObjectProperty))
                dataset_individuals = []
                for individual in individuals:
                    if (
                        not individual.is_a(dcat3.Catalog)
                        and not individual.is_a(dcat3.Distribution)
                        and not individual.is_a(dcat3.DataService)
                    ):
                        dataset = individual
                        dist = list(dataset[dcat3.distribution])[0]
                        if list(dist[dcterms.title])[0] == dataset_name:
                            dataset_individuals.append(dataset)
                            dataset_individuals.append(dist)

                            identifier = list(dataset[dcterms.identifier])[0]
                            format = list(dist[dcterms.format])[0]
                            dataset_iri = dataset.iri

                            # unlink it from the catalog
                            triples = session.graph.triples(
                                (
                                    None,
                                    rdflib.term.URIRef(
                                        "http://www.w3.org/ns/dcat#dataset"
                                    ),
                                    rdflib.term.URIRef(dataset_iri),
                                )
                            )
                            for triple in list(triples):
                                session.graph.remove(triple)

                            # delete meta data information
                            data = export_file(
                                dataset_individuals,
                                file=None,
                                format="turtle",
                                all_triples=True,
                            )
                            g = Graph()
                            g.parse(io.StringIO(data), format="turtle")
                            triples = g.triples((None, None, None))
                            triples = list(triples)
                            for triple in triples:
                                session.graph.remove(triple)
                            break

            else:
                raise HTTPException(
                    status_code=404,
                    detail="There is no root collection " + "with given name.",
                )
            # delete actual data
            if identifier is not None:
                dataset = by_dataset_id(db=db, dataset_id=identifier)
                if format != "raw":
                    named_graph = collection_name + "_" + dataset_name
                    g = data_space_session()
                    context = g.get_context(named_graph)
                    g.remove_context(context)
                    g.commit()
                """tps = list(session.graph.triples((None, None, None)))
                print("triples after data metadata: ", len(tps)) """

                delete_dataset(db=db, dataset=dataset)
                # commit after the postgres update to make
                # sure both db are in sync
                session.commit()
                return identifier
        return None

    @classmethod
    def list_collections(cls, db=None):
        """list_collections

        Return all the catalogs/collections in the datastore.

        Parameters:
        - db : postgres sql database session.

        Returns:
        list: List of metadata information of all collections.
        """
        collections = []
        with allegro_graph_session() as session:
            # send only root catalogs. Only root catalogs
            # will have the attribute dcterms:type
            query = """
                SELECT ?catalog WHERE {{
                    ?catalog rdf:type <http://www.w3.org/ns/dcat#Catalog> .
                    ?catalog dcterms:type ?type
                    FILTER (?type = "http://purl.org/dc/dcmitype/Collection")
                }}
                """
            result = sparql(query, session=session)(catalog=OntologyIndividual)
            catalogs = list(result)
            for catalog in catalogs:
                individuals = list(find(catalog[0], rel=owl.topObjectProperty))
                for individual in individuals:
                    # consider only root catalog. ignore sub folder catalogs
                    if (
                        individual.is_a(dcat3.Catalog)
                        and len(list(individual[dcterms.type])) > 0
                    ):
                        title = list(individual[dcterms.title])[0]
                        datasets = cls.list_datasets(title, db)
                        total_size = 0

                        for dataset in datasets:
                            size = dataset["bytes"]
                            # ignore sub folders with in collection
                            if size is None:
                                size = 0
                            total_size += size

                        catalog = {
                            "name": title,
                            "id": list(individual[dcterms.identifier])[0],
                            "last_modified": list(
                                individual[dcterms.modified]
                            )[0],
                            "dcat_type": "http://www.w3.org/ns/dcat#Catalog",
                            "count": len(datasets),
                            "bytes": total_size,
                        }
                        collections.append(catalog)
        return collections

    @classmethod
    def list_datasets(cls, collection_name, db=None):
        """list_datasets

        Return all the datasets with in a specific collection in the datastore.

        Parameters:
        - collection_name (str): name of the collection.
        - db : postgres sql database session.

        Returns:
        list: List of metadata information of all datasets.
        """
        datasets = []
        with allegro_graph_session() as session:
            query = f"""
                SELECT ?catalog WHERE {{
                    ?catalog rdf:type <{dcat3.Catalog.iri}> .
                    ?catalog dcterms:title ?title.
                    ?catalog dcterms:type ?type
                    FILTER (?type="http://purl.org/dc/dcmitype/Collection" \
                        && ?title="{collection_name}")
                }}
                """
            result = sparql(query, session=session)(catalog=OntologyIndividual)
            catalogs = list(result)
            if len(catalogs) > 0:
                catalog = catalogs[0][0]
                individuals = list(find(catalog, rel=owl.topObjectProperty))
                for individual in individuals:
                    # identify all the dataset objects and non root
                    # catalog objects. Ignore the actual root object here
                    # print(individual, list(individual[dcterms.title]))
                    if (
                        not individual.is_a(dcat3.Catalog)
                        or list(individual[dcterms.title])[0]
                        != collection_name
                    ):
                        if individual.is_a(dcat3.Dataset):
                            if individual.is_a(dcat3.Catalog):
                                type = "http://www.w3.org/ns/dcat#Catalog"
                                title = list(individual[dcterms.title])[0]
                                size = None
                                hash = None
                                content_type = None
                            else:
                                type = "http://www.w3.org/ns/dcat#Dataset"
                                title = list(
                                    list(individual[dcat3.distribution])[0][
                                        dcterms.title
                                    ]
                                )[0]
                                identifier = list(
                                    individual[dcterms.identifier]
                                )[0]
                                dataset = by_dataset_id(
                                    db=db, dataset_id=identifier
                                )
                                size = len(dataset.data)
                                hash = dataset.hash
                                content_type = "application/octet-stream"
                            path = ""
                            if len(list(individual[dcterms.isPartOf])) > 0:
                                path = cls.get_dataset_full_path(
                                    list(individual[dcterms.isPartOf])[0]
                                )
                            item = {
                                "name": title,
                                "last_modified": list(
                                    individual[dcterms.modified]
                                )[0],
                                "dcat_type": type,
                                "relative_path": path,
                                "bytes": size,
                                "hash": hash,
                                "content_type": content_type,
                            }
                            datasets.append(item)
                if len(datasets) > 0:
                    return datasets
            else:
                raise HTTPException(
                    status_code=404,
                    detail="There is no root collection " + "with given name.",
                )
        return datasets

    @classmethod
    def get_dataset_full_path(cls, id, suffix=""):
        """get_dataset_full_path

        Returns the relative path of the dataset from the root collection.

        Parameters:
        - id (str): Identifier of the dataset.

        Returns:
        str: relative path.
        """
        query = f"""
            SELECT ?dataset WHERE {{
                ?dataset rdf:type <{dcat3.Catalog.iri}> .
                ?dataset dcterms:isPartOf ?o
                FILTER (?dataset=<{id}>)
            }}
            """
        result = sparql(query)(dataset=OntologyIndividual)
        datasets = list(result)
        if len(datasets) > 0:
            parent = datasets[0][0]
            suffix = list(parent[dcterms.title])[0] + "/" + suffix
            if len(list(parent[dcterms.isPartOf])) > 0:
                suffix = cls.get_dataset_full_path(
                    list(parent[dcterms.isPartOf])[0], suffix=suffix
                )
                return suffix
        return "./" + suffix

    @classmethod
    def export_catalog(cls, catalog_id) -> str:
        """export_catalog

        Returns all the DCAT objects stored with in a Collection.

        Parameters:
        - catalog_id (str): Identifier of the Catalog.

        Returns:
        dict: DCAT data in json-ld format.
        """
        with allegro_graph_session() as session:
            query = f"""
            SELECT ?dataset WHERE {{
                ?dataset rdf:type <{dcat3.Catalog.iri}> .
                ?dataset dcterms:identifier ?identifier
                FILTER (?identifier = "{catalog_id}")
            }}
            """
            result = sparql(query, session=session)(dataset=OntologyIndividual)
            datasets = list(result)
            if len(datasets) > 0:
                dataset = datasets[0][0]
                individuals = list(find(dataset, rel=owl.topObjectProperty))
                result = export_file(
                    individuals, file=None, format="json-ld", all_triples=True
                )
                return result
            else:
                return None

    @classmethod
    def get_data(cls, collection_name, dataset_name, db):
        """get_data

        Returns binary data from the postgres database.

        Parameters:
        - collection_name (str): Name of the Catalog.
        - dataset_name (str): Name of the Dataset.
        - db : postgres sql database session.

        Returns:
        bytes: Binary content.
        """
        with allegro_graph_session() as session:
            query = f"""
                SELECT ?catalog WHERE {{
                    ?catalog rdf:type <{dcat3.Catalog.iri}> .
                    ?catalog dcterms:title ?title.
                    ?catalog dcterms:type ?type
                    FILTER (?type="http://purl.org/dc/dcmitype/Collection" \
                        && ?title="{collection_name}")
                }}
                """
            result = sparql(query, session=session)(catalog=OntologyIndividual)
            catalogs = list(result)
            if len(catalogs) > 0:
                catalog = catalogs[0][0]
                individuals = list(find(catalog, rel=owl.topObjectProperty))
                dataset_individuals = []
                for individual in individuals:
                    if (
                        not individual.is_a(dcat3.Catalog)
                        and not individual.is_a(dcat3.Distribution)
                        and not individual.is_a(dcat3.DataService)
                    ):
                        dataset = individual
                        dist = list(dataset[dcat3.distribution])[0]
                        if list(dist[dcterms.title])[0] == dataset_name:
                            dataset_individuals.append(dataset)
                            dataset_individuals.append(dist)

                            identifier = list(dataset[dcterms.identifier])[0]

                            dataset = by_dataset_id(
                                db=db, dataset_id=identifier
                            )
                            return dataset.data
        return None

    @classmethod
    def export_dataset(cls, dataset_id) -> str:
        """export_dataset

        Returns all the DCAT objects stored with in a Dataset.

        Parameters:
        - dataset_id (str): Identifier of the Dataset.

        Returns:
        dict: DCAT data in json-ld format.
        """
        with allegro_graph_session() as session:
            result = sparql(
                f"""
            SELECT ?dataset WHERE {{
                ?dataset rdf:type <{dcat3.Dataset.iri}> .
                ?dataset dcterms:identifier ?identifier
                FILTER (?identifier = "{dataset_id}")
            }}
            """,
                session=session,
            )(dataset=OntologyIndividual)
            dataset = next(result)[0]
            individuals = list(find(dataset, rel=owl.topObjectProperty))
            result = export_file(
                individuals, file=None, format="json-ld", all_triples=True
            )
            return result

    @classmethod
    def get_dataset_from_collection(cls, collection_name, dataset_name):
        """get_dataset_from_collection

        Returns all the DCAT objects stored with in a Dataset.

        Parameters:
        - collection_name (str): Name of the Collection.
        - dataset_name (str): Name of the Dataset.

        Returns:
        dict: DCAT data in json-ld format.
        """
        dataset_result = None
        with allegro_graph_session() as session:
            query = f"""
                SELECT ?catalog WHERE {{
                    ?catalog rdf:type <{dcat3.Catalog.iri}> .
                    ?catalog dcterms:title ?title.
                    ?catalog dcterms:type ?type
                    FILTER (?type="http://purl.org/dc/dcmitype/Collection" && \
                    ?title="{collection_name}")
                }}
                """
            result = sparql(query, session=session)(catalog=OntologyIndividual)
            catalogs = list(result)
            if len(catalogs) > 0:
                catalog = catalogs[0][0]
                individuals = list(find(catalog, rel=owl.topObjectProperty))
                dataset_individuals = []
                for individual in individuals:
                    if (
                        not individual.is_a(dcat3.Catalog)
                        and not individual.is_a(dcat3.Distribution)
                        and not individual.is_a(dcat3.DataService)
                    ):
                        dist = list(individual[dcat3.distribution])[0]
                        if list(dist[dcterms.title])[0] == dataset_name:
                            dataset_individuals.append(individual)
                            dataset_individuals.append(dist)
                if len(dataset_individuals) > 0:
                    dataset_result = export_file(
                        dataset_individuals,
                        file=None,
                        format="json-ld",
                        all_triples=True,
                    )
            else:
                raise HTTPException(
                    status_code=404,
                    detail="There is no " + "Root collection with given name.",
                )

        return dataset_result

    @classmethod
    def _is_cuds(cls, raw_text_data: str):
        """_is_cuds

        Detects whether the data content represents graph data or not.

        Parameters:
        - raw_text_data (str): bytes or text data.

        Returns:
        bool, str: True/False, format of the data.
        """
        exception_count = 0
        format = None
        for format_entry, mediaType in supported_formats:
            try:
                with allegro_graph_session(
                    filter="dummy_session"
                ) as temp_session:
                    import_file(
                        StringIO(raw_text_data),
                        format=format_entry,
                        session=temp_session,
                        all_triples=True,
                    )
                # if there is no error, there is a format matched, no more
                # need for further matching, just quit the loop
                format = format_entry
                break
            except Exception:
                # print(format_entry, _)
                exception_count += 1

        # check whether a format is matched by the number of exceptions
        if exception_count < len(supported_formats):
            return True, format
        elif exception_count == len(supported_formats):
            return False, format
        else:
            raise ValueError(
                f"More exceptions are raised than the\
                    maximal number:{len(supported_formats)}"
            )


def allegro_graph_session(filter=None):
    host = CONFIG.agraph_host
    port = "10035"
    user = CONFIG.agraph_super_user
    password = CONFIG.agraph_super_password
    if filter is None:
        return AllegroGraph(
            f"<http://{user}:{password}@{host}:{port}"
            f"/repositories/data-sink>"
        )
    else:
        return AllegroGraph(
            f"<http://{user}:{password}@{host}:{port}"
            f"/repositories/data-sink>",
            identifier=filter,
        )


def data_space_session(filter=None):
    host = CONFIG.agraph_host
    port = "10035"
    user = CONFIG.agraph_super_user
    password = CONFIG.agraph_super_password
    # g = Graph("AllegroGraph", filter)
    g = ConjunctiveGraph("AllegroGraph")
    g.open(f"<http://{user}:{password}@{host}:{port}/repositories/data-sink>")
    return g
