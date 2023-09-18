# Datasink app

## About
The Datasink app offers users a persistent storage platform to securely store their data. It categorizes the uploaded data into two types: graph data and binary data, and stores them in separate databases. The graph data is stored as triples in the datastore, allowing users to perform SPARQL queries and extract relevant information from the graph data. Datasink supports standard Object storage endpoints for convenient data storage and retrieval. Additionally, it provides DCAT metadata representation in the form of Catalogs and Datasets for the stored data.

## Authors

**Kiran Kumaraswamy (Fraunhofer IWM)** - [@kirankumaraswamy](https://github.com/Kirankumaraswamy)

## Installation
Create a .env file inside app directory and initialize the following env varaibles. Replace values with in <> brackets with your configuration values.
```
# Postgres db configuration
POSTGRES_USER=<postgres_user>
POSTGRES_PASSWORD=<postgres_password>
POSTGRES_HOST=<postgres_host>
POSTGRES_DB=<postgres_db>

#Allegrograph configuration
AGRAPH_SUPER_USER=<agraph_user>
AGRAPH_SUPER_PASSWORD=<agraph_password>
AGRAPH_HOST=<agraph_host>

#Access token configuration for two PyPi repositories.
#https://gitlab.cc-asp.fraunhofer.de/mat-info/rdflib-agraph
#https://gitlab.cc-asp.fraunhofer.de/simphony/wrappers/allegrograph-wrapper
RDFLIB_PACKAGE_TOKEN=<rdflib_token>
AGRAPH_PACKAGE_TOKEN=<agraph_token>

# The access URL of your datasink server.
# We need this to provide a direct link with download URL functionality
APPLICATION_URL=<access_url>

# Whether to provide end-point protection or not
AUTH_DEPS=True

# Marketplace Keycloak details for shield-api endpoint protection
# reference: https://github.com/materials-marketplace/shieldapi
KEYCLOAK_HOST=<keycloak_host>
KEYCLOAK_CLIENT_ID=<keycloak_client_id>
KEYCLOAK_REALM_NAME=<keycloak_real_name>
KEYCLOAK_CLIENT_SECRET=<keycloak_client_secret

```

Build the container (only first time or after repository update)
```sh
docker compose build
```

Start the container
```sh
docker compose up
```

If everything goes fine the datasink app will be running in port 8080 and for example you can access the application using http://localhost:8080 .
