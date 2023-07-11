FROM python:3.10 as base

# Install system dependencies
RUN apt-get update && apt-get install -y libcurl4-openssl-dev

ARG RDFLIB_PACKAGE_TOKEN
ARG AGRAPH_PACKAGE_TOKEN
RUN pip install rdflib-agraph --index-url https://__token__:${RDFLIB_PACKAGE_TOKEN}@gitlab.cc-asp.fraunhofer.de/api/v4/projects/30574/packages/pypi/simple
RUN pip install simphony-osp-agraph --index-url https://__token__:${AGRAPH_PACKAGE_TOKEN}@gitlab.cc-asp.fraunhofer.de/api/v4/projects/16754/packages/pypi/simple

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

ADD ontologies ./ontologies

RUN cd ./ontologies && pico install ./marketplace.yml
RUN python3 -m simphony_osp.tools.pico install emmo
RUN cd ./ontologies &&  python3 -m simphony_osp.tools.pico install dcat3.yml

COPY requirements.txt packageinfo.py setup.py setup.cfg ./
COPY app ./app
RUN chmod -R 0777 .

EXPOSE 8080

from base as dev
WORKDIR /usr/src/app
RUN pip install .

ENTRYPOINT ["uvicorn"]

CMD ["app.main:app", "--host",  "0.0.0.0", "--port", "8080", "--log-level", "debug"]

from base as prod

ENTRYPOINT ["uvicorn"]

CMD ["app.main:app", "--host",  "0.0.0.0", "--port", "8080", "--log-level", "info"]
