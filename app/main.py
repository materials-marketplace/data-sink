import asyncio
from typing import List

import uvicorn
from fastapi import Depends, FastAPI, HTTPException
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.security import HTTPBearer

from app import models
from app.database import engine
from app.routers.datasink import data_sink_router
from app.schemas import AppSettings
from packageinfo import DESCRIPTION, NAME, VERSION

models.Base.metadata.create_all(bind=engine)


def run_app():
    asyncio.run(main())


async def main():
    config = uvicorn.Config(app)
    server = uvicorn.Server(config)
    await server.serve()


def get_auth_deps() -> List[Depends]:
    """Get authentication dependencies
    Fetch dependencies for authentication through the
    `DATASINK_AUTH_DEPS` environment variable.
    Returns:
        List of FastAPI dependencies with authentication functions.
    """
    if CONFIG.auth_deps:
        dependencies = [Depends(HTTPBearer(bearerFormat="JWT"))]
    else:
        dependencies = []
        print("No dependencies for authentication assigned.")

    # dependencies.append(Depends(security))
    print("Dependencies... ", dependencies)
    return dependencies


CONFIG = AppSettings()

app = FastAPI(
    dependencies=get_auth_deps(),
    title=NAME,
    version=VERSION,
    description=DESCRIPTION,
)

app.include_router(data_sink_router)


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title=NAME,
        description=DESCRIPTION,
        version=VERSION,
        contact={
            "name": "Kiran Kumaraswamy",
            "url": "https://materials-marketplace.eu/",
            "email": "kiran.kumaraswamy@iwm.fraunhofer.de",
        },
        servers=[{"url": CONFIG.application_url}],
        routes=app.routes,
    )
    openapi_schema["info"]["x-api-version"] = "0.3.0"

    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi


@app.get("/", include_in_schema=False)
async def root():
    new_url = "/docs"
    return RedirectResponse(url=new_url)


@app.get(
    "/heartbeat", operation_id="heartbeat", summary="Check if app is alive"
)
async def heartbeat():
    return "Datasink app up and running"


@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    error_code = exc.status_code
    error_message = exc.detail
    # print("Error :", error_code, " ", error_message)
    response = JSONResponse(
        status_code=error_code, content={"detail": error_message}
    )
    return response
