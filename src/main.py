import logging

from fastapi import FastAPI
from prometheus_client import generate_latest
from prometheus_fastapi_instrumentator import Instrumentator
from starlette.responses import Response

from src.config import settings
from src.lifespan import lifespan_routine
from src.metrics import METRICS
from src.route.chat import chat_router
from src.route.namespace import namespace_router
from src.schema_docs import build_markdown

logging.basicConfig(
    level=settings.logging.level,
    format=settings.logging.format,
    datefmt=settings.logging.datefmt,
    force=True,  # overrides existing logging configuration (useful for repeated runs)
)
app = FastAPI(
    title="Data Pilot FastApi Backend Service",
    debug=True,
    lifespan=lifespan_routine
)


@app.get("/health")
def health_route() -> dict:
    return {"status": "ok"}


@app.get("/description")
def description_route() -> dict:
    return {"message": "Here will be a description of database"}


@app.get("/schema")
def schema_route() -> dict:
    return {"schema_markdown": build_markdown()}


fastapi_metrics = Instrumentator().instrument(app)


@app.get("/metrics")
def metrics() -> Response:
    METRICS.set_external_exporter(lambda: generate_latest(fastapi_metrics.registry).decode("utf-8"))
    payload = METRICS.export_prometheus()
    return Response(content=payload, media_type="text/plain; version=0.0.4; charset=utf-8")


app.include_router(namespace_router)
app.include_router(chat_router)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host=settings.server.host, port=settings.server.port, reload=True)
