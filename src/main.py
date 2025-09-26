import logging

from fastapi import FastAPI
from prometheus_client import generate_latest
from prometheus_fastapi_instrumentator import Instrumentator
from starlette.responses import Response

from src.metrics import METRICS
from src.routes import common_router
from src.settings import LOG_LEVEL, LOG_FORMAT, DATE_FORMAT, HOST, PORT
from src.chain import nl_to_sql
from src.constants import ROW_LIMIT, LOG_LEVEL, LOG_FORMAT, DATE_FORMAT, SERVER_HOST, SERVER_PORT
from src.sql_runner import extract_sql_from_markdown, run, IncorrectQuestionError

logging.basicConfig(
    level=LOG_LEVEL,
    format=LOG_FORMAT,
    datefmt=DATE_FORMAT,
    force=True,  # overrides existing logging configuration (useful for repeated runs)
)
app = FastAPI(
    title="Data Pilot FastApi Backend Service",
    debug=True
)

app.include_router(common_router)


@app.get("/health")
def health_route() -> dict:
    return {"status": "ok"}


@app.get("/description")
def description_route() -> dict:
    return {"message": "Here will be a description of database"}


@app.get("/schema")
def schema_route() -> dict:
    return {"message": "Here will be a schema of database"}


fastapi_metrics = Instrumentator().instrument(app)


@app.get("/metrics")
def metrics() -> Response:
    METRICS.set_external_exporter(lambda: generate_latest(fastapi_metrics.registry).decode("utf-8"))
    payload = METRICS.export_prometheus()
    return Response(content=payload, media_type="text/plain; version=0.0.4; charset=utf-8")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host=SERVER_HOST, port=SERVER_PORT, reload=True)
