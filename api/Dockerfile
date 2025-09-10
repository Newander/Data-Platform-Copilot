FROM python:3.13-bookworm

ENV ROOT_DIR=/app

COPY app/ $ROOT_DIR
COPY db /db
COPY data /data
WORKDIR $ROOT_DIR

RUN pip install -r $ROOT_DIR/requirements.txt

ENTRYPOINT exec "$ROOT_DIR/entrypoint.sh"