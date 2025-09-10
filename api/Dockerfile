FROM python:3.13-bookworm

ENV ROOT_DIR=/app

COPY app/ /app/

RUN pip install -r /app/requirements.txt

ENTRYPOINT exec "/app/entrypoint.sh"