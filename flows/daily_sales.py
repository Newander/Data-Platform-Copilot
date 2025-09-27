from datetime import datetime, UTC

from prefect import flow, task


@task
def extract():
    # заглушка для демо
    return {"rows": 123, "date": datetime.now(tz=UTC).isoformat()}


@task
def transform(payload):
    payload["rows_transformed"] = payload["rows"] * 2
    return payload


@task
def load(payload):
    # тут могла бы быть запись в DWH
    return f"Loaded {payload['rows_transformed']} rows on {payload['date']}"


@flow(name="daily_sales")
def daily_sales_flow(days_back: int = 1):
    _ = days_back
    data = extract()
    data2 = transform(data)
    result = load(data2)
    return result


if __name__ == "__main__":
    daily_sales_flow()
