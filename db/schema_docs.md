# Data Warehouse Schema (auto-generated)

## customers

| column | type | pk | not_null | description |
|---|---|---:|---:|---|
| customer_id | BIGINT |  |  |  |
| country | VARCHAR |  |  |  |
| signup_date | VARCHAR |  |  |  |

## items

| column | type | pk | not_null | description |
|---|---|---:|---:|---|
| order_id | BIGINT |  |  |  |
| sku | VARCHAR |  |  |  |
| qty | BIGINT |  |  |  |
| unit_price | DOUBLE |  |  |  |

## orders

| column | type | pk | not_null | description |
|---|---|---:|---:|---|
| order_id | BIGINT |  |  |  |
| customer_id | BIGINT |  |  |  |
| order_ts | VARCHAR |  |  |  |
| currency | VARCHAR |  |  |  |
| total_amount | DOUBLE |  |  |  |

## v_revenue_by_country

| column | type | pk | not_null | description |
|---|---|---:|---:|---|
| country | VARCHAR |  |  |  |
| revenue | DOUBLE |  |  |  |
