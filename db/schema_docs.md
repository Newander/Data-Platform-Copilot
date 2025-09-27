# Data Warehouse Schema (auto-generated)

## customers

| column | type | pk | not_null | description |
|---|---|---:|---:|---|
| customer_id | BIGINT |  |  |  |
| country | VARCHAR |  |  |  |
| signup_date | VARCHAR |  |  |  |

## events

| column | type | pk | not_null | description |
|---|---|---:|---:|---|
| event_id | BIGINT |  |  | Unique event identifier (surrogate PK-like) |
| user_id | BIGINT |  |  | User identifier |
| event_type | VARCHAR |  |  | Categorical event kind: view/click/purchase/signup/refund |
| amount | DOUBLE |  |  | Monetary amount for purchase/refund; 0 otherwise |
| event_ts | TIMESTAMP |  |  | UTC timestamp when event happened |
| country | VARCHAR |  |  | ISO-like country code |
| device | VARCHAR |  |  | User device group |
| source | VARCHAR |  |  | Acquisition channel |

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
