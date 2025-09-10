tables:
  customers(customer_id PK, country, signup_date)
  orders(order_id PK, customer_id FK→customers, order_ts, total_amount, currency)
  items(order_id FK→orders, sku, qty, unit_price)

business notes:
- Revenue = SUM(orders.total_amount) in EUR (в демо все EUR).
- Top countries by revenue = group by customers.country order by revenue desc.
- 2024 = filter where order_ts >= '2024-01-01' and < '2025-01-01'
