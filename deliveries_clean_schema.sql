CREATE TABLE IF NOT EXISTS deliveries_clean (
  order_id TEXT PRIMARY KEY,
  customer_name TEXT,
  delivery_date DATE,
  amount NUMERIC,
  status TEXT,
  ingested_at TIMESTAMP
);
