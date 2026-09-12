# `datagen/`

## Purpose
Synthetic data generation for the upstream PostgreSQL database. Simulates a real e-commerce platform's transactional data using the `Faker` library.

## Contents

| File | Purpose |
|---|---|
| `datagen.py` | Generates synthetic records for all 13 upstream tables using Faker and inserts them into PostgreSQL via psycopg2 |
| `upstream_data.sql` | PostgreSQL DDL — creates the `rainforest` schema and all upstream tables with correct types, constraints, and foreign keys |

## What Gets Generated
Records are inserted into these tables in the `rainforest` schema:

| Table | What it contains |
|---|---|
| `appuser` | Base user accounts (username, email, active flag, timestamps) |
| `buyer` | Buyer profiles linked to appuser |
| `seller` | Seller profiles linked to appuser |
| `brand` | Product brands |
| `manufacturer` | Product manufacturers |
| `product` | Products with price, weight, brand, manufacturer |
| `category` | Product categories |
| `product_category` | Many-to-many: products → categories |
| `seller_product` | Many-to-many: sellers → products |
| `orders` | Orders placed by buyers |
| `order_item` | Line items within orders |
| `ratings` | Product ratings by buyers |
| `clickstream` | View, impression, checkout events |

## How to Run
Data generation runs inside the Spark master container — triggered automatically by `make setup`.

To run manually:
```bash
docker exec spark-master bash -c "python3 /opt/spark/work-dir/datagen/datagen.py"
```

## Notes
- The script connects to PostgreSQL using the Docker service hostname `upstream` — it is designed to run **inside** the Docker network, not from a host machine directly
- Credentials (`sdeuser`/`sdepassword`) are Docker-internal dev defaults — safe for local use only, never for production
