# Project Reference — Rainforest E-Commerce Spark Data Engineering Pipeline

> Quick-reference sheet for resume bullet points, interview prep, and recruiter conversations.

**GitHub:** [github.com/morid648/rainforest-spark-pipeline](https://github.com/morid648/rainforest-spark-pipeline)

---

## One-Line Summary

Built a production-grade, fully containerised data engineering pipeline for a synthetic e-commerce platform using Apache Spark, Medallion Architecture (Bronze → Silver → Gold → Interface), Delta Lake on MinIO, PostgreSQL, Great Expectations for data quality, and PyTest for unit and integration testing — all orchestrated locally via Docker Compose.

---

## Resume Bullet Points

Pick 3–4 based on the role. Use the strongest ones first.

- Designed and implemented an **end-to-end Medallion Architecture** data pipeline (Bronze → Silver → Gold → Interface) using **Apache Spark** and **Delta Lake**, processing synthetic e-commerce data from a PostgreSQL upstream source into daily analytical reports
- Built an **OOP-based, metadata-driven ETL framework** using an abstract base class (`TableETL`) and `ETLDataSet` dataclass — enabling consistent Extract, Transform, Validate, Write, and Read patterns across all 27 pipeline tables with minimal code duplication
- Implemented **Great Expectations data quality validation** integrated into the pipeline's run cycle — enforcing no-duplicate and revenue-threshold checks before any dataset is written to the data lake
- Developed a **PyTest test suite** covering 13 bronze-layer unit tests, 4 silver-layer unit tests, and 1 end-to-end integration test — validating transformation logic before production deployment
- Constructed **One Big Tables (OBTs)** in the Gold layer by joining fact and dimension tables across buyers, sellers, products, categories, and orders — producing analytics-ready datasets for two daily business reports
- Containerised the entire data platform using **Docker Compose** — Spark cluster (1 master + 2 workers + history server), MinIO S3-compatible data lake, and PostgreSQL upstream database — reproducible with a single `make up` command

---

## Technology Stack

| Category | Technologies |
|---|---|
| Distributed Processing | Apache Spark (PySpark) |
| Language | Python |
| Data Lake Storage | MinIO (S3-compatible), Delta Lake |
| Architecture | Medallion (Bronze / Silver / Gold / Interface) |
| Upstream Database | PostgreSQL |
| Data Quality | Great Expectations |
| Testing | PyTest (unit + integration) |
| Containerisation | Docker, Docker Compose |
| Code Quality | mypy, black, flake8, isort |
| Data Generation | Faker |

---

## Architecture at a Glance

```
PostgreSQL (upstream)
      │  JDBC (Spark read)
      ▼
Bronze Layer (13 tables)
  Raw ingestion + etl_inserted timestamp partition
      │
      ▼
Silver Layer (8 tables)
  Fact: fact_orders, fact_order_items
  Dim:  dim_buyer, dim_seller, dim_category, dim_product
  Brg:  bridge_product_category, bridge_seller_product
      │
      ▼
Gold Layer (4 tables)
  OBT:     wide_orders_gold, wide_order_items_gold
  Metrics: daily_order_metrics, daily_category_metrics
      │
      ▼
Interface Layer (2 views)
  daily_order_report, daily_category_report
      │
      ▼
Spark SQL → Business Reports
```

**Storage:** All layers write to MinIO (`s3a://rainforest/delta/<layer>/<table>`) in Delta format, partitioned by `etl_inserted`.

---

## Key Design Decisions — Interview Talking Points

### Why an abstract base class for ETL tables?
Every table in this pipeline follows the same lifecycle: extract from upstream, transform, validate, write, read back. Without a base class, that logic would be duplicated 27 times. `TableETL` defines the contract and provides shared `validate()` and `write()` implementations — each table only needs to implement the parts that are unique to it (`extract_upstream`, `transform_upstream`, `read`). This makes adding a new table straightforward and keeps the codebase consistent.

### Why metadata-driven with ETLDataSet?
Hardcoding storage paths, partition keys, and data formats into individual functions is brittle — change the format and you're hunting through 27 files. `ETLDataSet` encapsulates all of that as attributes on the dataset object itself, so the `write()` and `read()` logic in the base class always knows where data lives and how it's structured without any hardcoding.

### Why Delta Lake instead of plain Parquet?
Delta Lake adds ACID transactions, schema enforcement, and time-travel capabilities on top of Parquet. For a pipeline that runs daily and writes incrementally, being able to recover from a bad write without corrupting the table is important. `mergeSchema=true` in the write step also lets the schema evolve without breaking downstream readers.

### Why Great Expectations inside the pipeline instead of as a separate step?
Running GE validation inside `validate()` in the base class means data quality is checked at write-time, not after. If the `daily_order_metrics` table has an average revenue above 100,000 USD, it raises `InvalidDataException` and the write never happens — consumers never see bad data. A separate validation step could be skipped or bypassed; this can't.

### Why One Big Tables (OBTs) in the Gold layer?
Silver tables are normalized for correctness — a buyer has a row in `dim_buyer`, their orders are in `fact_orders`, and you join to get the full picture. For analytical queries this means many joins every time. OBTs pre-compute those joins once at pipeline runtime, so the Interface layer (and any BI tool on top) queries a single wide table rather than reconstructing joins at query time.

### Why MinIO instead of real S3?
This is a locally reproducible portfolio project — MinIO provides full S3 API compatibility so the code is identical to what would run against real AWS S3 (same boto3 calls, same `s3a://` paths in Spark). Swapping to real S3 requires only updating the endpoint URL and credentials, not changing any pipeline code.

---

## Pipeline Table Reference

### Bronze (13 tables)
| Table | Source | Key transformation |
|---|---|---|
| `orders` | `rainforest.orders` | Add `etl_inserted` timestamp |
| `order_item` | `rainforest.order_item` | Add `etl_inserted` timestamp |
| `buyer` | `rainforest.buyer` | Add `etl_inserted` timestamp |
| `seller` | `rainforest.seller` | Add `etl_inserted` timestamp |
| `product` | `rainforest.product` | Add `etl_inserted` timestamp |
| `category` | `rainforest.category` | Add `etl_inserted` timestamp |
| `appuser` | `rainforest.appuser` | Add `etl_inserted` timestamp |
| `brand` | `rainforest.brand` | Add `etl_inserted` timestamp |
| `manufacturer` | `rainforest.manufacturer` | Add `etl_inserted` timestamp |
| `product_category` | `rainforest.product_category` | Add `etl_inserted` timestamp |
| `seller_product` | `rainforest.seller_product` | Add `etl_inserted` timestamp |
| `ratings` | `rainforest.ratings` | Add `etl_inserted` timestamp |
| `clickstream` | `rainforest.clickstream` | Add `etl_inserted` timestamp |

### Silver (8 tables)
| Table | Upstream | Key transformation |
|---|---|---|
| `fact_orders` | `orders` bronze | Add `total_price_usd`, `total_price_inr` (currency conversion) |
| `fact_order_items` | `order_item` bronze | Item-level data with order linkage |
| `dim_buyer` | `buyer` + `appuser` bronze | Join buyer profile with user account |
| `dim_seller` | `seller` + `appuser` bronze | Join seller profile with user account |
| `dim_category` | `category` bronze | Category attributes |
| `dim_product` | `product` bronze | Product attributes with metrics |
| `bridge_product_category` | `product_category` bronze | Product → category mapping |
| `bridge_seller_product` | `seller_product` bronze | Seller → product mapping |

### Gold (4 tables)
| Table | Upstream | Purpose |
|---|---|---|
| `wide_orders_gold` | Silver fact + dims | OBT: orders enriched with buyer, seller, product |
| `wide_order_items_gold` | Silver fact + dims | OBT: items enriched with all dimensions |
| `daily_order_metrics` | `wide_orders_gold` | Total + avg revenue by day (GE validated) |
| `daily_category_metrics` | `wide_order_items_gold` | Avg + median revenue by day and category (GE validated) |

---

## Testing Reference

| Test file | What it validates |
|---|---|
| `test_orders_bronze.py` | `etl_inserted` column added to orders |
| `test_buyer_bronze.py` | Bronze buyer extraction and timestamp |
| `test_seller_bronze.py` | Bronze seller extraction and timestamp |
| *(+ 10 more bronze tests)* | Same pattern for each upstream table |
| `test_dim_buyer_silver.py` | Buyer + appuser join produces correct schema |
| `test_dim_product_silver.py` | Product attributes pass through correctly |
| `test_dim_seller_silver.py` | Seller + appuser join produces correct schema |
| `test_fact_order_items_silver.py` | Currency conversions and column selection |
| `test_integration_fact_order_items.py` | End-to-end: Bronze → Silver pipeline for order items |

---

## Suggested Interview Answers

**"Walk me through this project."**
> "It's a fully local data engineering pipeline for a synthetic e-commerce company called Rainforest. The upstream source is a PostgreSQL database populated with Faker-generated data — buyers, sellers, products, orders, and clickstream events. Spark reads from that via JDBC and processes it through a Medallion Architecture: Bronze ingests raw data and adds a timestamp partition, Silver models it into fact and dimension tables with business transformations like currency conversion, Gold joins everything into wide tables and computes daily metrics, and the Interface layer creates Spark SQL views that deliver the final daily reports. The whole thing runs in Docker — a Spark cluster, MinIO as a local S3-compatible data lake, and PostgreSQL, all containerised together."

**"How did you ensure code reusability across 27 tables?"**
> "I built an abstract base class called `TableETL` that defines the contract every ETL table must follow — extract, transform, validate, write, read. Each table inherits from it and only implements the parts unique to that table. The shared `validate()` and `write()` methods in the base class are reused across all 27 tables. I also wrapped every dataset in a dataclass called `ETLDataSet` that carries the storage path, format, partition keys, and schema — so the base class's write and read methods always know what they're working with, without any hardcoding."

**"How do you handle data quality?"**
> "Great Expectations is integrated directly into the base class's `validate()` method, which runs automatically during every `run()` call before data is written. If a dataset fails validation — say, duplicate order IDs, or average revenue above 100,000 USD — the pipeline raises an `InvalidDataException` and the write never happens. Downstream consumers never see bad data because it's stopped before it's written, not caught after."

**"What's the dependency management approach — how does the pipeline know what to run first?"**
> "Each ETL class has an `upstream_tables` attribute — a list of the ETL classes it depends on. When `run()` is called on a Gold table, it calls `extract_upstream()`, which iterates over `upstream_tables`, instantiates each one, calls their `run()` if needed, and reads the result. This recursive pattern means you only need to trigger the final Gold table and the whole dependency graph resolves automatically, bottom-up."

---

## Known Limitations

- **No deployed dashboard or live endpoint** — runs entirely locally via Docker Compose; there is nothing to link to publicly
- No incremental loading — each run overwrites the existing partition
- No orchestration framework (Airflow, Prefect) — dependencies are resolved recursively in code
- Data quality rules are limited to two checks; no null validation or referential integrity checks
- MinIO credentials and PostgreSQL passwords are Docker dev defaults — must be changed for any real deployment
