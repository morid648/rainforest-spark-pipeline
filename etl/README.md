# `etl/`

## Purpose
The core ETL pipeline code. Implements the Medallion Architecture across four layers (Bronze → Silver → Gold → Interface), driven by an OOP base class that enforces a consistent Extract → Transform → Validate → Write → Read pattern for every table.

## Structure

```
etl/
├── utils/
│   ├── base_table.py      ← Abstract TableETL + ETLDataSet dataclass
│   └── database.py        ← JDBC upstream PostgreSQL reader
│
├── layers/
│   ├── bronze/            ← 13 raw ingestion tables
│   ├── silver/            ← 4 fact + 2 dim + 2 bridge tables
│   ├── gold/              ← 2 OBT wide tables + 2 metric tables
│   └── interface/         ← 2 Spark SQL report views
│
├── great_expectations/    ← Data quality validation configuration
│   ├── checkpoints/
│   └── expectations/      ← 3 expectation suites
│
└── test/
    ├── unit_tests/        ← 17 unit tests (bronze + silver)
    └── integration/       ← 1 integration test
```

## Base Class Pattern
Every ETL table class inherits from `TableETL` (defined in `utils/base_table.py`) and implements:
- `extract_upstream()` — pull from PostgreSQL (bronze) or upstream layer tables (silver/gold)
- `transform_upstream()` — business logic and transformations
- `read()` — read back with explicitly defined column selection

The base class provides shared `validate()`, `write()`, and `run()` implementations.

## How to Run
See the root README for `make project` and `make test` instructions.
Pipeline entrypoint is `run_etl.py` at the repo root.
