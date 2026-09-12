# Changelog

All notable changes to this project are documented here.

## [1.0.0] - 2026-09-13

### Added
- Full Medallion Architecture pipeline: Bronze (13 tables) → Silver (8 tables) → Gold (4 tables) → Interface (2 views)
- Abstract `TableETL` base class with `ETLDataSet` metadata dataclass for consistent, reusable ETL pattern
- Faker-based synthetic data generator (`datagen/datagen.py`) + PostgreSQL schema DDL
- Great Expectations data quality validation — 3 expectation suites (duplicate checks + revenue threshold)
- PyTest suite — 13 bronze unit tests, 4 silver unit tests, 1 integration test
- Fully containerised local infrastructure via Docker Compose (Spark cluster, MinIO, PostgreSQL)
- `Makefile` with commands for build, setup, run, test, and reset
- Two daily business reports: Order Report and Category Report via Interface layer Spark SQL views
