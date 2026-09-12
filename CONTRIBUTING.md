# Contributing

## How to Contribute

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-change`
3. Make your changes
4. Run the test suite: `make test`
5. Run type checks: `make mypy`
6. Commit with a descriptive message
7. Open a pull request

## Project-Specific Guidelines

- Every new ETL table must inherit from `TableETL` in `etl/utils/base_table.py` and implement `extract_upstream()`, `transform_upstream()`, and `read()`
- New tables that need data quality checks should have a matching expectation suite in `etl/great_expectations/expectations/<table_name>.json`
- Storage paths follow the pattern `s3a://rainforest/delta/<layer>/<table_name>`
- All tables use `etl_inserted` as the partition key for time-based partitioning
- Unit tests for new tables go in `etl/test/unit_tests/<layer>/`

## Reporting Issues

Open a GitHub issue with:
- Steps to reproduce
- Expected vs actual behaviour
- Relevant logs from the Spark UI (localhost:9090) or container logs
