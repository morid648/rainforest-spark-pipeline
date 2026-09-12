# `spark/`

## Purpose
Spark cluster configuration and container setup. Everything needed to build and run the Spark Docker image used by the master, workers, and history server.

## Contents

| File | Purpose |
|---|---|
| `Dockerfile` | Builds the Spark image — installs Python dependencies, JDBC driver, Delta Lake JARs |
| `entrypoint.sh` | Container startup script — routes to master, worker, or history-server mode based on argument |
| `requirements.txt` | Python packages installed into the Spark image |
| `create_buckets.py` | Creates the `rainforest` bucket in MinIO via boto3 if it doesn't already exist |
| `run_etl.py` | Copy of the pipeline entrypoint — mounted into the Spark work directory |
| `conf/spark-defaults.conf` | Spark configuration: S3A endpoint, Delta Lake extensions, Hive support |
| `conf/metrics.properties` | Spark metrics configuration for the history server |

## Python Dependencies (`requirements.txt`)

| Package | Purpose |
|---|---|
| `great_expectations==0.17.11` | Data quality validation |
| `pytest==7.4.0` | Unit and integration testing |
| `faker==19.3.0` | Synthetic data generation |
| `delta-spark==2.3.0` | Delta Lake support for Spark |
| `psycopg2-binary==2.9.7` | PostgreSQL JDBC connectivity |
| `boto3==1.34.104` | MinIO / S3-compatible bucket operations |
| `mypy==0.991` | Static type checking |
| `black==24.3.0` | Code formatting |
| `flake8==6.0.0` | Linting |
| `isort==5.12.0` | Import sorting |

## Notes
- The Spark image is shared across master, workers, and history server — role is determined by the `entrypoint.sh` argument
- `spark-defaults.conf` configures S3A to point to the local MinIO service (`http://minio:9000`) — this is why all storage paths use `s3a://` even though the storage is local
- MinIO credentials (`minio`/`minio123`) are Docker-internal dev defaults only
