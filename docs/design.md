# Design notes

## Storage choice (SQLite) & scalability
I used SQLite in this take-home as a lightweight local “warehouse”: it’s a single file (`output/warehouse.db`), requires no server setup, and still supports a proper RAW staging table via DDL and simple SQL-based QA checks (counts, required fields, dedup via `record_hash_raw`, etc.). This keeps the pipeline reproducible and easy to run locally.

If the dataset were significantly larger or this were production, I would keep the same RAW staging contract and mappings but swap only the storage layer to Parquet/S3 + DuckDB/Spark or a cloud warehouse (e.g., Postgres/BigQuery/Snowflake) for better performance, concurrency, and partitioning. In other words: SQLite is a pragmatic take-home choice with a clear upgrade path that doesn’t change the business logic.
