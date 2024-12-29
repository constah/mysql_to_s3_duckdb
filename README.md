# mysql_to_s3_duckdb 

This repository contains a Python script that exports a MySQL table into a Parquet file on S3 **using DuckDB**.  
It demonstrates:

1. How to **attach** a MySQL database to DuckDB via the **mysql_scanner** extension.  
2. How to **write** Parquet files directly to S3 via DuckDB’s **httpfs** extension.  
3. How to **store** S3 credentials in DuckDB using **`CREATE SECRET`**.  
4. Basic **logging**, including row counts and timing metrics.

## Requirements

- **Python 3.7+**  
- **pip install**: `duckdb`, `logging` (built in), plus any other dependencies such as `boto3` if you need it for additional tasks (not required for direct S3 exports).  
- **MySQL** or **MariaDB** server credentials.  
- **AWS** access key and secret key for writing to S3.

## Configuration

Create a `settings.py` file or similar configuration module that defines:

```python
# Example settings.py

AWS_ACCESS_KEY_ID = "AKIA...EXAMPLE"
AWS_SECRET_ACCESS_KEY = "wJa...EXAMPLEKEY"
AWS_REGION = "e.g us-east-2"
S3_BUCKET_NAME = "my-bucket"

# MySQL connection details for DuckDB
duckdb_mysql_string = "host=my-mysql-host user=myuser password=mypass database=mydb port=3306"
