import time
import logging
import duckdb
import settings

# Configure the default logging settings for this script.
# - 'format' defines the structure of each log line, here including:
#   timestamp (asctime), log level (levelname), logger name (name), and the message.
# - 'level=logging.INFO' means only INFO-level (and above) messages are logged.
logging.basicConfig(
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def export_table_to_s3():
    """
    This function uses DuckDB to:
      1) Connect to a local DuckDB database file (or create it if it doesn't exist).
      2) Install & load the httpfs extension for S3 read/write.
      3) Create or replace an S3 secret to store AWS credentials info.
      4) Install & load the MySQL extension to access a remote MySQL database.
      5) Attach the MySQL database via mysql_scanner.
      6) COPY the 'CUSTOMER' table directly to a Parquet file on S3.
    Logs the start/end time, duration, and row count in the table.
    """
    logger.info("Starting DuckDB → S3 export process.")

    # 1) Connect to (or create) a local DuckDB database file.
    conn = duckdb.connect("my_duckdb_database.duckdb")
    logger.info("Connected to DuckDB database file.")

    # 2) Install & load the httpfs extension (for S3 read/write).
    conn.execute("INSTALL httpfs")
    conn.execute("LOAD httpfs")
    logger.info("httpfs extension installed and loaded.")

    # 3) Create (or overwrite) an S3 secret with your credentials & region.
    #    Replace region if needed, e.g., 'us-east-1'.
    secret_sql = f"""
        CREATE OR REPLACE SECRET (
            TYPE S3,
            KEY_ID '{settings.AWS_ACCESS_KEY_ID}',
            SECRET '{settings.AWS_SECRET_ACCESS_KEY}',
            REGION 'us-east-2'
        )
    """
    conn.execute(secret_sql)
    logger.info("S3 secret created in DuckDB.")

    # 4) Install & load the MySQL extension.
    conn.execute("INSTALL mysql")
    conn.execute("LOAD mysql")
    logger.info("MySQL extension installed and loaded.")

    # 5) Attach to the remote MySQL database using mysql_scanner
    conn.execute(f"ATTACH '{settings.duckdb_mysql_string}' AS mysql_db (TYPE mysql_scanner, READ_ONLY)")
    conn.execute("USE mysql_db")
    logger.info("Attached remote MySQL database and switched context to 'mysql_db'.")

    # Count rows in CUSTOMER before exporting (optional, for logging).
    row_count_result = conn.execute("SELECT COUNT(*) FROM CUSTOMER").fetchone()
    row_count = row_count_result[0] if row_count_result else 0
    logger.info(f"Row count in CUSTOMER table: {row_count}")

    # 6) Write directly to S3 using a s3:// path.
    #    Replace "my-bucket" and "prefix" with your actual bucket/path.
    s3_output_path = f"s3://{settings.S3_BUCKET_NAME}/my_parquet_uploads/CUSTOMER.parquet"
    logger.info(f"Preparing to export CUSTOMER table to: {s3_output_path}")

    copy_sql = f"""
        COPY (
            SELECT * 
            FROM CUSTOMER
        )
        TO '{s3_output_path}'
        (FORMAT 'parquet', COMPRESSION 'snappy');
    """

    start_time = time.time()
    conn.execute(copy_sql)
    end_time = time.time()
    duration = end_time - start_time

    logger.info(f"Export to S3 complete: {s3_output_path}")
    logger.info(f"Export duration: {duration:.2f} seconds.")

    conn.close()
    logger.info("DuckDB connection closed.")

if __name__ == "__main__":
    export_table_to_s3()
