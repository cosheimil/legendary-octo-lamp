from prefect import flow, task
from dask.distributed import Client
import dask.dataframe as dd
import pandas as pd
from minio import Minio
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Date,
    String,
    Float,
    Integer,
    Boolean,
)
from datetime import datetime
import os
import sys

sys.path.insert(0, "/app")

from config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    DB_URL,
    AMAZON_SALES_FILE,
    INTERNATIONAL_SALES_FILE,
    ENABLE_MINIO,
    TEMP_PATH,
)
from logger import get_logger

logger = get_logger(__name__)


@task(retries=3, retry_delay_seconds=60)
def load_config():
    """
    Load and validate application configuration.

    Returns:
        dict: Configuration dictionary with file paths and bucket names.
              Keys:
              - amazon_sales (str): Path to Amazon sales CSV file
              - international_sales (str): Path to international sales CSV file
              - output_bucket (str): MinIO bucket for processed data

    Raises:
        FileNotFoundError: If any required data files are not found.

    Example:
        >>> config = load_config()
        >>> print(config['amazon_sales'])
        /app/data/Amazon Sale Report.csv
    """
    logger.info("Loading configuration...")

    config = {
        "amazon_sales": AMAZON_SALES_FILE,
        "international_sales": INTERNATIONAL_SALES_FILE,
        "output_bucket": "processed-data",
    }

    for key, path in config.items():
        if key != "output_bucket" and not os.path.exists(path):
            logger.error(f"File not found: {path}")
            raise FileNotFoundError(f"Data file not found: {path}")

    logger.info(f"Configuration loaded successfully with {len(config)} settings")
    return config


@task(retries=2)
def connect_minio():
    """
    Establish connection to MinIO S3-compatible storage.

    Returns:
        Minio: MinIO client instance.

    Raises:
        Exception: If connection to MinIO fails.

    Example:
        >>> minio_client = connect_minio()
        >>> print(minio_client.bucket_exists('processed-data'))
        True
    """
    logger.info(f"Connecting to MinIO at {MINIO_ENDPOINT}...")
    try:
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )
        logger.info("✓ MinIO connection established")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to MinIO: {e}")
        raise


@task(retries=2)
def extract_amazon_data(file_path: str):
    """
    Extract Amazon sales data from CSV file using Dask.

    This function reads Amazon sales data with proper type inference
    to optimize memory usage and computation speed.

    Args:
        file_path (str): Path to Amazon Sales CSV file.

    Returns:
        dd.DataFrame: Dask DataFrame with Amazon sales data.
                     Columns: SKU, ASIN, Order ID, Amount, Qty, etc.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        Exception: If CSV parsing fails.

    Example:
        >>> df = extract_amazon_data('/app/data/Amazon Sale Report.csv')
        >>> print(len(df))
        982
    """
    logger.info(f"Extracting Amazon sales data from {file_path}...")
    try:
        df = dd.read_csv(
            file_path,
            dtype={
                "SKU": "str",
                "ASIN": "str",
                "Order ID": "str",
                "Amount": "float64",
                "Qty": "float64",
            },
        )
        logger.info(f"✓ Successfully extracted Amazon data ({len(df)} rows)")
        return df
    except Exception as e:
        logger.error(f"Failed to extract Amazon sales data: {e}")
        raise


@task(retries=2)
def extract_international_data(file_path: str):
    """
    Extract international sales data from CSV file using Dask.

    This function handles flexible parsing of numeric fields that may
    contain mixed data types or formatting issues.

    Args:
        file_path (str): Path to International Sales CSV file.

    Returns:
        dd.DataFrame: Dask DataFrame with international sales data.
                     Columns: DATE, CUSTOMER, SKU, PCS, RATE, GROSS AMT, etc.

    Raises:
        FileNotFoundError: If the specified file does not exist.

    Example:
        >>> df = extract_international_data('/app/data/International sale Report.csv')
        >>> df.columns.tolist()
        ['DATE', 'CUSTOMER', 'SKU', 'PCS', 'RATE', 'GROSS AMT']
    """
    logger.info(f"Extracting international sales data from {file_path}...")
    try:
        df = dd.read_csv(
            file_path,
            dtype={
                "SKU": "str",
                "Style": "str",
                "Size": "str",
                "DATE": "str",
                "Months": "str",
                "CUSTOMER": "str",
                "PCS": "str",
                "RATE": "str",
                "GROSS AMT": "str",
            },
            on_bad_lines="skip",
        )

        df["PCS"] = dd.to_numeric(df["PCS"], errors="coerce")
        df["RATE"] = dd.to_numeric(df["RATE"], errors="coerce")
        df["GROSS AMT"] = dd.to_numeric(df["GROSS AMT"], errors="coerce")

        df = df.dropna(subset=["PCS", "RATE", "GROSS AMT"])

        logger.info(f"✓ Successfully extracted international data ({len(df)} rows)")
        return df

    except Exception as e:
        logger.error(f"Failed to extract international sales data: {e}")
        return dd.from_pandas(pd.DataFrame(), npartitions=1)


@task
def transform_amazon_data(df: "dd.DataFrame") -> "dd.DataFrame":
    """
    Transform and enrich Amazon sales data.

    Performs the following transformations:
    - Parse dates from MM-DD-YY format
    - Filter only successful deliveries
    - Calculate derived metrics (total revenue)
    - Fill missing values with defaults

    Args:
        df (dd.DataFrame): Raw Amazon sales Dask DataFrame.

    Returns:
        dd.DataFrame: Transformed Amazon sales DataFrame with enriched columns.
                     Additional columns:
                     - Date (datetime): Parsed date
                     - total_revenue (float): Amount value
                     - quantity (float): Order quantity

    Example:
        >>> df_raw = extract_amazon_data('/path/to/file.csv')
        >>> df_transformed = transform_amazon_data(df_raw)
        >>> df_transformed[['Date', 'total_revenue']].head()
    """
    logger.info("Transforming Amazon sales data...")
    try:
        df["Date"] = dd.to_datetime(df["Date"], format="%m-%d-%y", errors="coerce")

        initial_count = len(df)
        df = df[df["Status"].isin(["Shipped", "Shipped - Delivered to Buyer"])]
        logger.debug(
            f"Filtered {initial_count} → {len(df)} records (successful deliveries only)"
        )

        df["total_revenue"] = df["Amount"].fillna(0)
        df["quantity"] = df["Qty"].fillna(0)

        df["Category"] = df["Category"].fillna("Unknown")
        df["Size"] = df["Size"].fillna("Unknown")

        logger.info(f"✓ Amazon data transformation complete ({len(df)} records)")
        return df

    except Exception as e:
        logger.error(f"Failed to transform Amazon sales data: {e}")
        raise


@task
def transform_international_data(df: "dd.DataFrame") -> "dd.DataFrame":
    """
    Transform and enrich international sales data.

    Performs the following transformations:
    - Parse dates from MM-DD-YY format
    - Calculate derived metrics
    - Handle missing values
    - Add data source identifier

    Args:
        df (dd.DataFrame): Raw international sales Dask DataFrame.

    Returns:
        dd.DataFrame: Transformed international sales DataFrame.
                     Additional columns:
                     - total_revenue (float): GROSS AMT value
                     - quantity (float): PCS value
                     - source (str): Always 'International'

    Example:
        >>> df_raw = extract_international_data('/path/to/file.csv')
        >>> df_transformed = transform_international_data(df_raw)
    """
    logger.info("Transforming international sales data...")
    try:
        df["DATE"] = dd.to_datetime(df["DATE"], format="%m-%d-%y", errors="coerce")

        initial_count = len(df)
        df = df.dropna(subset=["DATE"])
        logger.debug(f"Filtered {initial_count} → {len(df)} records (valid dates only)")

        df["total_revenue"] = df["GROSS AMT"].fillna(0)
        df["quantity"] = df["PCS"].fillna(0)

        df["source"] = "International"

        logger.info(f"✓ International data transformation complete ({len(df)} records)")
        return df

    except Exception as e:
        logger.error(f"Failed to transform international sales data: {e}")
        raise


@task
def load_to_minio(df: "dd.DataFrame", bucket_name: str, file_prefix: str):
    """
    Load processed DataFrame to MinIO S3-compatible storage.

    This function:
    - Creates bucket if it doesn't exist
    - Converts Dask DataFrame to Pandas
    - Saves to CSV format
    - Uploads to MinIO
    - Cleans up temporary files

    Args:
        df (dd.DataFrame): Dask DataFrame to save.
        bucket_name (str): Target MinIO bucket name.
        file_prefix (str): Prefix for the output file (e.g., 'amazon_sales').
                          Final filename: {prefix}-{YYYYMMDD}.csv

    Returns:
        bool: True if successful, False otherwise.

    Raises:
        Exception: If MinIO operations fail.

    Example:
        >>> df = dd.read_csv('data.csv')
        >>> load_to_minio(df, 'processed-data', 'daily_sales')
        ✓ Данные сохранены в MinIO: s3://processed-data/daily_sales-20231225.csv
    """
    if not ENABLE_MINIO:
        logger.info("MinIO storage is disabled, skipping upload")
        return False

    logger.info(f"Uploading data to MinIO bucket '{bucket_name}'...")
    try:
        minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )

        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            logger.info(f"Created MinIO bucket: {bucket_name}")

        filename = f"{file_prefix}-{datetime.now().strftime('%Y%m%d')}.csv"
        local_path = os.path.join(TEMP_PATH, filename)

        logger.debug("Converting Dask DataFrame to Pandas...")
        df_pandas = df.compute()
        df_pandas.to_csv(local_path, index=False)
        logger.debug(f"Saved {len(df_pandas)} records to {local_path}")

        minio_client.fput_object(bucket_name, filename, local_path)
        logger.info(f"✓ Uploaded to MinIO: s3://{bucket_name}/{filename}")

        if os.path.exists(local_path):
            os.remove(local_path)
            logger.debug(f"Cleaned up temporary file: {local_path}")

        return True

    except Exception as e:
        logger.error(f"Failed to upload to MinIO: {e}")
        return False


@task
def create_tables():
    """
    Create PostgreSQL tables for storing processed sales data.

    Creates three main tables:
    1. amazon_daily_sales: Aggregated daily sales by category
    2. amazon_sales_detail: Detailed transaction-level data
    3. international_sales: International channel sales

    Tables are created with appropriate data types and primary keys.
    If tables exist, they are not recreated (idempotent).

    Returns:
        bool: True if tables created successfully.

    Raises:
        SQLAlchemy.exc.DatabaseError: If database operations fail.

    Example:
        >>> create_tables()
        ✓ Tables created successfully
    """
    logger.info("Creating PostgreSQL tables...")
    try:
        engine = create_engine(DB_URL)
        metadata = MetaData()

        Table(
            "amazon_daily_sales",
            metadata,
            Column("date", Date, primary_key=True),
            Column("category", String(100), primary_key=True),
            Column("total_revenue", Float),
            Column("total_quantity", Integer),
            Column("order_count", Integer),
        )
        logger.debug("Defined table: amazon_daily_sales")

        Table(
            "international_sales",
            metadata,
            Column("date", Date),
            Column("customer", String(200)),
            Column("sku", String(100)),
            Column("quantity", Integer),
            Column("revenue", Float),
        )
        logger.debug("Defined table: international_sales")

        Table(
            "amazon_sales_detail",
            metadata,
            Column("order_id", String(100), primary_key=True),
            Column("date", Date),
            Column("sku", String(100)),
            Column("category", String(100)),
            Column("size", String(50)),
            Column("quantity", Integer),
            Column("amount", Float),
            Column("status", String(100)),
            Column("fulfilment", String(100)),
            Column("b2b", Boolean),
        )
        logger.debug("Defined table: amazon_sales_detail")

        metadata.create_all(engine)
        logger.info("✓ All PostgreSQL tables created successfully")
        return True

    except Exception as e:
        logger.error(f"Failed to create tables: {e}")
        raise


@task
def save_amazon_to_postgres(df: "dd.DataFrame"):
    """
    Save aggregated and detailed Amazon sales data to PostgreSQL.

    This function performs two operations:
    1. Aggregates data by date and category for daily_sales table
    2. Stores detailed transaction-level data

    Args:
        df (dd.DataFrame): Transformed Amazon sales Dask DataFrame.

    Returns:
        tuple: (daily_records_count, detail_records_count)

    Raises:
        SQLAlchemy.exc.DatabaseError: If database write operations fail.

    Example:
        >>> save_amazon_to_postgres(df_transformed)
        ✓ Saved 45 daily records and 982 detail records
    """
    logger.info("Saving Amazon sales data to PostgreSQL...")
    try:
        engine = create_engine(DB_URL)

        logger.debug("Aggregating data by date and category...")
        daily_agg = (
            df.groupby(["Date", "Category"])
            .agg({"total_revenue": "sum", "quantity": "sum", "Order ID": "count"})
            .compute()
        )

        daily_agg = daily_agg.reset_index()
        daily_agg.columns = [
            "date",
            "category",
            "total_revenue",
            "total_quantity",
            "order_count",
        ]

        daily_agg.to_sql("amazon_daily_sales", engine, if_exists="replace", index=False)
        logger.info(f"✓ Saved {len(daily_agg)} daily aggregated records")

        logger.debug("Preparing detailed sales records...")
        detail_df = df[
            [
                "Order ID",
                "Date",
                "SKU",
                "Category",
                "Size",
                "Qty",
                "Amount",
                "Status",
                "Fulfilment",
                "B2B",
            ]
        ].compute()
        detail_df.columns = [
            "order_id",
            "date",
            "sku",
            "category",
            "size",
            "quantity",
            "amount",
            "status",
            "fulfilment",
            "b2b",
        ]

        detail_df.to_sql(
            "amazon_sales_detail", engine, if_exists="replace", index=False
        )
        logger.info(f"✓ Saved {len(detail_df)} detailed transaction records")

        return (len(daily_agg), len(detail_df))

    except Exception as e:
        logger.error(f"Failed to save Amazon sales data: {e}")
        raise


@task
def save_international_to_postgres(df: "dd.DataFrame"):
    """
    Save international sales data to PostgreSQL.

    This function:
    - Validates data completeness
    - Extracts required columns
    - Removes duplicates
    - Stores in international_sales table

    Args:
        df (dd.DataFrame): Transformed international sales Dask DataFrame.

    Returns:
        int: Number of records saved.

    Raises:
        Exception: If data processing or database operations fail.

    Example:
        >>> save_international_to_postgres(df_transformed)
        ✓ Saved 156 international sales records
    """
    logger.info("Saving international sales data to PostgreSQL...")
    try:
        engine = create_engine(DB_URL)

        if len(df) == 0:
            logger.warning("International sales dataframe is empty, skipping save")
            return 0

        available_cols = set(df.columns)
        needed_cols = ["DATE", "CUSTOMER", "SKU", "PCS", "GROSS AMT"]
        cols_to_use = [col for col in needed_cols if col in available_cols]

        if not cols_to_use:
            logger.error("Required columns not found in international sales data")
            return 0


        logger.debug(f"Extracting columns: {cols_to_use}")
        int_df = df[cols_to_use].compute()


        column_mapping = {
            "DATE": "date",
            "CUSTOMER": "customer",
            "SKU": "sku",
            "PCS": "quantity",
            "GROSS AMT": "revenue",
        }
        int_df.rename(
            columns={k: v for k, v in column_mapping.items() if k in int_df.columns},
            inplace=True,
        )

        initial_count = len(int_df)
        int_df = int_df.drop_duplicates(subset=["date", "customer", "sku"])
        duplicates_removed = initial_count - len(int_df)
        if duplicates_removed > 0:
            logger.debug(f"Removed {duplicates_removed} duplicate records")

        int_df.to_sql("international_sales", engine, if_exists="replace", index=False)
        logger.info(f"✓ Saved {len(int_df)} international sales records")

        return len(int_df)

    except Exception as e:
        logger.error(f"Failed to save international sales data: {e}")
        raise


@flow(
    name="E-commerce Fashion ETL Pipeline",
    description="Extract, transform, and load e-commerce sales data from multiple sources",
)
def main_flow():
    """
    Main ETL orchestration flow for fashion e-commerce data processing.

    This flow orchestrates the complete data pipeline:
    1. Loads configuration from environment
    2. Creates/verifies database tables
    3. Processes Amazon sales data (extraction → transformation → load)
    4. Processes international sales data (extraction → transformation → load)
    5. Uploads processed data to MinIO for archival

    Flow uses Prefect for task orchestration and Dask for parallel processing.

    Returns:
        dict: Summary of pipeline execution with record counts.

    Example:
        >>> result = main_flow()
        >>> print(result['amazon_records_saved'])
        982
    """
    logger.info("🚀 Starting E-commerce ETL Pipeline...")
    logger.info(f"Configuration: {DB_URL}, MinIO: {MINIO_ENDPOINT}")

    try:
        config = load_config()
        logger.info("✓ Configuration loaded")

        create_tables()
        logger.info("✓ Database tables verified")

        logger.info("Initializing Dask client with 2 workers...")
        with Client(
            n_workers=2, threads_per_worker=2, memory_limit="2GB"
        ) as dask_client:
            logger.info(f"✓ Dask cluster started: {dask_client.dashboard_link}")

            logger.info("=" * 70)
            logger.info("📊 PROCESSING AMAZON SALES DATA")
            logger.info("=" * 70)

            amazon_raw = extract_amazon_data(config["amazon_sales"])
            amazon_processed = transform_amazon_data(amazon_raw)
            amazon_saved = save_amazon_to_postgres(amazon_processed)
            load_to_minio(amazon_processed, config["output_bucket"], "amazon_sales")

            logger.info(
                f"✓ Amazon pipeline complete: {amazon_saved[0]} aggregated, {amazon_saved[1]} detailed records"
            )

            logger.info("=" * 70)
            logger.info("🌍 PROCESSING INTERNATIONAL SALES DATA")
            logger.info("=" * 70)

            intl_raw = extract_international_data(config["international_sales"])
            intl_processed = transform_international_data(intl_raw)
            intl_saved = save_international_to_postgres(intl_processed)
            load_to_minio(
                intl_processed, config["output_bucket"], "international_sales"
            )

            logger.info(
                f"✓ International pipeline complete: {intl_saved} records saved"
            )

        logger.info("=" * 70)
        logger.info("✅ ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 70)

        summary = {
            "status": "SUCCESS",
            "amazon_daily_records": amazon_saved[0],
            "amazon_detail_records": amazon_saved[1],
            "international_records": intl_saved,
            "total_records": amazon_saved[1] + intl_saved,
            "timestamp": datetime.now().isoformat(),
        }

        logger.info(f"Pipeline Summary: {summary}")
        return summary

    except Exception as e:
        logger.error(f"❌ ETL Pipeline failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    logger.info("Starting ETL pipeline execution...")
    main_flow()
