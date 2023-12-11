from dagster import (
    asset,
    get_dagster_logger,
    DailyPartitionsDefinition,
    MetadataValue,
    Output,
)
from datetime import datetime, timedelta
import polars as pl
import pandas

from .partition_defs import daily_partitions
from ..constants.constants import DATE_FORMAT


@asset(
    compute_kind="DuckDB"
)
def raw_transactions() -> pl.DataFrame:
    """
    All transactions from the Kaggle dataset.
    """

    dtypes = {
        "InvoiceNo": str,
        "StockCode": str,
        "Description": str,
        "Quantity": pl.Int32,
        "InvoiceDate": pl.Datetime,
        "UnitPrice": pl.Float32,
        "CustomerID": pl.Int32,
        "Country": str
    }

    transactions = pl.read_csv("data/transactions.csv", dtypes=dtypes)

    yield Output(
        value=transactions,
        metadata={
            "length": MetadataValue.int(len(transactions)),
            "preview": MetadataValue.md(transactions.head().to_pandas().to_markdown()),
        }
    )

@asset(
    compute_kind="DuckDB",
    partitions_def=daily_partitions,
    metadata={
        "partition_expr": "invoice_date"
    }
)
def transactions(context, raw_transactions: pl.DataFrame) -> pl.DataFrame:
    start_date = datetime.strptime(context.asset_partition_key_for_output(), DATE_FORMAT)
    end_date = start_date + timedelta(days=1)

    transactions = raw_transactions \
        .select([
            pl.col("InvoiceNo").alias("invoice_no"),
            pl.col("StockCode").alias("stock_code"),
            pl.col("Description").alias("description"),
            pl.col("Quantity").alias("quantity"),
            pl.col("InvoiceDate").alias("invoice_date"),
            pl.col("UnitPrice").alias("unit_price"),
            pl.col("CustomerID").alias("customer_id"),
            pl.col("Country").alias("country")
        ]) \
        .filter((pl.col("invoice_date") >= start_date) & (pl.col("invoice_date") < end_date))

    yield Output(
        value=transactions,
        metadata={
            "length": MetadataValue.int(len(transactions)),
            "preview": MetadataValue.md(transactions.head().to_pandas().to_markdown()),
        }
    )