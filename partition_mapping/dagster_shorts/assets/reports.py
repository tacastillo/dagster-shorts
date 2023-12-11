from dagster import (
    asset,
    get_dagster_logger,
    AssetIn,
    AssetKey,
    MetadataValue,
    Output,
)

import polars as pl
import pandas

from .partition_defs import daily_partitions, weekly_partitions, monthly_partitions

@asset(
    partitions_def=daily_partitions,
    compute_kind="DuckDB",
    metadata={
        "partition_expr": "invoice_date"
    },
    ins={
        "transactions": AssetIn(AssetKey(["sources", "transactions"]))
    }
)
def daily_report(context, transactions: pl.DataFrame) -> pl.DataFrame:
    """
    Daily report of transactions.
    """
    
    totals = transactions.with_columns(
        total_price=transactions["unit_price"] * transactions["quantity"]
    )

    # Get the sum of the total_price column
    total_sales = totals["total_price"].sum()

    # Get the number of unique invoice numbers
    total_transactions = totals["invoice_no"].n_unique()

    most_popular = transactions \
        .groupby(["stock_code", "description"]) \
        .count() \
        .sort("count", descending=True)
    
    if len(most_popular) == 0:
        most_popular = pl.DataFrame({
            "stock_code": [""],
            "description": [""],
        })

    raw_result = {
        "date": [context.asset_partition_key_for_output()],
        "total_sales": [total_sales],
        "total_transactions": [total_transactions],
        "most_popular_item_stock_code": [pl.first(most_popular["stock_code"])],
        "most_popular_item_description": [pl.first(most_popular["description"])],
    }

    result = pl.DataFrame(raw_result)

    yield Output(
        value=result,
        metadata={
            "preview": MetadataValue.md(result.head().to_pandas().to_markdown()),
            **{key: value[0] for key, value in raw_result.items()}
        }
    )

@asset(
    partitions_def=weekly_partitions,
    compute_kind="DuckDB",
    metadata={
        "partition_expr": "week_of"
    },
)
def weekly_report(context, daily_report) -> pl.DataFrame:
    """
    Weekly report of transactions.
    """
    reports = pl.concat(list(daily_report.values()))

    total_sales = reports["total_sales"].sum()
    total_transactions = reports["total_transactions"].sum()

    raw_result = {
        "week_of": [context.asset_partition_key_for_output()],
        "total_sales": [total_sales],
        "total_transactions": [total_transactions],
    }

    result = pl.DataFrame(raw_result)

    yield Output(
        value=result,
        metadata={
            "preview": MetadataValue.md(result.head().to_pandas().to_markdown()),
            **{key: value[0] for key, value in raw_result.items()}
        }
    )

@asset(
    partitions_def=monthly_partitions,
    compute_kind="DuckDB",
    metadata={
        "partition_expr": "invoice_date"
    },
)
def monthly_report(context, weekly_report) -> pl.DataFrame:
    """
    Monthly report of transactions.
    """
    reports = pl.concat(list(weekly_report.values()))

    total_sales = reports["total_sales"].sum()
    total_transactions = reports["total_transactions"].sum()

    raw_result = {
        "month_of": [context.asset_partition_key_for_output()],
        "total_sales": [total_sales],
        "total_transactions": [total_transactions],
    }

    result = pl.DataFrame(raw_result)

    yield Output(
        value=result,
        metadata={
            "preview": MetadataValue.md(result.head().to_pandas().to_markdown()),
            **{key: value[0] for key, value in raw_result.items()}
        }
    )
