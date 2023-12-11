from dagster import Definitions, asset, AssetExecutionContext, WeeklyPartitionsDefinition, StaticPartitionsDefinition, DailyPartitionsDefinition, BackfillPolicy
from datetime import datetime
import pandas as pd
import duckdb

ORDERS_PATH = "orders.csv"
DATABASE_PATH = "database.db"

START_DATE = "2024-01-01"
END_DATE = "2024-02-01"

###

@asset
def orders(context: AssetExecutionContext) -> None:
    """Read in the records from the orders.csv file and write them to a database."""
    raw_orders = pd.read_csv(ORDERS_PATH)
    conn = duckdb.connect(DATABASE_PATH)
    conn.execute("insert into ingested_orders select * from raw_orders")

    context.log.info(f"Writing {len(raw_orders)} orders to database")

####

weekly_partition = WeeklyPartitionsDefinition(
    start_date=START_DATE,
    end_date=END_DATE,
)


@asset(
    partitions_def=weekly_partition,
)
def partitioned_orders(context: AssetExecutionContext) -> None:
    conn = duckdb.connect(DATABASE_PATH)

    partition_date_str = context.asset_partition_key_for_output()
    week_to_fetch = datetime.strptime(partition_date_str, "%Y-%m-%d").date().strftime("%Y-%m-%d")

    context.log.info(f"Fetching orders for week starting {week_to_fetch}")

    conn.execute(
        f"""
            insert into ingested_orders
            select *
            from '{ORDERS_PATH}'
            where date_trunc('week', order_date) = {week_to_fetch}
        """
    )

###

state_partitions = StaticPartitionsDefinition(
    partition_keys=["CA", "FL", "NY", "TX"]
)


@asset(
    partitions_def=state_partitions,
    deps=[orders]
)
def state_metrics(context: AssetExecutionContext) -> None:
    conn = duckdb.connect(DATABASE_PATH)

    state_to_fetch = context.asset_partition_key_for_output()

    context.log.info(f"Making metrics for {state_to_fetch}")

    conn.execute(
        f"""
            insert into state_metrics
            select
                state,
                count(*) as total_orders,
                sum(order_total) as total_revenue
            from ingested_orders
            where state = '{state_to_fetch}'
            group by state
        """
    )

###

daily_partition = DailyPartitionsDefinition(
    start_date=START_DATE,
    end_date=END_DATE,
)

@asset(
    partitions_def=daily_partition,
    backfill_policy=BackfillPolicy.single_run(),
    deps=[orders]
)
def daily_metrics(context: AssetExecutionContext) -> None:
    start_datetime, end_datetime = context.partition_time_window

    context.log.info(f"Making metrics for {start_datetime} to {end_datetime}")

    conn = duckdb.connect(DATABASE_PATH)
    conn.execute(
        f"""
            insert into daily_metrics
            select
                date_trunc('day', order_date) as date_of_business,
                count(*) as total_orders,
                sum(order_total) as total_revenue
            from ingested_orders
            where order_date >= '{start_datetime}' and order_date < '{end_datetime}'
            group by date_trunc('day', order_date)
        """
    )




defs = Definitions(
    assets=[orders, partitioned_orders, state_metrics, daily_metrics]
)