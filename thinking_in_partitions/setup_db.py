import duckdb

DATABASE_PATH = "database.db"

if __name__ == "__main__":
    conn = duckdb.connect(DATABASE_PATH)
    conn.execute("""create or replace table ingested_orders (
        order_id integer,
        order_date date,
        order_total DECIMAL(10,2),
        customer_id integer,
        state varchar(16)
    )""")

    conn.execute("""create or replace table state_metrics (
        state varchar(16),
        total_orders integer,
        total_revenue DECIMAL(10,2)
    )""")

    conn.execute("""create or replace table daily_metrics (
        day date,
        total_orders integer,
        total_revenue DECIMAL(10,2)
    )""")