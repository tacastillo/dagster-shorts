from dagster import Definitions, load_assets_from_modules, define_asset_job, AssetSelection
from dagster_duckdb_polars import duckdb_polars_io_manager

from .assets import sources, reports

source_assets = load_assets_from_modules(
    [sources],
    group_name="sources",
    key_prefix="sources",
)

report_assets = load_assets_from_modules(
    [reports],
    group_name="reports",
    key_prefix="reports",
)

database_io_manager = duckdb_polars_io_manager.configured({
    "database": "data/database.db",
})

job = define_asset_job(
    "sources",
    selection=AssetSelection.groups("sources"),
)


defs = Definitions(
    assets=source_assets + report_assets,
    jobs=[job],
    resources={
        "database_io_manager": database_io_manager
    },
)