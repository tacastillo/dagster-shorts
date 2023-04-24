from setuptools import find_packages, setup

setup(
    name="dagster_shorts",
    packages=find_packages(exclude=["dagster_shorts_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-duckdb-polars",
        "pandas",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
