from dagster import (
    DailyPartitionsDefinition,
    WeeklyPartitionsDefinition,
    MonthlyPartitionsDefinition,
)

daily_partitions = DailyPartitionsDefinition("2010-12-01")
weekly_partitions = WeeklyPartitionsDefinition("2010-12-01")
monthly_partitions = MonthlyPartitionsDefinition("2010-12-01")