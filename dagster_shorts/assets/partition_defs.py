from dagster import (
    DailyPartitionsDefinition,
    WeeklyPartitionsDefinition,
    MonthlyPartitionsDefinition,
)

from math import floor
from datetime import datetime, timedelta

from ..constants.constants import START_DATE, END_DATE

start_date = datetime.strptime(START_DATE, "%Y-%m-%d")
end_date = datetime.strptime(END_DATE, "%Y-%m-%d")
this_month = datetime.today().replace(day=1)
offset_delta = this_month - end_date
end_offset_days = offset_delta.days * -1 - 8
end_offset_weeks = end_offset_days // 7
end_offset_months = int(end_offset_days // 30.5) - 1

monthly_start_date = start_date + timedelta(days=31)

daily_partitions = DailyPartitionsDefinition(START_DATE, end_offset=end_offset_days)
weekly_partitions = WeeklyPartitionsDefinition(START_DATE, end_offset=end_offset_weeks)
monthly_partitions = MonthlyPartitionsDefinition(monthly_start_date, end_offset=end_offset_months)