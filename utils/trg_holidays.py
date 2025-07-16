from datetime import datetime, timezone, timedelta

holidays = spark.table("finance_catalog.db_landing.src_raw_holidays").toPandas()["date"].tolist()
today = (datetime.utcnow() - timedelta(hours=5)).strftime("%Y-%m-%d")


if today in holidays:
    raise Exception("HOLIDAY")
else:
    pass