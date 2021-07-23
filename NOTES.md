# NOTEBOOK

## 20210723

### Backfilling of ticker data

Ran the following:

```
airflow dags backfill -s 2021-07-13 -e 2021-07-14 sp500_dag
airflow dags backfill -s 2021-07-06 -e 2021-07-07 sp500_dag
```

while only running the tasks `get_ticker_symbols` and `get_ticker_data` and
temporarily setting the Airflow variable `sp500_recent_days` to 6 since only max 7 days
can be fetched at 1m intervals.
Only two backfills above where performed because minute-based data is limit
to 30 days.
