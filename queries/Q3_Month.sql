SELECT AVG(l_discount)
FROM read_parquet('TARGET_PATH/**/*.parquet', hive_partitioning=1)
WHERE ship_month = 1;
