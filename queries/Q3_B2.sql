SELECT AVG(l_discount)
FROM read_parquet('TARGET_PATH/**/*.parquet', hive_partitioning=1)
WHERE ship_year = 1996
  AND l_shipdate BETWEEN '1996-01-01' AND '1996-01-31';
