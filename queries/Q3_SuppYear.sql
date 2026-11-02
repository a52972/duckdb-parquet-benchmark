SELECT AVG(l_discount)
FROM read_parquet('TARGET_PATH/**/*.parquet', hive_partitioning=1)
WHERE supp = 17 AND ship_year = 1996;
