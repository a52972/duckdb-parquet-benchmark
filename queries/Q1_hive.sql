SELECT AVG(l_quantity), SUM(l_extendedprice)
FROM read_parquet('TARGET_PATH/**/*.parquet', hive_partitioning=1);
