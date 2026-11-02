SELECT COUNT(*)
FROM read_parquet('TARGET_PATH/**/*.parquet', hive_partitioning=1);
