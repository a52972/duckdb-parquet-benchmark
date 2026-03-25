SELECT COUNT(*), AVG(l_extendedprice)
FROM read_parquet('TARGET_PATH/*.parquet')
WHERE l_shipdate BETWEEN '1996-01-01' AND '1996-01-31';
