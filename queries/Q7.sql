SELECT
    o.o_orderpriority,
    COUNT(DISTINCT l.l_orderkey)              AS order_count,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM read_parquet('TARGET_PATH/*.parquet') l
JOIN read_parquet('ORDERS_PATH')            o
  ON l.l_orderkey = o.o_orderkey
WHERE l.l_shipdate BETWEEN '1996-01-01' AND '1996-06-30'
GROUP BY o.o_orderpriority
ORDER BY o.o_orderpriority;
