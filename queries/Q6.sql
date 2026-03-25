SELECT
    date_part('year',  l_shipdate)::INTEGER AS ship_year,
    date_part('month', l_shipdate)::INTEGER AS ship_month,
    SUM(l_extendedprice * (1 - l_discount))  AS revenue,
    COUNT(*)                                  AS line_count
FROM read_parquet('TARGET_PATH/*.parquet')
GROUP BY 1, 2
ORDER BY 1, 2;
