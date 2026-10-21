-- Credit consumption per warehouse and day (last 30 days) and the most expensive queries.
-- ACCOUNT_USAGE views have up to ~3 hours of latency.

SELECT
    warehouse_name,
    DATE_TRUNC('day', start_time)       AS dia,
    ROUND(SUM(credits_used), 2)         AS creditos
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY 2 DESC, 3 DESC;

SELECT
    warehouse_name,
    user_name,
    LEFT(query_text, 120)               AS consulta,
    total_elapsed_time / 1000           AS segundos,
    bytes_spilled_to_local_storage      AS spill_local,  -- > 0 means the warehouse is too small
    queued_overload_time / 1000         AS segundos_em_fila
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND warehouse_name IS NOT NULL
ORDER BY total_elapsed_time DESC
LIMIT 50;
