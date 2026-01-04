-- Monitoring log table
CREATE TABLE IF NOT EXISTS pipeline_health_monitoring (
    monitor_date       DATE PRIMARY KEY,
    daily_revenue      NUMERIC,
    purchase_count     INTEGER,
    raw_event_count    INTEGER,
    max_timestamp      TIMESTAMPTZ,
    data_age_hours     NUMERIC,
    raw_revenue_sum    NUMERIC,
    reconciliation_diff NUMERIC,
    bot_event_count    INTEGER,
    bot_percentage     NUMERIC
);

-- Simple daily monitoring insert (run after pipeline completes)
INSERT INTO pipeline_health_monitoring
SELECT
    CURRENT_DATE - 1 AS monitor_date,  -- Monitor previous day

    -- Transformed revenue
    COALESCE(SUM(revenue), 0) AS daily_revenue,

    -- Purchase count
    COUNT(*) AS purchase_count,

    -- Raw event volume
    (SELECT COUNT(*) FROM raw_events WHERE event_date = CURRENT_DATE - 1) AS raw_event_count,

    -- Latest timestamp
    MAX(ts) AS max_timestamp,

    -- Data age in hours
    EXTRACT(EPOCH FROM (NOW() - MAX(ts))) / 3600 AS data_age_hours,

    -- Raw revenue from checkout events (for reconciliation)
    (SELECT COALESCE(SUM((event_data ->> 'revenue')::NUMERIC), 0)
     FROM raw_events
     WHERE event_name = 'checkout_completed'
       AND event_date = CURRENT_DATE - 1) AS raw_revenue_sum,

    -- Reconciliation difference
    ABS(
        COALESCE(SUM(revenue), 0) -
        COALESCE((SELECT SUM((event_data ->> 'revenue')::NUMERIC)
                  FROM raw_events
                  WHERE event_name = 'checkout_completed'
                    AND event_date = CURRENT_DATE - 1), 0)
    ) AS reconciliation_diff,

    -- Bot detection
    (SELECT COUNT(*) FROM raw_events
     WHERE event_date = CURRENT_DATE - 1
       AND user_agent ILIKE ANY(ARRAY['%bot%', '%crawler%', '%googlebot%', '%ahrefs%'])) AS bot_event_count,

    -- Bot percentage
    (SELECT 100.0 * COUNT(*) FILTER (WHERE user_agent ILIKE ANY(ARRAY['%bot%', '%crawler%', '%googlebot%', '%ahrefs%']))
     / COUNT(*)::NUMERIC
     FROM raw_events
     WHERE event_date = CURRENT_DATE - 1) AS bot_percentage

FROM mat_attribution
WHERE purchase_date = CURRENT_DATE - 1

ON CONFLICT (monitor_date) DO UPDATE SET
    daily_revenue       = EXCLUDED.daily_revenue,
    purchase_count      = EXCLUDED.purchase_count,
    raw_event_count     = EXCLUDED.raw_event_count,
    max_timestamp       = EXCLUDED.max_timestamp,
    data_age_hours      = EXCLUDED.data_age_hours,
    raw_revenue_sum     = EXCLUDED.raw_revenue_sum,
    reconciliation_diff = EXCLUDED.reconciliation_diff,
    bot_event_count     = EXCLUDED.bot_event_count,
    bot_percentage      = EXCLUDED.bot_percentage;

---------------------------We need to use this snippet in the dashboard logic-----------------------------------


SELECT 
    monitor_date,
    daily_revenue,
    purchase_count,
    raw_event_count,
    data_age_hours,
    reconciliation_diff,
    bot_percentage
FROM pipeline_health_monitoring
ORDER BY monitor_date DESC
LIMIT 30;

