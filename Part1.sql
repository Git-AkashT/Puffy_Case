CREATE TABLE IF NOT EXISTS raw_events (
    client_id TEXT,
    page_url TEXT,
    referrer TEXT,
    timestamp TIMESTAMPTZ,
    event_name TEXT,
    event_data JSONB,
    user_agent TEXT,
    "clientId" TEXT,
    "Pkey" TEXT PRIMARY KEY  
);

-- Creating a Procedure to create the validation framework

CREATE OR REPLACE FUNCTION validate_event_data(
    expected_event_names TEXT[] DEFAULT ARRAY['page_viewed', 'email_filled_on_popup', 'product_added_to_cart', 'checkout_started', 'checkout_completed'],
    revenue_threshold NUMERIC DEFAULT 0,
    bot_regex TEXT DEFAULT '(bot|crawler|spider|googlebot|ahrefs)',
    max_null_pct NUMERIC DEFAULT 0.1,
    anomaly_drop_threshold NUMERIC DEFAULT 0.5,
    session_gap_threshold INTERVAL DEFAULT '1 day',
    max_events_per_client BIGINT DEFAULT 100
)
RETURNS TABLE (check_type TEXT, issue_description TEXT, details TEXT) AS $$
DECLARE
    total_rows BIGINT;
BEGIN
    SELECT COUNT(*) INTO total_rows FROM raw_events;

    -- 1. Primary Key Checks (Pkey uniqueness/nulls enforced by constraint; query for issues)
    RETURN QUERY
    SELECT 'PK Check' AS check_type, 'Null Pkey' AS issue_description, COUNT(*)::TEXT AS details FROM raw_events WHERE "Pkey" IS NULL
    UNION ALL
    SELECT 'PK Check', 'Duplicate Pkey', (COUNT(*) - COUNT(DISTINCT "Pkey"))::TEXT FROM raw_events WHERE COUNT(*) - COUNT(DISTINCT "Pkey") > 0;

    -- 2. Null Checks
    RETURN QUERY
    WITH null_stats AS (
        SELECT
            (COUNT(*) FILTER (WHERE timestamp IS NULL))::NUMERIC / total_rows AS null_ts_pct,
            (COUNT(*) FILTER (WHERE event_name IS NULL))::NUMERIC / total_rows AS null_event_pct,
            (COUNT(*) FILTER (WHERE client_id IS NULL AND "clientId" IS NULL))::NUMERIC / total_rows AS null_client_pct,
            (COUNT(*) FILTER (WHERE referrer IS NULL))::NUMERIC / total_rows AS null_ref_pct
        FROM raw_events
    )
    SELECT 'Null Check', 'High nulls in timestamp', null_ts_pct::TEXT || '%' FROM null_stats WHERE null_ts_pct > 0
    UNION ALL
    SELECT 'Null Check', 'High nulls in event_name', null_event_pct::TEXT || '%' FROM null_stats WHERE null_event_pct > 0
    UNION ALL
    SELECT 'Null Check', 'High nulls in unified client IDs', null_client_pct::TEXT || '%' || ' (> ' || (max_null_pct * 100)::TEXT || '%)' FROM null_stats WHERE null_client_pct > max_null_pct
    UNION ALL
    SELECT 'Null Check', 'High nulls in referrer', null_ref_pct::TEXT || '%' FROM null_stats WHERE null_ref_pct > 0.5;

    -- 3. Duplicate Rows (beyond PK)
    RETURN QUERY
    SELECT 'Duplicate Check', 'Duplicate rows (non-PK)', COUNT(*)::TEXT FROM (SELECT *, COUNT(*) OVER (PARTITION BY client_id, "clientId", timestamp, event_name, event_data) AS cnt FROM raw_events) sub WHERE cnt > 1;

    -- 4. Format Validation
    RETURN QUERY
    SELECT 'Format Check', 'Invalid/future timestamps', COUNT(*)::TEXT FROM raw_events WHERE timestamp > NOW() + INTERVAL '1 day' OR timestamp < NOW() - INTERVAL '365 days'
    UNION ALL
    SELECT 'Format Check', 'Span >14 days', (MAX(timestamp) - MIN(timestamp))::TEXT FROM raw_events WHERE (MAX(timestamp) - MIN(timestamp)) > INTERVAL '14 days'
    UNION ALL
    SELECT 'Format Check', 'Invalid JSON in event_data', COUNT(*)::TEXT FROM raw_events WHERE event_data IS NOT NULL AND NOT jsonb_valid(event_data);

    -- 5. Constraint Checks
    RETURN QUERY
    SELECT 'Constraint Check', 'Invalid event_names', STRING_AGG(DISTINCT event_name, ', ') FROM raw_events WHERE NOT (event_name = ANY (expected_event_names))
    UNION ALL
    SELECT 'Constraint Check', 'Invalid revenues (<= ' || revenue_threshold::TEXT || ')', COUNT(*)::TEXT FROM raw_events WHERE event_name = 'checkout_completed' AND (event_data ->> 'revenue')::NUMERIC <= revenue_threshold;

    -- 6. Bot Detection
    RETURN QUERY
    WITH bot_stats AS (
        SELECT COUNT(*)::NUMERIC / total_rows AS bot_pct, COUNT(*) AS bot_count FROM raw_events WHERE user_agent ~* bot_regex
    )
    SELECT 'Bot Check', 'High bot traffic', bot_pct::TEXT || '% (' || bot_count::TEXT || ' events)' FROM bot_stats WHERE bot_pct > 0.05;

    -- 7. Anomaly Detection
    RETURN QUERY
    WITH daily_rev AS (
        SELECT DATE_TRUNC('day', timestamp) AS day, SUM((event_data ->> 'revenue')::NUMERIC) AS rev FROM raw_events WHERE event_name = 'checkout_completed' GROUP BY day
    ), rev_stats AS (
        SELECT COUNT(*) FILTER (WHERE rev <= 0) AS zero_days,
               STRING_AGG(day::TEXT, ', ') FILTER (WHERE rev / LAG(rev) OVER (ORDER BY day) < (1 - anomaly_drop_threshold)) AS drop_days
        FROM daily_rev
    )
    SELECT 'Anomaly Check', 'Zero/low revenue days', zero_days::TEXT FROM rev_stats WHERE zero_days > 0
    UNION ALL
    SELECT 'Anomaly Check', 'Revenue drops on dates', drop_days FROM rev_stats WHERE drop_days IS NOT NULL;

    -- 8. Freshness Check
    RETURN QUERY
    SELECT 'Freshness Check', 'Stale data', MAX(timestamp)::TEXT FROM raw_events WHERE (NOW() - MAX(timestamp)) > INTERVAL '1 day';

    -- 9. Outlier Check
    RETURN QUERY
    WITH client_events AS (
        SELECT COALESCE(client_id, "clientId") AS client, COUNT(*) AS cnt FROM raw_events GROUP BY client
    )
    SELECT 'Outlier Check', 'Excessive events/client (> ' || max_events_per_client::TEXT || ')', COUNT(*)::TEXT FROM client_events WHERE cnt > max_events_per_client;

    -- 10. Sessionization Checks
    RETURN QUERY
    WITH client_ts AS (
        SELECT COALESCE(client_id, "clientId") AS client, timestamp, LAG(timestamp) OVER (PARTITION BY client ORDER BY timestamp) AS prev_ts
        FROM raw_events WHERE COALESCE(client_id, "clientId") IS NOT NULL
    )
    SELECT 'Session Check', 'Out-of-order timestamps', COUNT(*)::TEXT FROM client_ts WHERE timestamp < prev_ts
    UNION ALL
    SELECT 'Session Check', 'Large gaps (> ' || session_gap_threshold::TEXT || ')', COUNT(*)::TEXT FROM client_ts WHERE timestamp - prev_ts > session_gap_threshold
    UNION ALL
    WITH ua_stats AS (
        SELECT COALESCE(client_id, "clientId") AS client, COUNT(DISTINCT user_agent) AS ua_cnt FROM raw_events GROUP BY client
    )
    SELECT 'Session Check', 'Inconsistent user_agents', COUNT(*)::TEXT FROM ua_stats WHERE ua_cnt > 1
    UNION ALL
    WITH checkout_clients AS (
        SELECT DISTINCT COALESCE(client_id, "clientId") AS client FROM raw_events WHERE event_name = 'checkout_completed'
    ), add_cart_clients AS (
        SELECT DISTINCT COALESCE(client_id, "clientId") AS client FROM raw_events WHERE event_name = 'product_added_to_cart'
    )
    SELECT 'Session Check', 'Orphan checkouts', COUNT(*)::TEXT FROM checkout_clients LEFT JOIN add_cart_clients USING(client) WHERE add_cart_clients.client IS NULL;
END;
$$ LANGUAGE plpgsql;

-- Usage
SELECT * FROM validate_event_data();

