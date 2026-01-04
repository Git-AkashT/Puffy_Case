ALTER TABLE raw_events ADD COLUMN IF NOT EXISTS event_date DATE GENERATED ALWAYS AS (DATE(ts)) STORED;

-- Staging (parse once; can be incremental)
CREATE OR REPLACE VIEW stg_events AS
SELECT
    COALESCE(client_id, "clientId") AS unified_client,
    "Pkey",
    ts,
    event_name,
    event_data,
    user_agent,
    event_date,
    COALESCE(
        (event_data ->> 'utm_source'),
        regexp_match(page_url || '?' || COALESCE(NULLIF(referrer, ''), ''), '([?&](utm_source|gclid|irclickid|sfdr_ptcid)=([^&]+))')[3],
        COALESCE(NULLIF(split_part(NULLIF(referrer, ''), '://', 2), '/')[1], 'direct')
    ) AS channel,
    CASE
        WHEN user_agent ILIKE '%mobile%' OR user_agent ILIKE '%android%' OR user_agent ILIKE '%iphone%' THEN 'mobile'
        WHEN user_agent ILIKE '%ipad%' OR user_agent ILIKE '%tablet%' THEN 'tablet'
        ELSE 'desktop'
    END AS device,
    CASE WHEN event_name = 'checkout_completed' THEN (event_data ->> 'revenue')::NUMERIC ELSE 0 END AS revenue
FROM raw_events;

-- Incremental Sessions Table (using MERGE for upsert)
CREATE TABLE IF NOT EXISTS mat_sessions (
    unified_client TEXT,
    session_id BIGINT,
    session_start TIMESTAMPTZ,
    session_end TIMESTAMPTZ,
    page_views INT,
    add_to_carts INT,
    checkouts_started INT,
    purchases INT,
    session_revenue NUMERIC,
    first_channel TEXT,
    last_channel TEXT,
    device TEXT,
    last_updated_date DATE DEFAULT CURRENT_DATE
);

-- Daily Incremental Refresh for Sessions
WITH new_data AS (
    SELECT * FROM stg_events WHERE event_date = CURRENT_DATE - INTERVAL '1 day'  -- Latest partition
),
lagged AS (
    SELECT *,
        LAG(ts) OVER (PARTITION BY unified_client ORDER BY ts) AS prev_ts
    FROM new_data
),
session_flags AS (
    SELECT *,
        CASE WHEN ts - prev_ts > INTERVAL '30 minutes' OR prev_ts IS NULL THEN 1 ELSE 0 END AS new_session_flag
    FROM lagged
),
session_groups AS (
    SELECT *,
        SUM(new_session_flag) OVER (PARTITION BY unified_client ORDER BY ts ROWS UNBOUNDED PRECEDING) AS session_id
    FROM session_flags
),
new_sessions AS (
    SELECT
        unified_client,
        session_id,
        MIN(ts) AS session_start,
        MAX(ts) AS session_end,
        COUNT(CASE WHEN event_name = 'page_viewed' THEN 1 END) AS page_views,
        COUNT(CASE WHEN event_name = 'product_added_to_cart' THEN 1 END) AS add_to_carts,
        COUNT(CASE WHEN event_name = 'checkout_started' THEN 1 END) AS checkouts_started,
        COUNT(CASE WHEN event_name = 'checkout_completed' THEN 1 END) AS purchases,
        SUM(revenue) AS session_revenue,
        MIN(channel) AS first_channel,
        MAX(channel) AS last_channel,
        MIN(device) AS device
    FROM session_groups
    GROUP BY unified_client, session_id
),
affected_clients AS (
    SELECT DISTINCT unified_client FROM new_data
),
sessions_to_delete AS (
    SELECT s.* FROM mat_sessions s
    JOIN affected_clients ac ON s.unified_client = ac.unified_client
    -- Optional: Or delete only sessions overlapping new data
),
sessions_to_insert AS (
    SELECT ns.*, CURRENT_DATE AS last_updated_date FROM new_sessions ns
)
-- Upsert: Delete affected, insert new
DELETE FROM mat_sessions WHERE unified_client IN (SELECT unified_client FROM affected_clients);
INSERT INTO mat_sessions SELECT * FROM sessions_to_insert;

-- Incremental Attribution Table
CREATE TABLE IF NOT EXISTS mat_attribution (
    transaction_id TEXT,
    revenue NUMERIC,
    first_click_channel TEXT,
    last_click_channel TEXT,
    purchase_date DATE,
    last_updated_date DATE DEFAULT CURRENT_DATE
);

-- Daily Incremental Refresh for Attribution
WITH new_purchases AS (
    SELECT
        (event_data ->> 'transaction_id') AS transaction_id,
        revenue,
        ts AS purchase_ts,
        unified_client,
        event_date
    FROM stg_events
    WHERE event_name = 'checkout_completed' AND event_date = CURRENT_DATE - INTERVAL '1 day'
),
new_attribution AS (
    SELECT
        p.transaction_id,
        p.revenue,
        MIN(t.channel) FILTER (WHERE t.ts >= p.purchase_ts - INTERVAL '7 days') AS first_click_channel,
        MAX(t.channel) FILTER (WHERE t.ts >= p.purchase_ts - INTERVAL '7 days' AND t.ts <= p.purchase_ts) AS last_click_channel,
        p.event_date AS purchase_date
    FROM new_purchases p
    JOIN stg_events t ON t.unified_client = p.unified_client AND t.ts <= p.purchase_ts
    GROUP BY p.transaction_id, p.revenue, p.event_date
)
-- Simple insert (or MERGE if duplicates possible)
INSERT INTO mat_attribution
SELECT transaction_id, revenue, first_click_channel, last_click_channel, purchase_date, CURRENT_DATE
FROM new_attribution
ON CONFLICT (transaction_id) DO UPDATE SET
    revenue = EXCLUDED.revenue,
    first_click_channel = EXCLUDED.first_click_channel,
    last_click_channel = EXCLUDED.last_click_channel,
    last_updated_date = CURRENT_DATE;

