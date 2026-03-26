{{ config(materialized = 'table') }}

WITH events AS (
    SELECT user_id, event_timestamp
    FROM {{ ref('stg_events') }}
),

ordered_events AS (
    SELECT 
        user_id,
        event_timestamp,
        LAG(event_timestamp) OVER (
            PARTITION BY user_id 
            ORDER BY event_timestamp
        ) AS previous_event_time
    FROM events
),

session_flags AS (
    SELECT 
        user_id,
        event_timestamp,
        CASE 
            WHEN previous_event_time IS NULL THEN 1
            WHEN TIMESTAMP_DIFF(event_timestamp, previous_event_time, MINUTE) > 30 THEN 1
            ELSE 0
        END AS new_session
    FROM ordered_events
),

session_ids AS (
    SELECT 
        user_id,
        event_timestamp,
        SUM(new_session) OVER (
            PARTITION BY user_id 
            ORDER BY event_timestamp
        ) AS session_id
    FROM session_flags
),

session_summary AS (
    SELECT 
        user_id,
        session_id,
        MIN(event_timestamp) AS session_start,
        MAX(event_timestamp) AS session_end,
        COUNT(*) AS events_in_session
    FROM session_ids
    GROUP BY user_id, session_id
)

SELECT 
    user_id,
    session_id,
    session_start,
    session_end,
    TIMESTAMP_DIFF(session_end, session_start, SECOND) AS session_duration,
    events_in_session
FROM session_summary