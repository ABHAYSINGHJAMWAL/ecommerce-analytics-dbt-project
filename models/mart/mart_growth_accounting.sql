WITH daily_activity AS (
    SELECT
        user_id,
        DATE(event_timestamp) AS activity_date
    FROM {{ ref('stg_events') }}
    GROUP BY 1, 2
),

user_activity_with_lag AS (
    SELECT
        user_id,
        activity_date,
        LAG(activity_date) OVER (
            PARTITION BY user_id
            ORDER BY activity_date
        ) AS prev_activity_date
    FROM daily_activity
),

user_status_cte AS (
    SELECT
        user_id,
        activity_date,
        prev_activity_date,
        DATE_DIFF(activity_date, prev_activity_date, DAY) AS days_since_last_active,
        CASE
            WHEN prev_activity_date IS NULL THEN 'new'
            WHEN DATE_DIFF(activity_date, prev_activity_date, DAY) = 1 THEN 'retained'
            WHEN DATE_DIFF(activity_date, prev_activity_date, DAY) > 7 THEN 'resurrected'
            ELSE 'retained'
        END AS user_status
    FROM user_activity_with_lag
),

growth_accounting AS (
    SELECT
        activity_date,
        COUNT(DISTINCT CASE WHEN user_status = 'new' THEN user_id END) AS new_users,
        COUNT(DISTINCT CASE WHEN user_status = 'retained' THEN user_id END) AS retained_users,
        COUNT(DISTINCT CASE WHEN user_status = 'resurrected' THEN user_id END) AS resurrected_users,
        COUNT(DISTINCT user_id) AS dau
    FROM user_status_cte
    GROUP BY 1
)

SELECT
    activity_date,
    new_users,
    retained_users,
    resurrected_users,
    dau,
    {{ safe_divide('retained_users', 'dau') }} AS retention_ratio,
    {{ safe_divide('new_users', 'dau') }} AS new_user_ratio
FROM growth_accounting
ORDER BY activity_date