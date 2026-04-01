{{ config(materialized = 'view') }}

WITH orders AS (
    SELECT
        order_id,
        user_id,
        order_timestamp,
        order_amount,
        currency
    FROM {{ ref('stg_orders') }}
),

users AS (
    SELECT
        user_id,
        signup_timestamp,
        country,
        acquisition_channel
    FROM {{ ref('stg_users') }}
),

joined AS (
    SELECT  
        o.order_id,
        o.user_id,
        o.order_timestamp,
        o.order_amount,
        o.currency,
        u.signup_timestamp,
        u.country,
        u.acquisition_channel
    FROM orders o
    LEFT JOIN users u
        ON o.user_id = u.user_id
)

SELECT
    order_id,
    user_id,
    order_timestamp,
    order_amount,
    currency,
    signup_timestamp,
    country,
    acquisition_channel
FROM joined