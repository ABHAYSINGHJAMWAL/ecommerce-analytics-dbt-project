{% snapshot snap_users %}
{{
    config(
        target_schema = 'snapshots',
        unique_key = 'user_id',
        strategy = 'timestamp',
        updated_at = 'signup_timestamp'

    )
}}

select 
user_id,signup_timestamp,country,acquisition_channel
from {{ ref('stg_users') }}
{% endsnapshot %}