{{ config(materialized = 'view')}}

with source as (
    select *
     from {{ source('raw','experiment_assignments_raw')}}
),

cleaned as (
select
 cast(assignment_id as string) as assignment_id,
experiment_id,experiment_name,
cast(user_id as string) as user_id,
lower(experiment_group) as experiment_group,
cast(assigned_at as timestamp) as assigned_at,
cast(experiment_start_date as date) as experiment_start_date
from source
)
select * from cleaned