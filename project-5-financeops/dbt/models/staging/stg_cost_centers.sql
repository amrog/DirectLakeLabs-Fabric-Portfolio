-- stg_cost_centers.sql
-- Standardizes cost center reference data from seed.

with source as (
    select * from {{ ref('cost_centers') }}
),

cleaned as (
    select
        cast(cost_center_id as varchar(10))      as cost_center_id,
        trim(cost_center_name)                    as cost_center_name,
        trim(department_group)                    as department_group,
        trim(manager_name)                        as manager_name,
        case
            when upper(is_active) = 'TRUE' then 1
            else 0
        end                                       as is_active
    from source
)

select * from cleaned
