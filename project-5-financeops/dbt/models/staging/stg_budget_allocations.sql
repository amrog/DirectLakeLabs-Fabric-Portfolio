-- stg_budget_allocations.sql
-- Standardizes budget data from seed.

with source as (
    select * from {{ ref('budget_allocations') }}
),

cleaned as (
    select
        cast(fiscal_period_id as varchar(10))    as fiscal_period_id,
        cast(account_id as varchar(10))           as account_id,
        cast(cost_center_id as varchar(10))       as cost_center_id,
        cast(budget_amount as decimal(18,2))      as budget_amount,
        trim(budget_type)                         as budget_type
    from source
)

select * from cleaned
