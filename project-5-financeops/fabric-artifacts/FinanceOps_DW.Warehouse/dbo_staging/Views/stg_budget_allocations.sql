-- Auto Generated (Do not modify) 0172BFA5D7969A855581613F383C9F8A0DA4C67C5DEB7D8505BC830C95F69CC4
create view "dbo_staging"."stg_budget_allocations" as -- stg_budget_allocations.sql
-- Standardizes budget data from seed.

with source as (
    select * from "FinanceOps_DW"."dbo_raw"."budget_allocations"
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

select * from cleaned;