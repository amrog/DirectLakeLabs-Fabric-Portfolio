-- Auto Generated (Do not modify) CC871FD1CED90ABDC125D4DFD44D38C1ECBF0972CBD92AA9F6F4E63203645F27
create view "dbo_staging"."stg_fiscal_periods" as -- stg_fiscal_periods.sql
-- Standardizes fiscal period reference data from seed.

with source as (
    select * from "FinanceOps_DW"."dbo_raw"."fiscal_periods"
),

cleaned as (
    select
        cast(fiscal_period_id as varchar(10))    as fiscal_period_id,
        cast(fiscal_year as int)                  as fiscal_year,
        cast(fiscal_month as int)                 as fiscal_month,
        trim(fiscal_quarter)                      as fiscal_quarter,
        cast(period_start_date as date)           as period_start_date,
        cast(period_end_date as date)             as period_end_date,
        case
            when upper(is_closed) = 'TRUE' then 1
            else 0
        end                                       as is_closed
    from source
)

select * from cleaned;