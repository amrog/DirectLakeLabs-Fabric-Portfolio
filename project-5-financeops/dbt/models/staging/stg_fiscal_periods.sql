-- stg_fiscal_periods.sql
-- Standardizes fiscal period reference data from seed.

with source as (
    select * from {{ ref('fiscal_periods') }}
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

select * from cleaned
