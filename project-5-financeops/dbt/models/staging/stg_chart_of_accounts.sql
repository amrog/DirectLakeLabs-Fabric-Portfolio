-- stg_chart_of_accounts.sql
-- Standardizes chart of accounts from seed data.

with source as (
    select * from {{ ref('chart_of_accounts') }}
),

cleaned as (
    select
        cast(account_id as varchar(10))          as account_id,
        trim(account_name)                        as account_name,
        trim(account_type)                        as account_type,
        trim(financial_statement)                 as financial_statement,
        trim(account_subgroup)                    as account_subgroup,
        case
            when upper(is_active) = 'TRUE' then 1
            else 0
        end                                       as is_active
    from source
)

select * from cleaned
