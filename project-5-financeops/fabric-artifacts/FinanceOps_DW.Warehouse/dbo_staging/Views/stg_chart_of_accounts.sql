-- Auto Generated (Do not modify) 1F491D0D5C5B13A09AC54BBE3A0E4BEE472652BF03BED990A9B642502CA154B7
create view "dbo_staging"."stg_chart_of_accounts" as -- stg_chart_of_accounts.sql
-- Standardizes chart of accounts from seed data.

with source as (
    select * from "FinanceOps_DW"."dbo_raw"."chart_of_accounts"
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

select * from cleaned;