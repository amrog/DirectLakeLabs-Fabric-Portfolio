-- Auto Generated (Do not modify) 19667131337B18591FABC21615DEC8CCB158071EF4B7C6DD6EB3FA11C79D4736
create view "dbo_staging"."stg_journal_entry_lines" as -- stg_journal_entry_lines.sql
-- Cleans journal entry line items. Adds net_amount (debit - credit)
-- for easier downstream aggregation.

with source as (
    select * from "FinanceOps_DW"."raw"."journal_entry_lines"
),

cleaned as (
    select
        cast(journal_entry_id as varchar(20))    as journal_entry_id,
        cast(line_number as int)                  as line_number,
        cast(account_id as varchar(10))           as account_id,
        cast(cost_center_id as varchar(10))       as cost_center_id,
        cast(debit_amount as decimal(18,2))       as debit_amount,
        cast(credit_amount as decimal(18,2))      as credit_amount,
        cast(debit_amount as decimal(18,2))
            - cast(credit_amount as decimal(18,2)) as net_amount,
        trim(line_description)                    as line_description
    from source
)

select * from cleaned;