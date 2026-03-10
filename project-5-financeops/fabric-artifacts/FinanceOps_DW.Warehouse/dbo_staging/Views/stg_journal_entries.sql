-- Auto Generated (Do not modify) DD0C0C32FD2BE3217F9C6F50E2BC14B6ED4ADE56A4E538CF1628FEDD9E92D186
create view "dbo_staging"."stg_journal_entries" as -- stg_journal_entries.sql
-- Cleans and standardizes journal entry headers from the raw ERP extract.
-- Filters to Posted entries only (reversals kept — they're valid postings).

with source as (
    select * from "FinanceOps_DW"."raw"."journal_entries"
),

cleaned as (
    select
        cast(journal_entry_id as varchar(20))   as journal_entry_id,
        cast(posting_date as date)               as posting_date,
        trim(description)                        as description,
        trim(source_system)                      as source_system,
        trim(created_by)                         as created_by,
        trim(status)                             as status,
        cast(created_at as datetime2(6))         as created_at
    from source
    where status in ('Posted', 'Reversed')
)

select * from cleaned;