-- Auto Generated (Do not modify) F90B1D26FE8580341384A0E913464E5B9C7B8BFC90C4D48713226F8126767243
create view "dbo_intermediate"."int_gl_transactions" as -- int_gl_transactions.sql
-- Core GL transaction grain: one row per journal entry line,
-- enriched with account metadata, cost center, and fiscal period.
-- This is the central fact table that feeds all downstream reporting.

with je_lines as (
    select * from "FinanceOps_DW"."dbo_staging"."stg_journal_entry_lines"
),

je_headers as (
    select * from "FinanceOps_DW"."dbo_staging"."stg_journal_entries"
),

accounts as (
    select * from "FinanceOps_DW"."dbo_staging"."stg_chart_of_accounts"
),

cost_centers as (
    select * from "FinanceOps_DW"."dbo_staging"."stg_cost_centers"
),

periods as (
    select * from "FinanceOps_DW"."dbo_staging"."stg_fiscal_periods"
),

joined as (
    select
        -- Transaction identifiers
        jel.journal_entry_id,
        jel.line_number,
        je.posting_date,
        je.description             as je_description,
        jel.line_description,
        je.source_system,
        je.created_by,
        je.status                  as je_status,

        -- Amounts
        jel.debit_amount,
        jel.credit_amount,
        jel.net_amount,

        -- Account dimensions
        jel.account_id,
        a.account_name,
        a.account_type,
        a.financial_statement,
        a.account_subgroup,

        -- Cost center dimensions
        jel.cost_center_id,
        cc.cost_center_name,
        cc.department_group,

        -- Fiscal period
        p.fiscal_period_id,
        p.fiscal_year,
        p.fiscal_month,
        p.fiscal_quarter,
        p.is_closed                as period_is_closed

    from je_lines jel
    inner join je_headers je
        on jel.journal_entry_id = je.journal_entry_id
    inner join accounts a
        on jel.account_id = a.account_id
    inner join cost_centers cc
        on jel.cost_center_id = cc.cost_center_id
    inner join periods p
        on je.posting_date between p.period_start_date and p.period_end_date
)

select * from joined;