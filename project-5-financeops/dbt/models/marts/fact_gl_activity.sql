-- fact_gl_activity.sql
-- Core fact table: one row per journal entry line per period.
-- Joins to dimension surrogate keys for star schema.

with gl as (
    select * from {{ ref('int_gl_transactions') }}
),

dim_acct as (
    select account_key, account_id from {{ ref('dim_accounts') }}
),

dim_cc as (
    select cost_center_key, cost_center_id from {{ ref('dim_cost_centers') }}
),

dim_fp as (
    select fiscal_period_key, fiscal_period_id from {{ ref('dim_fiscal_periods') }}
),

final as (
    select
        -- Surrogate keys for star schema joins
        da.account_key,
        dc.cost_center_key,
        dp.fiscal_period_key,

        -- Degenerate dimensions (kept on fact for drill-through)
        gl.journal_entry_id,
        gl.line_number,
        gl.posting_date,
        gl.je_description,
        gl.source_system,
        gl.je_status,

        -- Measures
        gl.debit_amount,
        gl.credit_amount,
        gl.net_amount

    from gl
    inner join dim_acct da on gl.account_id = da.account_id
    inner join dim_cc dc on gl.cost_center_id = dc.cost_center_id
    inner join dim_fp dp on gl.fiscal_period_id = dp.fiscal_period_id
)

select * from final
