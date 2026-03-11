-- test_trial_balance_nets_to_zero.sql
-- Validates the fundamental accounting equation:
-- Total Debits = Total Credits for each fiscal period.
-- Any imbalance indicates a data quality problem upstream.

with period_totals as (
    select
        fiscal_period_id,
        sum(total_debits)  as period_debits,
        sum(total_credits) as period_credits,
        abs(sum(total_debits) - sum(total_credits)) as period_imbalance
    from {{ ref('rpt_trial_balance') }}
    group by fiscal_period_id
)

select *
from period_totals
where period_imbalance > 0.10
