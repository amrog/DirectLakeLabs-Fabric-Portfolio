-- rpt_trial_balance.sql
-- Trial balance by fiscal period.
-- Validates that total debits = total credits per period.
-- Balance Sheet accounts show cumulative balance;
-- Income Statement accounts show period activity.

with balances as (
    select * from {{ ref('int_account_balances') }}
),

final as (
    select
        fiscal_period_id,
        fiscal_year,
        fiscal_month,
        fiscal_quarter,
        account_id,
        account_name,
        account_type,
        financial_statement,
        account_subgroup,
        total_debits,
        total_credits,
        net_activity,
        ending_balance,
        -- Sign convention for reporting:
        -- Revenue/Liability/Equity show as positive when credit balance
        -- Expense/Asset show as positive when debit balance
        case
            when account_type in ('Revenue', 'Liability', 'Equity')
                then ending_balance * -1
            else ending_balance
        end as reported_balance
    from balances
)

select * from final
