-- int_account_balances.sql
-- Aggregates GL transactions to account-period grain.
-- For Balance Sheet accounts: computes cumulative balance.
-- For Income Statement accounts: shows period activity only.

with gl as (
    select * from {{ ref('int_gl_transactions') }}
),

period_activity as (
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
        sum(debit_amount)       as total_debits,
        sum(credit_amount)      as total_credits,
        sum(net_amount)         as net_activity
    from gl
    where je_status = 'Posted'
    group by
        fiscal_period_id,
        fiscal_year,
        fiscal_month,
        fiscal_quarter,
        account_id,
        account_name,
        account_type,
        financial_statement,
        account_subgroup
),

-- Running balance for Balance Sheet accounts
-- Assets: natural debit balance (debits increase, credits decrease)
-- Liabilities/Equity: natural credit balance (credits increase, debits decrease)
with_running_balance as (
    select
        pa.*,
        case
            when pa.financial_statement = 'Balance Sheet' then
                sum(pa.net_activity) over (
                    partition by pa.account_id
                    order by pa.fiscal_year, pa.fiscal_month
                    rows between unbounded preceding and current row
                )
            else pa.net_activity
        end as ending_balance
    from period_activity pa
)

select * from with_running_balance
