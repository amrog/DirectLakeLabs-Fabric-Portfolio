-- rpt_budget_vs_actuals.sql
-- Compares actual GL activity against budget by account and cost center.
-- Calculates variance ($ and %) for management reporting.

with actuals as (
    select
        fiscal_period_id,
        account_id,
        cost_center_id,
        sum(debit_amount)       as actual_debits,
        sum(credit_amount)      as actual_credits,
        sum(net_amount)         as actual_net
    from {{ ref('int_gl_transactions') }}
    where je_status = 'Posted'
    group by fiscal_period_id, account_id, cost_center_id
),

budgets as (
    select * from {{ ref('stg_budget_allocations') }}
),

accounts as (
    select * from {{ ref('stg_chart_of_accounts') }}
),

cost_centers as (
    select * from {{ ref('stg_cost_centers') }}
),

periods as (
    select * from {{ ref('stg_fiscal_periods') }}
),

combined as (
    select
        coalesce(b.fiscal_period_id, a.fiscal_period_id) as fiscal_period_id,
        coalesce(b.account_id, a.account_id)             as account_id,
        coalesce(b.cost_center_id, a.cost_center_id)     as cost_center_id,
        coalesce(b.budget_amount, 0)                     as budget_amount,
        -- For revenue: actual = credits - debits (positive when earned)
        -- For expense: actual = debits - credits (positive when spent)
        case
            when acct.account_type = 'Revenue'
                then coalesce(a.actual_credits - a.actual_debits, 0)
            else coalesce(a.actual_debits - a.actual_credits, 0)
        end                                               as actual_amount,
        acct.account_name,
        acct.account_type,
        acct.account_subgroup,
        cc.cost_center_name,
        cc.department_group,
        p.fiscal_year,
        p.fiscal_month,
        p.fiscal_quarter
    from budgets b
    full outer join actuals a
        on b.fiscal_period_id = a.fiscal_period_id
        and b.account_id = a.account_id
        and b.cost_center_id = a.cost_center_id
    left join accounts acct
        on coalesce(b.account_id, a.account_id) = acct.account_id
    left join cost_centers cc
        on coalesce(b.cost_center_id, a.cost_center_id) = cc.cost_center_id
    left join periods p
        on coalesce(b.fiscal_period_id, a.fiscal_period_id) = p.fiscal_period_id
),

with_variance as (
    select
        c.*,
        actual_amount - budget_amount                     as variance_amount,
        case
            when budget_amount <> 0
                then cast((actual_amount - budget_amount) as decimal(18,4))
                     / cast(budget_amount as decimal(18,4))
            else null
        end                                               as variance_pct,
        -- Favorable/Unfavorable classification
        case
            when account_type = 'Revenue' and actual_amount >= budget_amount then 'Favorable'
            when account_type = 'Revenue' and actual_amount < budget_amount then 'Unfavorable'
            when account_type = 'Expense' and actual_amount <= budget_amount then 'Favorable'
            when account_type = 'Expense' and actual_amount > budget_amount then 'Unfavorable'
            else 'N/A'
        end                                               as variance_status
    from combined c
)

select * from with_variance
