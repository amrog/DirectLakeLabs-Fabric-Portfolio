-- rpt_income_statement.sql
-- Monthly income statement with gross profit, operating income, and net income.
-- Revenue shown as positive, expenses as positive (conventional P&L format).

with gl as (
    select * from {{ ref('int_gl_transactions') }}
    where je_status = 'Posted'
),

monthly_summary as (
    select
        fiscal_period_id,
        fiscal_year,
        fiscal_month,
        fiscal_quarter,
        account_subgroup,

        -- Revenue: credit balances, flip sign to show positive
        sum(case when account_type = 'Revenue' then credit_amount - debit_amount else 0 end)
            as revenue_amount,

        -- Expenses: debit balances, show as positive
        sum(case when account_type = 'Expense' then debit_amount - credit_amount else 0 end)
            as expense_amount

    from gl
    group by
        fiscal_period_id,
        fiscal_year,
        fiscal_month,
        fiscal_quarter,
        account_subgroup
),

pivoted as (
    select
        fiscal_period_id,
        fiscal_year,
        fiscal_month,
        fiscal_quarter,

        -- Revenue lines
        sum(case when account_subgroup = 'Operating Revenue' then revenue_amount else 0 end)
            as operating_revenue,
        sum(case when account_subgroup = 'Non-operating Revenue' then revenue_amount else 0 end)
            as other_revenue,

        -- COGS
        sum(case when account_subgroup = 'Cost of Revenue' then expense_amount else 0 end)
            as cost_of_revenue,

        -- OpEx
        sum(case when account_subgroup = 'Operating Expenses' then expense_amount else 0 end)
            as operating_expenses,

        -- Non-operating
        sum(case when account_subgroup = 'Non-operating Expenses' then expense_amount else 0 end)
            as non_operating_expenses

    from monthly_summary
    group by
        fiscal_period_id,
        fiscal_year,
        fiscal_month,
        fiscal_quarter
),

with_margins as (
    select
        p.*,
        operating_revenue + other_revenue                   as total_revenue,
        operating_revenue - cost_of_revenue                 as gross_profit,
        operating_revenue - cost_of_revenue - operating_expenses
                                                            as operating_income,
        operating_revenue + other_revenue
            - cost_of_revenue - operating_expenses
            - non_operating_expenses                        as net_income,

        -- Margin calculations
        case when operating_revenue > 0
            then cast((operating_revenue - cost_of_revenue) as decimal(18,4))
                 / cast(operating_revenue as decimal(18,4))
            else 0
        end                                                 as gross_margin_pct,

        case when operating_revenue > 0
            then cast((operating_revenue - cost_of_revenue - operating_expenses) as decimal(18,4))
                 / cast(operating_revenue as decimal(18,4))
            else 0
        end                                                 as operating_margin_pct
    from pivoted p
)

select * from with_margins
