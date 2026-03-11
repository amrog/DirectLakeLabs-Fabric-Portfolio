-- dim_accounts.sql
-- Account dimension for the financial data warehouse.
-- Surrogate key generated via hash (Fabric DW does not support IDENTITY columns).

with accounts as (
    select * from {{ ref('stg_chart_of_accounts') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['account_id']) }} as account_key,
        account_id,
        account_name,
        account_type,
        financial_statement,
        account_subgroup,
        is_active,
        -- Derived: normal balance direction
        case
            when account_type in ('Asset', 'Expense') then 'Debit'
            when account_type in ('Liability', 'Equity', 'Revenue') then 'Credit'
        end as normal_balance_type
    from accounts
)

select * from final
