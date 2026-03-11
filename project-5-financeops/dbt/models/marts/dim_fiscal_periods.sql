-- dim_fiscal_periods.sql
-- Fiscal calendar dimension with period metadata.

with periods as (
    select * from {{ ref('stg_fiscal_periods') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['fiscal_period_id']) }} as fiscal_period_key,
        fiscal_period_id,
        fiscal_year,
        fiscal_month,
        fiscal_quarter,
        period_start_date,
        period_end_date,
        is_closed,
        -- Derived: period display name
        concat('FY', cast(fiscal_year as varchar(4)), ' ',
               fiscal_quarter, ' - M',
               cast(fiscal_month as varchar(2))) as period_display_name
    from periods
)

select * from final
