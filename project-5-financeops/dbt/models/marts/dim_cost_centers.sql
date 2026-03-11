-- dim_cost_centers.sql
-- Cost center dimension for departmental reporting.

with cost_centers as (
    select * from {{ ref('stg_cost_centers') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['cost_center_id']) }} as cost_center_key,
        cost_center_id,
        cost_center_name,
        department_group,
        manager_name,
        is_active
    from cost_centers
)

select * from final
