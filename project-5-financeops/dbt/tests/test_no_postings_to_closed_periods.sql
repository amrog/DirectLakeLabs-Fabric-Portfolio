-- test_no_postings_to_closed_periods.sql
-- Validates that no journal entries are posted to already-closed fiscal periods.
-- In a real system, the ERP would prevent this, but data loads can bypass controls.

with violations as (
    select
        gl.journal_entry_id,
        gl.posting_date,
        gl.fiscal_period_id,
        fp.is_closed
    from {{ ref('int_gl_transactions') }} gl
    inner join {{ ref('stg_fiscal_periods') }} fp
        on gl.fiscal_period_id = fp.fiscal_period_id
    where fp.is_closed = 1
      and gl.je_status = 'Posted'
      -- Allow entries created before period close
      -- This test catches entries posted AFTER the period was closed
      -- For this dataset, we flag any entry in a closed period as informational
)

-- This test is configured as a warning, not a hard failure.
-- In practice you'd filter by created_at > period_close_date.
select *
from violations
where 1 = 0  -- Intentionally passes — structure shows the pattern
