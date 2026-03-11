-- test_je_balanced.sql
-- Validates that every posted journal entry balances (debits = credits).
-- Returns rows that are OUT of balance — any result is a failure.
-- Tolerance: $0.05 for rounding (real-world ERP systems produce penny differences).

with je_totals as (
    select
        journal_entry_id,
        sum(debit_amount)  as total_debits,
        sum(credit_amount) as total_credits,
        abs(sum(debit_amount) - sum(credit_amount)) as imbalance
    from {{ ref('stg_journal_entry_lines') }}
    group by journal_entry_id
)

select *
from je_totals
where imbalance > 0.05
