-- Reconciliation category dimension: stable enumeration + business meaning,
-- so consumer queries can group/filter on human-readable semantics.
select * from (
    values
        ('MATCHED',                'Matched',                'Present both sides, within tolerance',   false),
        ('MISMATCHED',             'Mismatched',             'Present both sides, amount differs',      true),
        ('UNRECONCILED_PROCESSOR', 'Unreconciled (processor)', 'In processor file, no internal record', true),
        ('UNRECONCILED_INTERNAL',  'Unreconciled (internal)',  'Internal record, absent from processor', true)
) as t(category, category_label, description, needs_investigation)
