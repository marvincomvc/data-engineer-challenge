{#
  Deterministically parse a monetary string into DECIMAL(18,2), tolerating the
  dirty-data patterns present in the PaySettler feed:

    "152.30"          US decimal                       -> 152.30
    "152,30"          pt-BR comma decimal              -> 152.30
    "R$ 1.234,56"     pt-BR w/ symbol + thousands sep  -> 1234.56
    "R$ 873,16"       pt-BR w/ symbol                  -> 873.16

  Strategy: strip everything but digits / separators / sign, then decide the
  decimal separator by which separators are present. Whatever is STILL
  unparseable after this returns NULL, so the not_null integrity gate blocks
  the run on genuinely corrupt money rather than silently coercing it.
#}
{% macro clean_amount(col) %}
    try_cast(
        case
            when {{ col }} is null then null
            -- both separators -> pt-BR: '.' thousands, ',' decimal
            when strpos({{ col }}, '.') > 0 and strpos({{ col }}, ',') > 0
                then replace(replace(regexp_replace({{ col }}, '[^0-9.,-]', '', 'g'), '.', ''), ',', '.')
            -- only ',' -> comma decimal
            when strpos({{ col }}, ',') > 0
                then replace(regexp_replace({{ col }}, '[^0-9,-]', '', 'g'), ',', '.')
            -- only '.' or plain integer -> US decimal
            else regexp_replace({{ col }}, '[^0-9.-]', '', 'g')
        end
    as decimal(18, 2))
{% endmacro %}
