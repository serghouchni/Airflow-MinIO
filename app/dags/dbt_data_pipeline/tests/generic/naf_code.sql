{% test naf_code(model, column_name) %}

with source as (

    select
        naf_code
    from
        {{ ref('spending_by_naf_code') }}
)

select
    naf_code
from
    source
where
    naf_code !~ '^[0-9]{2}\.[0-9]{2}[A-Z]$'

{% endtest %}

