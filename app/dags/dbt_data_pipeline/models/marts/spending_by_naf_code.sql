{{ config(
    materialized='incremental',
    schema='marts',
    unique_key='date||naf_code'
) }}

SELECT
    t.date,
    d.naf_code,
    SUM(CASE WHEN t.status = 'CAPTURED' THEN t.amount ELSE 0 END) -
    SUM(CASE WHEN t.status = 'REFUNDED' THEN t.amount ELSE 0 END) AS spent
FROM
    {{ source('curated', 'transactions') }} t
JOIN
    {{ source('curated', 'naf_details') }} d ON t.siret = d.siret
WHERE
    {% if is_incremental() %}
       t.date = '{{ var("target_date") }}'
    {% else %}
        1=1
    {% endif %}
GROUP BY
    t.date,
    d.naf_code
ORDER BY
    t.date,
    spent
