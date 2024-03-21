SELECT *
FROM {{ source('raw_data', 'orders') }}
