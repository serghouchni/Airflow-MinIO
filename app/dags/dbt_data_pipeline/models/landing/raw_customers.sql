SELECT *
FROM {{ source('raw_data', 'customers') }}
