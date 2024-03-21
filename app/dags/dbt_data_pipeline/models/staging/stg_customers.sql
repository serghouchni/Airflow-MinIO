-- models/staging/stg_customers.sql
{%- set yaml_metadata -%}
source_model: 'raw_customers'
derived_columns:
  RECORD_SOURCE: '!WEB-CUSTOMERS'
  EFFECTIVE_FROM: TO_TIMESTAMP('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S.%f") }}', 'YYYY-MM-DD HH24:MI:SS.US')
hashed_columns:
  CUSTOMER_PK: 'CUSTOMER_ID'
  CUSTOMER_HASHDIFF:
    is_hashdiff: true
    columns:
      - 'first_name'
      - 'last_name'
      - 'email'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set source_model = metadata_dict['source_model'] %}

{% set derived_columns = metadata_dict['derived_columns'] %}

{% set hashed_columns = metadata_dict['hashed_columns'] %}

WITH staging AS (
{{ automate_dv.stage(include_source_columns=true,
                     source_model=source_model,
                     derived_columns=derived_columns,
                     hashed_columns=hashed_columns,
                     ranked_columns=none) }}
)

SELECT *,
       CURRENT_TIMESTAMP AS load_datetime
FROM staging