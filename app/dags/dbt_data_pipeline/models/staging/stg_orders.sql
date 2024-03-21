-- models/staging/stg_orders.sql
{%- set yaml_metadata -%}
source_model: 'raw_orders'
derived_columns:
  RECORD_SOURCE: '!WEB-ORDERS'
  EFFECTIVE_FROM: 'order_date'
hashed_columns:
  ORDER_PK: "ORDER_ID"
  CUSTOMER_PK: "CUSTOMER_ID"
  LINK_CUSTOMER_ORDER_PK:
      - "CUSTOMER_ID"
      - "ORDER_ID"
  ORDER_HASHDIFF:
    is_hashdiff: true
    columns:
      - "order_date"
      - "order_amount"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

-- Assuming you're using a hypothetical hashing function for illustration
-- Adjust to use actual available functions in your dbt environment
{% set source_model = metadata_dict["source_model"] %}
{% set hashed_columns = metadata_dict["hashed_columns"] %}
{% set derived_columns = metadata_dict["derived_columns"] %}
{% set null_columns = metadata_dict["null_columns"] %}
{% set ranked_columns = metadata_dict["ranked_columns"] %}

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
