-- models/vault/satellites/sat_orders_details.sql
{%- set yaml_metadata -%}
source_model: stg_orders
src_pk: ORDER_PK
src_hashdiff: ORDER_HASHDIFF
src_payload:
  - ORDER_DATE
  - ORDER_AMOUNT
src_ldts: LOAD_DATETIME
src_source: RECORD_SOURCE
src_eff: EFFECTIVE_FROM
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(
    src_pk=metadata_dict['src_pk'],
    src_hashdiff=metadata_dict['src_hashdiff'],
    src_payload=metadata_dict['src_payload'],
    src_eff=metadata_dict['src_eff'],
    src_ldts=metadata_dict['src_ldts'],
    src_source=metadata_dict['src_source'],
    source_model=metadata_dict['source_model']
) }}
