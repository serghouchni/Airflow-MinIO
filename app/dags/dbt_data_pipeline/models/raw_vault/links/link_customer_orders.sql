-- models/vault/links/link_customer_order.sql
{%- set yaml_metadata -%}
source_model:
    - stg_orders
src_pk: LINK_CUSTOMER_ORDER_PK
src_fk:
  - CUSTOMER_PK
  - ORDER_PK
src_ldts: LOAD_DATETIME
src_source: RECORD_SOURCE
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.link(
    src_pk=metadata_dict["src_pk"],
    src_fk=metadata_dict["src_fk"],
    src_ldts=metadata_dict["src_ldts"],
    src_source=metadata_dict["src_source"],
    source_model=metadata_dict["source_model"]
) }}
