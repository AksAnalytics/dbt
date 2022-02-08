{{ config(materialized='incremental')    }}

{%- set yaml_metadata -%}
source_model: 
    - 'v_stg_c11_delivery_item_lips'
    - 'v_stg_e03_delivery_item_lips'
src_pk: 'DELIVERY_ITEM_HK'
src_fk: 
    - 'DELIVERY_HK'
src_ldts: 'LOAD_DT'
src_source: 'RECORD_SOURCE'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ dbtvault.link(src_pk=metadata_dict["src_pk"],
                 src_fk=metadata_dict["src_fk"], 
                 src_ldts=metadata_dict["src_ldts"],
                 src_source=metadata_dict["src_source"], 
                 source_model=metadata_dict["source_model"]) }}
