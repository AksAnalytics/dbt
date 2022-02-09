{{ config(materialized='incremental')    }}

{%- set yaml_metadata -%}
source_model: 
    - 'v_stg_c11_customer_sales_knvv'
src_pk: 'CUSTOMER_SALES_DISTRIBUTION_DIVISION_HK'
src_fk: 
    - 'CUSTOMER_HK'
    - 'SALES_ORG_HK'  
    - 'DISTRIBUTION_CHANNEL_HK' 
src_ldts: 'LOAD_DT'
src_source: 'RECORD_SOURCE'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ dbtvault.link(src_pk=metadata_dict["src_pk"],
                 src_fk=metadata_dict["src_fk"], 
                 src_ldts=metadata_dict["src_ldts"],
                 src_source=metadata_dict["src_source"], 
                 source_model=metadata_dict["source_model"]) }}

