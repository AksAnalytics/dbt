{%- set yaml_metadata -%}
source_model: 'v_src_c11_customer_hierarchy_knvh'
ranked_columns:
  DBTVAULT_RANK:
    partition_by: CUSTOMER_HIER_HK
    order_by:
      - EVENTDTS
      - LOADDTS
derived_columns:
  RECORD_SOURCE: '!SAPC11_KNVH'
  DV_BKEY_CODE: '!C11'
  LOAD_EFF_DT: "EVENTDTS"
  LOAD_DT: "LOADDTS"
hashed_columns:
  CUSTOMER_HIER_HK: 
    - 'MANDT'
    - 'HITYP'
    - 'KUNNR'
    - 'VKORG'
    - 'VTWEG'
    - 'SPART'
    - 'DATAB'
    - 'DV_BKEY_CODE'
  CUSTOMER_HIER_TYPES_HK:
    - 'MANDT'
    - 'HITYP'
    - 'DV_BKEY_CODE'
  CUSTOMER_HK:
    - 'MANDT'
    - 'KUNNR'
    - 'DV_BKEY_CODE'
  DIVISION_HK:
    - 'MANDT'
    - 'SPART'
    - 'DV_BKEY_CODE'
  SALES_ORG_HK:
    - 'MANDT'
    - 'VKORG'
    - 'DV_BKEY_CODE'
  DISTRIBUTION_CHANNEL_HK:
    - 'MANDT'
    - 'VTWEG'
    - 'DV_BKEY_CODE'
  CUSTOMER_HIER_HK_HASHDIFF:
    is_hashdiff: true
    exclude_columns: true
    columns:
      - 'LOAD_EFF_DT'
      - 'LOAD_DT'
      - 'QRY_NBR'
      - 'LOADDTS'
      - 'EVENTDTS'
      - 'CASPIAN_CHANGEINDICATOR_OPERATION'
      - 'HEADER_CHANGE_SEQ'
      - 'HEADER_CHANGE_SEQ'
      - 'HEADER_CHANGE_OPER'
      - 'HEADER_CHANGE_MASK'
      - 'HEADER_STREAM_POSITION'
      - 'HEADER_OPERATION'
      - 'HEADER_TRANSACTION_ID'
      - 'HEADER_TIMESTAMP'
 {%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set source_model = metadata_dict['source_model'] %}

{% set derived_columns = metadata_dict['derived_columns'] %}

{% set hashed_columns = metadata_dict['hashed_columns'] %}

{% set ranked_columns = metadata_dict['ranked_columns'] %}

{{ config(
    materialized='table'
    )
}}

{{ dbtvault.stage(include_source_columns=true,
                  source_model=source_model,
                  derived_columns=derived_columns,
                  hashed_columns=hashed_columns,
                  ranked_columns=ranked_columns) }}
