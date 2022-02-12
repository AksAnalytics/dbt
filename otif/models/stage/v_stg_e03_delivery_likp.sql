{%- set yaml_metadata -%}
source_model: 'v_src_e03_delivery_likp'
ranked_columns:
  DBTVAULT_RANK:
    partition_by: DELIVERY_HK
    order_by:
      - EVENTDTS
      - LOADDTS
derived_columns:
  RECORD_SOURCE: '!SAPE03_LIKP'
  DV_BKEY_CODE: '!E03'
  LOAD_EFF_DT: "EVENTDTS"
  LOAD_DT: "LOADDTS"
hashed_columns:
  DELIVERY_HK: 
    - 'MANDT'
    - 'VBELN'
    - 'DV_BKEY_CODE'
  DELIVERY_HASHDIFF:
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
    materialized='table',
    cluster_by=["VBELN"]
    )
}}

{{ dbtvault.stage(include_source_columns=true,
                  source_model=source_model,
                  derived_columns=derived_columns,
                  hashed_columns=hashed_columns,
                  ranked_columns=ranked_columns) }}