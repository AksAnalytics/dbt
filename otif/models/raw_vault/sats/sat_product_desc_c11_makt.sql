{{ config(
    materialized='vault_insert_by_rank',
    rank_column='DBTVAULT_RANK',
    rank_source_models = 'v_stg_c11_product_description_makt'
    )
}}

{%- set source_model = "v_stg_c11_product_description_makt" -%}
{%- set src_pk = "PRODUCT_DESC_HK" -%}
{%- set src_hashdiff = "PRODUCT_DESC_HASHDIFF" -%}
{% set columns_query %}
select column_name from "{{database}}".information_schema.columns where table_name = UPPER('{{source_model}}') and table_catalog = 'DEV_RAW' and table_schema = '{{schema}}'
and column_name not like  ('%HASHDIFF') and column_name not like '%HK' and column_name not like 'LOAD_%D%' and column_name <> 'RECORD_SOURCE' 
and column_name <> 'QRY_NBR' and column_name <> 'DBTVAULT_RANK' and column_name <> 'CASPIAN_CHANGEINDICATOR_OPERATION' and column_name <> 'HEADER_CHANGE_SEQ'
and column_name <> 'HEADER_CHANGE_SEQ' and column_name <> 'HEADER_CHANGE_OPER' and column_name <> 'HEADER_CHANGE_MASK' and column_name <> 'HEADER_STREAM_POSITION'
and column_name <> 'HEADER_OPERATION' and column_name <> 'HEADER_TRANSACTION_ID' and column_name <> 'HEADER_TIMESTAMP'
{% endset %}


{% set results = run_query(columns_query) %}
{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}


{%- set src_payload = results_list -%}
{%- set src_eff = "LOAD_EFF_DT" -%}
{%- set src_ldts = "LOAD_DT" -%}
{%- set src_source = "RECORD_SOURCE" -%}




                WITH sats AS (
{{ dbtvault.sat(src_pk=src_pk, src_hashdiff=src_hashdiff,
                src_payload=src_payload, src_eff=src_eff,
                src_ldts=src_ldts, src_source=src_source,
                source_model=source_model) }}
)

SELECT *
FROM sats
{% if target_exists() %}
  -- this filter prevents select from target during initial load
  WHERE NOT EXISTS
    (SELECT 1 FROM {{ this }} tgt
    WHERE sats.{{src_pk}} = tgt.{{src_pk}} 
    AND sats.LOAD_EFF_DT <= tgt.LOAD_EFF_DT)
{% endif %}