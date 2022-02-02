-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "schema_name": "eondryrun",
    "table_name": "hfm_vw_hfm_forecast_trans_current",
    "transient_table": "false",
    "table_definition": "
        (
            loaddts TIMESTAMP WITHOUT TIME ZONE   
            ,eventdts VARCHAR(65535)   
            ,rec_src VARCHAR(65535)   
            ,row_sqn BIGINT   
            ,hash_full_record VARCHAR(65535)   
            ,id NUMERIC(38,10)   
            ,rectype VARCHAR(65535)   
            ,year VARCHAR(65535)   
            ,period VARCHAR(65535)   
            ,entity VARCHAR(65535)   
            ,acct VARCHAR(65535)   
            ,custom1 VARCHAR(65535)   
            ,custom2 VARCHAR(65535)   
            ,currkey VARCHAR(65535)   
            ,amt NUMERIC(38,10)   
            ,group_1 VARCHAR(65535)   
            ,bar_acct VARCHAR(65535)   
            ,bar_function VARCHAR(65535)   
            ,bar_entity VARCHAR(65535)   
            ,bar_shipto VARCHAR(65535)   
            ,bar_product VARCHAR(65535)   
            ,bar_brand VARCHAR(65535)   
            ,bar_custno VARCHAR(65535)   
            ,bar_scenario VARCHAR(65535)   
            ,bar_year VARCHAR(65535)   
            ,bar_period VARCHAR(65535)   
            ,bar_currtype VARCHAR(65535)   
            ,bar_amt NUMERIC(38,10)   
            ,bar_bu VARCHAR(65535)   
            ,runid VARCHAR(65535)   
            ,loaddatetime VARCHAR(65535)   
            ,period_part VARCHAR(65535)   
        )
    ",
    "full_refresh_ddl_statements": [

    ]
}%}

{%- set create_table_hook = create_data_mart_table(table_metadata) -%}

{{ config(
    materialized = "table",
    pre_hook = create_table_hook,
    schema = table_metadata.schema_name
)}}


WITH source AS (
    SELECT * FROM {{ table_metadata.table_name }}
),

renamed AS (
    SELECT 
      *
    FROM source
)

SELECT * FROM renamed
