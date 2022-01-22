-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "schema_name": "eondryrun",
    "table_name": "bods_hfm_currency_plrate",
    "transient_table": "false",
    "table_definition": "
        (
            year VARCHAR(65535)   
            ,period VARCHAR(65535)   
            ,year_period VARCHAR(65535)   
            ,from_curr VARCHAR(65535)   
            ,to_curr VARCHAR(65535)   
            ,amt NUMERIC(38,10)   
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
