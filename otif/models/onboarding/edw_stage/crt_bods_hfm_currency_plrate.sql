-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "table_definition": "
        CREATE TABLE IF NOT EXISTS edw_stage.bods_hfm_currency_plrate
        (
            year VARCHAR(65535)   
            ,period VARCHAR(65535)   
            ,year_period VARCHAR(65535)   
            ,from_curr VARCHAR(65535)   
            ,to_curr VARCHAR(65535)   
            ,amt NUMERIC(38,10)   
        )
    "
}%}

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}