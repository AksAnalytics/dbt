-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "table_definition": "
        CREATE TABLE IF NOT EXISTS edw_stage.bods_hfm_vw_hfm_forecast_trans_current
        (
            year VARCHAR(65535)   
            ,period VARCHAR(65535)   
            ,year_period VARCHAR(65535)   
            ,acct VARCHAR(65535)   
            ,custom1 VARCHAR(65535)   
            ,custom2 VARCHAR(65535)   
            ,amt NUMERIC(38,10)   
        )
    "
}%}

{{ config(materialized = "ephermeral") }}
{% do run_query(table_metadata.table_definition) %}