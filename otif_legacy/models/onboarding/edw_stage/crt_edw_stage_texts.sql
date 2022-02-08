-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "table_definition": "
        CREATE TABLE IF NOT EXISTS edw_stage.texts
        (
            source_sys VARCHAR(3)   
            ,field VARCHAR(10)   
            ,code VARCHAR(18)   
            ,code_txt VARCHAR(90)   
            ,code_longtxt VARCHAR(120)   
        )
    "
}%}

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}