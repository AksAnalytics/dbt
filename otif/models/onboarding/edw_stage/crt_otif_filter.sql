-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "table_definition": "
        CREATE TABLE IF NOT EXISTS edw_stage.otif_filter
        (
            source_sys VARCHAR(100) NOT NULL  
            ,field VARCHAR(100) NOT NULL  
            ,value VARCHAR(100) NOT NULL  
            ,PRIMARY KEY (source_sys, field, value)
        )
    "
}%}

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}