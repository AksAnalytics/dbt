-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "table_definition": "
       CREATE TABLE IF NOT EXISTS edw_stage.c11_default_demand_group
       (
            z_name VARCHAR(500)   
            ,z_key VARCHAR(500)   
            ,z_var VARCHAR(500)   
            ,z_comment VARCHAR(500)   
        )
    "
}%}

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}
