-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "table_definition": "
        CREATE TABLE IF NOT EXISTS edw_stage.consolidated_customer_hierarchy_stg
        (
            source_sys VARCHAR(3)   
            ,customer VARCHAR(65535)   
            ,kunnr VARCHAR(65535)   
            ,hityp VARCHAR(65535)   
            ,name1 VARCHAR(65535)   
            ,ktokd VARCHAR(65535)   
            ,vkorg VARCHAR(65535)   
            ,hkunnr VARCHAR(65535)   
            ,vtweg VARCHAR(65535)   
            ,spart VARCHAR(65535)   
        )
    "
}%}

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}