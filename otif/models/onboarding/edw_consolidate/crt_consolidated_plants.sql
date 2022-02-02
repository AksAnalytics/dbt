-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "table_definition": "
        CREATE TABLE IF NOT EXISTS edw.consolidated.consolidated_plants
        (
            source_sys VARCHAR(3)   
            ,plant VARCHAR(8)   
            ,plant_txt VARCHAR(60)   
            ,plant_country VARCHAR(6)   
            ,region VARCHAR(4)   
        )
    "
}%}

{{ config(materialized = "ephermeral") }}
{% do run_query(table_metadata.table_definition) %}