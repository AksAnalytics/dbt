-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "table_definition": "
        CREATE TABLE IF NOT EXISTS edw_stage.dim_factory_calendar
        (
            factory_calendar_id VARCHAR(10)   
            ,calendar_date VARCHAR(100)   
            ,working_date VARCHAR(100)   
        )
    "
}%}

{{ config(materialized = "ephermeral") }}
{% do run_query(table_metadata.table_definition) %}