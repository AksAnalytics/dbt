-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "table_definition": "
        CREATE TABLE IF NOT EXISTS edw.consolidated.consolidated_customer_hierarchy_flatten_e03
        (
            source_sys VARCHAR(65535)   
            ,customer_cons VARCHAR(65535)   
            ,salesdiv_cons VARCHAR(65535)   
            ,salesorg_cons VARCHAR(65535)   
            ,salesdist_cons VARCHAR(65535)   
            ,level_1b VARCHAR(65535)   
            ,level_2b VARCHAR(65535)   
            ,level_3b VARCHAR(65535)   
            ,level_4b VARCHAR(65535)   
            ,level_5b VARCHAR(65535)   
            ,level_6b VARCHAR(65535)   
            ,level_1d VARCHAR(65535)   
            ,level_2d VARCHAR(65535)   
            ,level_3d VARCHAR(65535)   
            ,level_4d VARCHAR(65535)   
            ,level_1bp VARCHAR(65535)   
            ,level_2bp VARCHAR(65535)   
            ,level_3bp VARCHAR(65535)   
            ,level_4bp VARCHAR(65535)   
            ,level_5bp VARCHAR(65535)   
            ,level_6bp VARCHAR(65535)   
            ,level_1dp VARCHAR(65535)   
            ,level_2dp VARCHAR(65535)   
            ,level_3dp VARCHAR(65535)   
            ,level_4dp VARCHAR(65535)   
            ,etl_crte_ts TIMESTAMP WITHOUT TIME ZONE   
            ,etl_updt_ts TIMESTAMP WITHOUT TIME ZONE   
        )
    "
}%}

{{ config(materialized = "ephermeral") }}
{% do run_query(table_metadata.table_definition) %}