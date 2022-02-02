-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "table_definition": "
        CREATE TABLE IF NOT EXISTS edw_consolidated.consolidated_customer_hierachy_flatten_c11
        (
            source_sys VARCHAR(65535)   
            ,customer_cons VARCHAR(65535)   
            ,salesdiv_cons VARCHAR(65535)   
            ,salesorg_cons VARCHAR(65535)   
            ,salesdist_cons VARCHAR(65535)   
            ,level_1a VARCHAR(65535)   
            ,level_2a VARCHAR(65535)   
            ,level_1c VARCHAR(65535)   
            ,level_1ap VARCHAR(65535)   
            ,level_2ap VARCHAR(65535)   
            ,level_1cp VARCHAR(65535)   
            ,etl_crte_ts TIMESTAMP WITHOUT TIME ZONE   
            ,etl_updt_ts TIMESTAMP WITHOUT TIME ZONE   
        )
    "
}%}

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}
