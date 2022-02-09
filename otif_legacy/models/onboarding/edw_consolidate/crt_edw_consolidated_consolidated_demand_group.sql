-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "table_definition": "
        CREATE TABLE IF NOT EXISTS edw_consolidated.consolidated_demand_group
        (
            source_sys VARCHAR(100) NOT NULL  
            ,customer_cons VARCHAR(100) NOT NULL  
            ,salesorg_cons VARCHAR(100)   
            ,material_cons VARCHAR(100)   
            ,demand_group VARCHAR(100)   
            ,etl_crte_user VARCHAR(100)   
            ,etl_crte_ts TIMESTAMP WITHOUT TIME ZONE   
            ,etl_updt_user VARCHAR(100)   
            ,etl_updt_ts TIMESTAMP WITHOUT TIME ZONE   
            ,PRIMARY KEY (source_sys, customer_cons)
        )
        CLUSTER BY (material_cons)
    "
}%}

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}