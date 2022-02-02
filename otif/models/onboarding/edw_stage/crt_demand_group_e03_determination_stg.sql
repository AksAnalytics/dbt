-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "table_definition": "
        CREATE TABLE IF NOT EXISTS edw_stage.demand_group_e03_determination_stg
        (
            salesorg_cons VARCHAR(100)   
            ,soldto_cons VARCHAR(100)   
            ,customer_cons VARCHAR(65535)   
            ,hier_typ VARCHAR(65535)   
            ,parent_name VARCHAR(65535)   
            ,parent_cons VARCHAR(65535)   
            ,acct_grp VARCHAR(65535)   
            ,hier_level INTEGER   
            ,demand_group_logic_level INTEGER   
            ,country_cons VARCHAR(65535)   
            ,industkey_cons VARCHAR(65535)   
            ,sales_office_cons VARCHAR(65535)   
            ,prodhl1_e03 VARCHAR(100)   
            ,prodhl2_e03 VARCHAR(100)   
            ,brand_cons VARCHAR(100)   
            ,material_cons VARCHAR(100)   
        )
    "
}%}

{{ config(materialized = "ephermeral") }}
{% do run_query(table_metadata.table_definition) %}