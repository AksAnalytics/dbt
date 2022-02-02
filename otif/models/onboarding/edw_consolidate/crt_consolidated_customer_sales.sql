-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "table_definition": "
        CREATE TABLE IF NOT EXISTS edw.consolidated.consolidated_customer_sales
        (
            source_sys VARCHAR(65535) NOT NULL  
            ,customer_cons VARCHAR(65535) NOT NULL  
            ,salesdiv_cons VARCHAR(65535) NOT NULL  
            ,salesorg_cons VARCHAR(65535) NOT NULL  
            ,salesdist_cons VARCHAR(65535) NOT NULL  
            ,sales_group_cons VARCHAR(65535)   
            ,sales_office_cons VARCHAR(65535)   
            ,industry_key_cons VARCHAR(65535)   
            ,accgroup_cons VARCHAR(65535)   
            ,custclass_cons VARCHAR(65535)   
            ,country_cons VARCHAR(65535)   
            ,name_cons VARCHAR(65535)   
            ,city_cons VARCHAR(65535)   
            ,region_cons VARCHAR(65535)   
            ,cust_chan_code VARCHAR(65535)   
            ,cust_sub_chan_code VARCHAR(65535)   
            ,cust_chan_txt VARCHAR(65535)   
            ,cust_sub_chan_txt VARCHAR(65535)   
            ,cust_field_zone VARCHAR(65535)   
            ,cust_field_area VARCHAR(65535)   
            ,cust_field_terr VARCHAR(65535)   
            ,cust_type VARCHAR(65535)   
            ,c11demand_group VARCHAR(65535)   
            ,loaddts TIMESTAMP WITHOUT TIME ZONE   
            ,etl_crte_user VARCHAR(65535)   
            ,etl_crte_ts TIMESTAMP WITHOUT TIME ZONE   
            ,etl_updt_user VARCHAR(65535)   
            ,etl_updt_ts TIMESTAMP WITHOUT TIME ZONE   
            ,PRIMARY KEY (source_sys, customer_cons, salesdiv_cons, salesorg_cons, salesdist_cons)
        )
        CLUSTER BY (customer_cons)
    "
}%}

{{ config(materialized = "ephermeral") }}
{% do run_query(table_metadata.table_definition) %}