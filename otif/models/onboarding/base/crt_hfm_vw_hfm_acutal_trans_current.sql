-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "table_definition": "
        CREATE TABLE IF NOT EXISTS bods.hfm_vw_hfm_actual_trans_current
        (
            loaddts TIMESTAMP WITHOUT TIME ZONE   
            ,eventdts VARCHAR(65535)   
            ,rec_src VARCHAR(65535)   
            ,row_sqn BIGINT   
            ,hash_full_record VARCHAR(65535)   
            ,id VARCHAR(65535)   
            ,rectype VARCHAR(65535)   
            ,year VARCHAR(65535)   
            ,period VARCHAR(65535)   
            ,entity VARCHAR(65535)   
            ,acct VARCHAR(65535)   
            ,custom1 VARCHAR(65535)   
            ,custom2 VARCHAR(65535)   
            ,currkey VARCHAR(65535)   
            ,amt NUMERIC(38,10)   
            ,bar_acct VARCHAR(65535)   
            ,bar_function VARCHAR(65535)   
            ,bar_entity VARCHAR(65535)   
            ,bar_shipto VARCHAR(65535)   
            ,bar_product VARCHAR(65535)   
            ,bar_brand VARCHAR(65535)   
            ,bar_custno VARCHAR(65535)   
            ,bar_scenario VARCHAR(65535)   
            ,bar_year VARCHAR(65535)   
            ,bar_period VARCHAR(65535)   
            ,bar_currtype VARCHAR(65535)   
            ,bar_amt NUMERIC(38,10)   
            ,bar_bu VARCHAR(65535)   
            ,group_name VARCHAR(65535)   
            ,runid BIGINT   
            ,loaddatetime VARCHAR(65535)   
            ,period_partition VARCHAR(65535)   
        )
    "
}%}

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}