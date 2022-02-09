-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "table_definition": "
        CREATE TABLE IF NOT EXISTS edw_stage.dim_calendar
        (
            dy_id VARCHAR(256)   
            ,dy_dte TIMESTAMP WITHOUT TIME ZONE   
            ,absolute_dy_nbr VARCHAR(256)   
            ,absolute_wk_nbr VARCHAR(256)   
            ,absolute_mth_nbr VARCHAR(256)   
            ,absolute_qtr_nbr INTEGER   
            ,dy_in_wk_nbr INTEGER   
            ,dy_in_wk_name VARCHAR(256)   
            ,julian_dy_nbr INTEGER   
            ,clndr_wk_nbr INTEGER   
            ,clndr_wk_id INTEGER   
            ,clndr_mth_nbr INTEGER   
            ,clndr_mth_id VARCHAR(256)   
            ,clndr_mth_name VARCHAR(256)   
            ,clndr_qtr_nbr INTEGER   
            ,clndr_qtr_id VARCHAR(256)   
            ,clndr_qtr_name VARCHAR(256)   
            ,clndr_yr_id VARCHAR(256)   
            ,clndr_dy_in_mth_nbr INTEGER   
            ,is_first_dy_in_clndr_mth_flag VARCHAR(256)   
            ,is_last_dy_in_clndr_mth_flag VARCHAR(256)   
            ,wk_begin_dte TIMESTAMP WITHOUT TIME ZONE   
            ,wk_end_dte TIMESTAMP WITHOUT TIME ZONE   
            ,fmth_begin_dte TIMESTAMP WITHOUT TIME ZONE   
            ,fmth_end_dte TIMESTAMP WITHOUT TIME ZONE   
            ,fqtr_begin_dte TIMESTAMP WITHOUT TIME ZONE   
            ,fqtr_end_dte TIMESTAMP WITHOUT TIME ZONE   
            ,fyr_begin_dte TIMESTAMP WITHOUT TIME ZONE   
            ,fyr_end_dte TIMESTAMP WITHOUT TIME ZONE   
            ,fwk_nbr INTEGER   
            ,fwk_id VARCHAR(256)   
            ,fwk_cd VARCHAR(256)   
            ,fmth_nbr INTEGER   
            ,fmth_id VARCHAR(256)   
            ,fmth_cd VARCHAR(256)   
            ,fmth_name VARCHAR(256)   
            ,fmth_short_name VARCHAR(256)   
            ,fqtr_nbr INTEGER   
            ,fqtr_id VARCHAR(256)   
            ,fqtr_cd VARCHAR(256)   
            ,fqtr_name VARCHAR(256)   
            ,fyr_id VARCHAR(256)   
            ,fdy_in_mth_nbr INTEGER   
            ,fscl_days_remaining_in_mth INTEGER   
            ,fdy_in_qtr_nbr INTEGER   
            ,fscl_days_remaining_in_qtr INTEGER   
            ,fdy_in_yr_nbr INTEGER   
            ,fscl_days_remaining_in_yr INTEGER   
            ,fwk_in_mth_nbr INTEGER   
            ,fwk_in_qtr INTEGER   
            ,is_wk_dy_flag VARCHAR(256)   
            ,is_weekend_flag VARCHAR(256)   
            ,is_first_dy_of_fwk_flag VARCHAR(256)   
            ,is_last_dy_of_fwk_flag VARCHAR(256)   
            ,is_first_dy_of_fmth_flag VARCHAR(256)   
            ,is_last_dy_of_fmth_flag VARCHAR(256)   
            ,is_first_dy_of_fqtr_flag VARCHAR(256)   
            ,is_last_dy_of_fqtr_flag VARCHAR(256)   
            ,is_first_dy_of_fyr_flag VARCHAR(256)   
            ,is_last_dy_of_fyr_flag VARCHAR(256)   
            ,season_name VARCHAR(256)   
            ,holiday_name VARCHAR(256)   
            ,holiday_season_name VARCHAR(256)   
            ,holiday_observed_name VARCHAR(256)   
            ,special_event_name VARCHAR(256)   
            ,etl_batch_id INTEGER   
        )
    "
}%}

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}