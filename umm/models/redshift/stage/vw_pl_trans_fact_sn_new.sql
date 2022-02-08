{{config(
    materialized='view',
    schema = 'public'
) }}


WITH base_pl_trans_fact_sn_new AS (
    
    SELECT * FROM pl_trans_fact_sn_new
),

final AS (
    SELECT 
    bar_account, 
    bar_amt_lc, 
    bar_brand, 
    bar_bu, 
    bar_currtype, 
    bar_customer, 
    bar_entity, 
    bar_function, 
    bar_period, 
    bar_product, 
    bar_scenario, 
    bar_shipto, 
    bar_year, 
    bar_fiscal_period, 
    erp_account, 
    erp_brand_code, 
    erp_business_area, 
    erp_company_code, 
    erp_cost_center, 
    erp_doc_type, 
    erp_doc_line_num, 
    erp_doc_num, 
    erp_document_text, 
    erp_vendor, 
    erp_material, 
    erp_customer_parent, 
    erp_posting_date, 
    erp_quantity, 
    erp_quantity_uom, 
    erp_ref_doc_type, 
    erp_ref_doc_line_num, 
    erp_ref_doc_num, 
    erp_profit_center, 
    erp_sales_group, 
    erp_sales_office, 
    erp_customer_ship_to, 
    erp_customer_sold_to, 
    erp_plant, 
    bar_bods_loaddatetime, 
    erp_chartaccts, 
    bar_bods_record_id, 
    erp_source, 
    bar_s_entity_currency, 
    bar_s_curr_rate_actual, 
    bar_amt_usd, 
    etl_crte_user, 
    etl_crte_ts
    FROM base_pl_trans_fact_sn_new
)

SELECT * FROM final