{{ config(
    materialized = 'view',
    schema = 'global_pl'
)}}

WITH t AS (

    SELECT * FROM global_pl.pl_trans_fact
),

ent AS (

    SELECT * FROM global_pl.bar_entity_attr
),

acc AS (

    SELECT * FROM global_pl.bar_acct_attr
),

cust AS (

    SELECT * FROM global_pl.bar_customer_attr
),

fnc AS (

    SELECT * FROM global_pl.bar_funct_attr
),

prd AS (

    SELECT * FROM global_pl.bar_product_attr
),

cm AS (

    SELECT * FROM global_pl.customer_master
),

mm AS (

    SELECT * FROM global_pl.customer_master
),

final AS (
    SELECT 
    t.bar_account, 
    t.bar_amt_lc, 
    t.bar_brand, 
    t.bar_bu, 
    t.bar_currtype, 
    t.bar_customer, 
    t.bar_entity, 
    t.bar_function, 
    t.bar_period, 
    t.bar_product, 
    t.bar_scenario, 
    t.bar_shipto, 
    t.bar_year, 
    t.bar_fiscal_period, 
    t.erp_account, 
    t.erp_brand_code, 
    t.erp_business_area, 
    t.erp_company_code, 
    t.erp_cost_center, 
    t.erp_doc_type, 
    t.erp_doc_line_num, 
    t.erp_doc_num, 
    t.erp_document_text, 
    t.erp_vendor, 
    t.erp_material,
    t.erp_customer_parent, 
    t.erp_posting_date, 
    t.erp_quantity, 
    t.erp_quantity_uom, 
    t.erp_ref_doc_type, 
    t.erp_ref_doc_line_num, 
    t.erp_ref_doc_num, 
    t.erp_profit_center, 
    t.erp_sales_group, 
    t.erp_sales_office, 
    t.erp_customer_ship_to, 
    t.erp_customer_sold_to, 
    t.erp_plant, 
    t.bar_bods_loaddatetime, 
    t.erp_chartaccts, 
    t.bar_bods_record_id, 
    t.erp_source, 
    t.bar_s_entity_currency, 
    t.bar_s_curr_rate_actual, 
    t.bar_amt_usd, 
    acc.bar_account_desc, 
    acc.bar_acct_type_lvl1, 
    acc.bar_acct_type_lvl2, 
    acc.bar_acct_type_lvl3, 
    acc.bar_acct_type_lvl4, 
    acc.indirect_flag, 
    acc.flipsign, 
    cust.bar_customer_desc, 
    cust.bar_customer_lvl1, 
    cust.bar_customer_lvl2, 
    cust.bar_customer_lvl3, 
    cust.bar_customer_lvl4, 
    ent.bar_entity_desc, 
    ent.bar_entity_currency, 
    ent.bar_entity_lvl1, 
    ent.bar_entity_lvl2, 
    ent.bar_entity_lvl3, 
    ent.bar_entity_lvl4, 
    ent.bar_entity_region, 
    fnc.bar_function_grp, 
    fnc.functiontype, 
    prd.bar_product_desc, 
    prd.bar_product_lvl1, 
    prd.bar_product_lvl2, 
    prd.bar_product_lvl3, 
    prd.bar_product_lvl4, 
    prd.bar_product_lvl5, 
    cm.erp_customer_number, 
    cm.erp_customer_address_code, 
    cm.erp_customer_industry_code_1, 
    cm.erp_customer_industry_code_2, 
    cm.erp_customer_industry_code_3, 
    cm.erp_customer_industry_code_4, 
    cm.erp_customer_industry_code_5, 
    cm.erp_customer_industry_key, 
    cm.erp_customer_city_code, 
    cm.erp_customer_county_code, 
    cm.erp_customer_country, 
    cm.erp_customer_name, 
    cm.erp_customer_city, 
    cm.erp_customer_district, 
    cm.erp_customer_po_box, 
    cm.erp_customer_po_box_postal_code, 
    cm.erp_customer_postal_code, 
    cm.erp_customer_region, 
    cm.erp_customer_regional_market, 
    cm.erp_source AS cust_mstr_erp_source, 
    cm.erp_customer_address, 
    mm.erp_material_number, 
    mm.erp_material_description, 
    mm.erp_material_category, 
    mm.erp_container_requirements, 
    mm.erp_generic_material_with_logistical_variants, 
    mm.erp_old_material_number, 
    mm.erp_brand, mm.erp_width, 
    mm.erp_gross_weight, 
    mm.erp_purchase_order_uom, 
    mm.erp_source_of_supply, 
    mm.erp_procurement_rule, 
    mm.erp_cad_indicator, 
    mm.erp_quality_conversion_method, 
    mm.erp_material_completion_level, 
    mm.erp_internal_object_number, 
    mm.erp_valid_from_date, 
    mm.erp_ean_upc, 
    mm.erp_purhcasing_value_key, 
    mm.erp_unit_of_weight_packaging, 
    mm.erp_allowed_packaging_weight, 
    mm.erp_volume_unit, 
    mm.erp_allowed_packaging_volume, 
    mm.erp_weight_unit, 
    mm.erp_size_dimensions, 
    mm.erp_height, 
    mm.erp_material_group, 
    mm.erp_industry_sector, 
    mm.erp_material_type, 
    mm.erp_net_weight, 
    mm.erp_product_hierarchy, 
    mm.erp_division, 
    mm.erp_hazardous_material_number, 
    mm.erp_transportation_group, 
    mm.erp_packaging_material_type, 
    mm.erp_global_product_hierarchy,
    mm.erp_source AS matr_mstr_erp_source
    FROM t
    JOIN ent 
      ON upper(t.bar_entity::text) = upper(ent.bar_entity::text) 
     AND ent.bar_entity_lvl1::text <> 'NonOp'::text
    FULL JOIN acc 
      ON upper(t.bar_account::text) = upper(acc.bar_account::text)
    FULL JOIN cust  
      ON upper(t.bar_customer::text) = upper(cust.bar_customer::text)
    FULL JOIN fnc 
      ON upper(t.bar_function::text) = upper(fnc.bar_function::text)
    FULL JOIN prd 
      ON upper(t.bar_product::text) = upper(prd.bar_product::text)
    FULL JOIN cm 
      ON upper(t.erp_customer_sold_to::text) = upper(cm.erp_customer_number::text) 
     AND t.erp_source::text = cm.erp_source::text
    FULL JOIN mm 
      ON upper(t.erp_material::text) = upper(mm.erp_material_number::text) 
     AND t.erp_source::text = mm.erp_source::text
)

SELECT * FROM final