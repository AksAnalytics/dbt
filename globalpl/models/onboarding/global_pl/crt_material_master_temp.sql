{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS global_pl.material_master_temp
(
	erp_material_number VARCHAR(100)   
	,erp_material_description VARCHAR(100)   
	,erp_material_category VARCHAR(100)   
	,erp_container_requirements VARCHAR(100)   
	,erp_generic_material_with_logistical_variants VARCHAR(100)   
	,erp_old_material_number VARCHAR(100)   
	,erp_brand VARCHAR(100)   
	,erp_width NUMERIC(38,10)   
	,erp_gross_weight NUMERIC(38,10)   
	,erp_purchase_order_uom VARCHAR(100)   
	,erp_source_of_supply VARCHAR(100)   
	,erp_procurement_rule VARCHAR(100)   
	,erp_cad_indicator VARCHAR(100)   
	,erp_quality_conversion_method VARCHAR(100)   
	,erp_material_completion_level NUMERIC(38,10)   
	,erp_internal_object_number NUMERIC(38,10)   
	,erp_valid_from_date DATE   
	,erp_ean_upc VARCHAR(100)   
	,erp_purhcasing_value_key VARCHAR(100)   
	,erp_unit_of_weight_packaging VARCHAR(100)   
	,erp_allowed_packaging_weight NUMERIC(38,10)   
	,erp_volume_unit VARCHAR(100)   
	,erp_allowed_packaging_volume NUMERIC(38,10)   
	,erp_weight_unit VARCHAR(100)   
	,erp_size_dimensions VARCHAR(100)   
	,erp_height NUMERIC(38,10)   
	,erp_material_group VARCHAR(100)   
	,erp_industry_sector VARCHAR(100)   
	,erp_material_type VARCHAR(100)   
	,erp_net_weight NUMERIC(38,10)   
	,erp_product_hierarchy VARCHAR(100)   
	,erp_division VARCHAR(100)   
	,erp_hazardous_material_number VARCHAR(100)   
	,erp_transportation_group VARCHAR(100)   
	,erp_packaging_material_type VARCHAR(100)   
	,erp_global_product_hierarchy VARCHAR(100)   
	,erp_source VARCHAR(100)   
	,etl_crte_user VARCHAR(50)   
	,etl_crte_ts VARCHAR(50)   
) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}