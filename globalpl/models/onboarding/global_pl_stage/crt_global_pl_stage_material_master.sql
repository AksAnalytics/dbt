{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS global_pl_stage.material_master
(
	erp_material_number VARCHAR(500) NOT NULL  
	,erp_material_description VARCHAR(500)   
	,erp_material_category VARCHAR(500)   
	,erp_container_requirements VARCHAR(500)   
	,erp_generic_material_with_logistical_variants VARCHAR(500)   
	,erp_old_material_number VARCHAR(500)   
	,erp_brand VARCHAR(500)   
	,erp_width NUMERIC(38,10)   
	,erp_gross_weight NUMERIC(38,10)   
	,erp_purchase_order_uom VARCHAR(500)   
	,erp_source_of_supply VARCHAR(500)   
	,erp_procurement_rule VARCHAR(500)   
	,erp_cad_indicator VARCHAR(500)   
	,erp_quality_conversion_method VARCHAR(500)   
	,erp_material_completion_level NUMERIC(38,10)   
	,erp_internal_object_number NUMERIC(38,10)   
	,erp_valid_from_date DATE   
	,erp_ean_upc VARCHAR(500)   
	,erp_purhcasing_value_key VARCHAR(500)   
	,erp_unit_of_weight_packaging VARCHAR(500)   
	,erp_allowed_packaging_weight NUMERIC(38,10)   
	,erp_volume_unit VARCHAR(500)   
	,erp_allowed_packaging_volume NUMERIC(38,10)   
	,erp_weight_unit VARCHAR(500)   
	,erp_size_dimensions VARCHAR(500)   
	,erp_height NUMERIC(38,10)   
	,erp_material_group VARCHAR(500)   
	,erp_industry_sector VARCHAR(500)   
	,erp_material_type VARCHAR(500)   
	,erp_net_weight NUMERIC(38,10)   
	,erp_product_hierarchy VARCHAR(500)   
	,erp_division VARCHAR(500)   
	,erp_hazardous_material_number VARCHAR(500)   
	,erp_transportation_group VARCHAR(500)   
	,erp_packaging_material_type VARCHAR(500)   
	,erp_global_product_hierarchy VARCHAR(500)   
	,erp_source VARCHAR(500) NOT NULL  
	,etl_crte_user VARCHAR(500)   
	,etl_crte_ts VARCHAR(500)   
	,hive_loaddatetime VARCHAR(500)   
	,PRIMARY KEY (erp_material_number, erp_source)
) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}