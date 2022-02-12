{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS ref_data.entity_to_plant_to_division_to_ssbu_mapping ( 
	plant_var_reg_pct    varchar(30)    ,
	raw_product          varchar(75)    ,
	description          varchar(100)    ,
	region               varchar(30)    ,
	division             varchar(30)    ,
	entity               varchar(10)    ,
	super_sbu            varchar(30)    ,
	jan                  decimal(8,4)    ,
	feb                  decimal(8,4)    ,
	mar                  decimal(8,4)    ,
	apr                  decimal(8,4)    ,
	may                  decimal(8,4)    ,
	jun                  decimal(8,4)    ,
	jul                  decimal(8,4)    ,
	aug                  decimal(8,4)    ,
	sep                  decimal(8,4)    ,
	oct                  decimal(8,4)    ,
	nov                  decimal(8,4)    ,
	dec                decimal(8,4)    
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}