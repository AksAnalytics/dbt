{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS dw.dim_dataprocessing_rule ( 
	data_processing_ruleid integer  NOT NULL  ,
	soldtoflag           varchar(1)    ,
	skuflag              varchar(1)    ,
	barcustflag          varchar(1)    ,
	barproductflag       varchar(1)    ,
	barbrandflag         varchar(1)    ,
	dataprocessing_group varchar(200)    ,
	dataprocessing_rule_description varchar(400)    ,
	dataprocessing_rule_steps varchar(500)    ,
	audit_loadts         date  NOT NULL  
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}