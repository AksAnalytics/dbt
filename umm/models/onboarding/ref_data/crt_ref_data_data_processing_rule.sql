{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS ref_data.data_processing_rule ( 
	data_processing_ruleid integer  NOT NULL  ,
	soldtoflag           varchar(1)    ,
	skuflag              varchar(1)    ,
	barcustflag          varchar(1)    ,
	barproductflag       varchar(1)    ,
	barbrandflag         varchar(1)    ,
	bar_acct             varchar(10)    ,
	data_source          varchar(10)    ,
	dataprocessing_group varchar(200)    ,
	dataprocessing_rule_description varchar(400)    ,
	dataprocessing_rule_steps varchar(500)    ,
	dataprocessing_hash  varchar(40)    
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}