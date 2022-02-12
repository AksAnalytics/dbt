{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS stage.marm_current_08272021 ( 
	loaddts              timestamp    ,
	eventdts             varchar(65535)    ,
	rec_src              varchar(65535)    ,
	row_sqn              bigint    ,
	hash_full_record     varchar(65535)    ,
	headchar             varchar(65535)    ,
	mandt                varchar(65535)    ,
	matnr                varchar(65535)    ,
	meinh                varchar(65535)    ,
	umrez                decimal(38,10)    ,
	umren                decimal(38,10)    ,
	eannr                varchar(65535)    ,
	ean11                varchar(65535)    ,
	numtp                varchar(65535)    ,
	laeng                decimal(38,10)    ,
	breit                decimal(38,10)    ,
	hoehe                decimal(38,10)    ,
	meabm                varchar(65535)    ,
	volum                decimal(38,10)    ,
	voleh                varchar(65535)    ,
	brgew                decimal(38,10)    ,
	gewei                varchar(65535)    ,
	mesub                varchar(65535)    ,
	atinn                decimal(38,10)    ,
	mesrt                decimal(38,10)    ,
	xfhdw                varchar(65535)    ,
	xbeww                varchar(65535)    ,
	kzwso                varchar(65535)    ,
	msehi                varchar(65535)    ,
	bflme_marm           varchar(65535)    ,
	gtin_variant         varchar(65535)    ,
	nest_ftr             decimal(38,10)    ,
	max_stack            decimal(38,10)    ,
	capause              decimal(38,10)    ,
	ty2tq                varchar(65535)    ,
	zpkmatcd             varchar(65535)    ,
	zpktypcd             varchar(65535)    ,
	zstackf              varchar(65535)    ,
	zstackwt             decimal(38,10)    ,
	zstkwtun             varchar(65535)    ,
	tailchar             varchar(65535)    
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}