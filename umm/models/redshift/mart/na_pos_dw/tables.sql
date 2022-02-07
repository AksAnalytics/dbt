-- global_pl_stage.bar_acct_attr definition

-- Drop table

-- DROP TABLE global_pl_stage.bar_acct_attr;

--DROP TABLE global_pl_stage.bar_acct_attr;
CREATE TABLE IF NOT EXISTS global_pl_stage.bar_acct_attr
(
	bar_account VARCHAR(250) NOT NULL  ENCODE lzo
	,bar_account_desc VARCHAR(250)   ENCODE lzo
	,bar_acct_type_lvl1 VARCHAR(250)   ENCODE lzo
	,bar_acct_type_lvl2 VARCHAR(250)   ENCODE lzo
	,bar_acct_type_lvl3 VARCHAR(250)   ENCODE lzo
	,bar_acct_type_lvl4 VARCHAR(250)   ENCODE lzo
	,indirect_flag VARCHAR(250)   ENCODE lzo
	,flipsign VARCHAR(250)   ENCODE lzo
	,hive_loaddatetime VARCHAR(250)   ENCODE lzo
	,etl_crte_user VARCHAR(100)   ENCODE lzo
	,etl_crte_ts VARCHAR(100)   ENCODE lzo
	,PRIMARY KEY (bar_account)
)
DISTSTYLE ALL
;
ALTER TABLE global_pl_stage.bar_acct_attr owner to base_admin;


-- global_pl_stage.bar_customer_attr definition

-- Drop table

-- DROP TABLE global_pl_stage.bar_customer_attr;

--DROP TABLE global_pl_stage.bar_customer_attr;
CREATE TABLE IF NOT EXISTS global_pl_stage.bar_customer_attr
(
	bar_customer VARCHAR(250) NOT NULL  ENCODE lzo
	,bar_customer_desc VARCHAR(250)   ENCODE lzo
	,bar_customer_lvl1 VARCHAR(250)   ENCODE lzo
	,bar_customer_lvl2 VARCHAR(250)   ENCODE lzo
	,bar_customer_lvl3 VARCHAR(250)   ENCODE lzo
	,bar_customer_lvl4 VARCHAR(250)   ENCODE lzo
	,hive_loaddatetime VARCHAR(250)   ENCODE lzo
	,etl_crte_user VARCHAR(100)   ENCODE lzo
	,etl_crte_ts VARCHAR(100)   ENCODE lzo
	,PRIMARY KEY (bar_customer)
)
DISTSTYLE ALL
;
ALTER TABLE global_pl_stage.bar_customer_attr owner to base_admin;


-- global_pl_stage.bar_entity_attr definition

-- Drop table

-- DROP TABLE global_pl_stage.bar_entity_attr;

--DROP TABLE global_pl_stage.bar_entity_attr;
CREATE TABLE IF NOT EXISTS global_pl_stage.bar_entity_attr
(
	bar_entity VARCHAR(250) NOT NULL  ENCODE lzo
	,bar_entity_desc VARCHAR(250)   ENCODE lzo
	,bar_entity_currency VARCHAR(250)   ENCODE lzo
	,bar_entity_lvl1 VARCHAR(250)   ENCODE lzo
	,bar_entity_lvl2 VARCHAR(250)   ENCODE lzo
	,bar_entity_lvl3 VARCHAR(250)   ENCODE lzo
	,bar_entity_lvl4 VARCHAR(250)   ENCODE lzo
	,bar_entity_region VARCHAR(250)   ENCODE lzo
	,hive_loaddatetime VARCHAR(250)   ENCODE lzo
	,etl_crte_user VARCHAR(100)   ENCODE lzo
	,etl_crte_ts VARCHAR(100)   ENCODE lzo
	,PRIMARY KEY (bar_entity)
)
DISTSTYLE ALL
;
ALTER TABLE global_pl_stage.bar_entity_attr owner to base_admin;


-- global_pl_stage.bar_funct_attr definition

-- Drop table

-- DROP TABLE global_pl_stage.bar_funct_attr;

--DROP TABLE global_pl_stage.bar_funct_attr;
CREATE TABLE IF NOT EXISTS global_pl_stage.bar_funct_attr
(
	bar_function VARCHAR(250) NOT NULL  ENCODE lzo
	,bar_function_grp VARCHAR(250)   ENCODE lzo
	,functiontype VARCHAR(250)   ENCODE lzo
	,hive_loaddatetime VARCHAR(250)   ENCODE lzo
	,etl_crte_user VARCHAR(100)   ENCODE lzo
	,etl_crte_ts VARCHAR(100)   ENCODE lzo
	,PRIMARY KEY (bar_function)
)
DISTSTYLE ALL
;
ALTER TABLE global_pl_stage.bar_funct_attr owner to base_admin;


-- global_pl_stage.bar_product_attr definition

-- Drop table

-- DROP TABLE global_pl_stage.bar_product_attr;

--DROP TABLE global_pl_stage.bar_product_attr;
CREATE TABLE IF NOT EXISTS global_pl_stage.bar_product_attr
(
	bar_product VARCHAR(250) NOT NULL  ENCODE lzo
	,bar_product_desc VARCHAR(250)   ENCODE lzo
	,bar_product_lvl1 VARCHAR(250)   ENCODE lzo
	,bar_product_lvl2 VARCHAR(250)   ENCODE lzo
	,bar_product_lvl3 VARCHAR(250)   ENCODE lzo
	,bar_product_lvl4 VARCHAR(250)   ENCODE lzo
	,bar_product_lvl5 VARCHAR(250)   ENCODE lzo
	,hive_loaddatetime VARCHAR(250)   ENCODE lzo
	,etl_crte_user VARCHAR(100)   ENCODE lzo
	,etl_crte_ts VARCHAR(100)   ENCODE lzo
	,PRIMARY KEY (bar_product)
)
DISTSTYLE ALL
;
ALTER TABLE global_pl_stage.bar_product_attr owner to base_admin;


-- global_pl_stage.calender_temp definition

-- Drop table

-- DROP TABLE global_pl_stage.calender_temp;

--DROP TABLE global_pl_stage.calender_temp;
CREATE TABLE IF NOT EXISTS global_pl_stage.calender_temp
(
	id INTEGER   ENCODE az64
	,date DATE   ENCODE az64
	,"year" SMALLINT   ENCODE az64
	,"month" SMALLINT   ENCODE az64
	,"day" SMALLINT   ENCODE az64
	,quarter SMALLINT   ENCODE az64
	,week SMALLINT   ENCODE az64
	,day_name VARCHAR(9)   ENCODE lzo
	,month_name VARCHAR(9)   ENCODE lzo
	,holiday_flag BOOLEAN   ENCODE RAW
	,weekend_flag BOOLEAN   ENCODE RAW
)
DISTSTYLE EVEN
;
ALTER TABLE global_pl_stage.calender_temp owner to base_admin;


-- global_pl_stage.calender_temp_1 definition

-- Drop table

-- DROP TABLE global_pl_stage.calender_temp_1;

--DROP TABLE global_pl_stage.calender_temp_1;
CREATE TABLE IF NOT EXISTS global_pl_stage.calender_temp_1
(
	id INTEGER   ENCODE az64
	,date DATE   ENCODE az64
	,"year" SMALLINT   ENCODE az64
	,"month" SMALLINT   ENCODE az64
	,"day" SMALLINT   ENCODE az64
	,quarter SMALLINT   ENCODE az64
	,week SMALLINT   ENCODE az64
	,day_name VARCHAR(9)   ENCODE lzo
	,month_name VARCHAR(9)   ENCODE lzo
	,holiday_flag BOOLEAN   ENCODE RAW
	,weekend_flag BOOLEAN   ENCODE RAW
	,fiscper VARCHAR(15)   ENCODE lzo
)
DISTSTYLE EVEN
;
ALTER TABLE global_pl_stage.calender_temp_1 owner to base_admin;


-- global_pl_stage.customer_master definition

-- Drop table

-- DROP TABLE global_pl_stage.customer_master;

--DROP TABLE global_pl_stage.customer_master;
CREATE TABLE IF NOT EXISTS global_pl_stage.customer_master
(
	erp_customer_number VARCHAR(500) NOT NULL  ENCODE RAW
	,erp_customer_address_code VARCHAR(500)   ENCODE lzo
	,erp_customer_industry_code_1 VARCHAR(500)   ENCODE lzo
	,erp_customer_industry_code_2 VARCHAR(500)   ENCODE lzo
	,erp_customer_industry_code_3 VARCHAR(500)   ENCODE lzo
	,erp_customer_industry_code_4 VARCHAR(500)   ENCODE lzo
	,erp_customer_industry_code_5 VARCHAR(500)   ENCODE lzo
	,erp_customer_industry_key VARCHAR(500)   ENCODE lzo
	,erp_customer_city_code VARCHAR(500)   ENCODE lzo
	,erp_customer_county_code VARCHAR(500)   ENCODE lzo
	,erp_customer_country VARCHAR(500)   ENCODE lzo
	,erp_customer_name VARCHAR(500)   ENCODE lzo
	,erp_customer_city VARCHAR(500)   ENCODE lzo
	,erp_customer_district VARCHAR(500)   ENCODE lzo
	,erp_customer_po_box VARCHAR(500)   ENCODE lzo
	,erp_customer_po_box_postal_code VARCHAR(500)   ENCODE lzo
	,erp_customer_postal_code VARCHAR(500)   ENCODE lzo
	,erp_customer_region VARCHAR(500)   ENCODE lzo
	,erp_customer_regional_market VARCHAR(500)   ENCODE lzo
	,erp_source VARCHAR(500) NOT NULL  ENCODE lzo
	,erp_customer_address VARCHAR(500)   ENCODE lzo
	,etl_crte_user VARCHAR(500)   ENCODE lzo
	,etl_crte_ts VARCHAR(500)   ENCODE lzo
	,hive_loaddatetime VARCHAR(500)   ENCODE lzo
	,PRIMARY KEY (erp_customer_number, erp_source)
)
DISTSTYLE ALL
 SORTKEY (
	erp_customer_number
	, erp_customer_name
	)
;
ALTER TABLE global_pl_stage.customer_master owner to base_admin;


-- global_pl_stage.material_master definition

-- Drop table

-- DROP TABLE global_pl_stage.material_master;

--DROP TABLE global_pl_stage.material_master;
CREATE TABLE IF NOT EXISTS global_pl_stage.material_master
(
	erp_material_number VARCHAR(500) NOT NULL  ENCODE lzo
	,erp_material_description VARCHAR(500)   ENCODE lzo
	,erp_material_category VARCHAR(500)   ENCODE lzo
	,erp_container_requirements VARCHAR(500)   ENCODE lzo
	,erp_generic_material_with_logistical_variants VARCHAR(500)   ENCODE lzo
	,erp_old_material_number VARCHAR(500)   ENCODE lzo
	,erp_brand VARCHAR(500)   ENCODE lzo
	,erp_width NUMERIC(38,10)   ENCODE az64
	,erp_gross_weight NUMERIC(38,10)   ENCODE az64
	,erp_purchase_order_uom VARCHAR(500)   ENCODE lzo
	,erp_source_of_supply VARCHAR(500)   ENCODE lzo
	,erp_procurement_rule VARCHAR(500)   ENCODE lzo
	,erp_cad_indicator VARCHAR(500)   ENCODE lzo
	,erp_quality_conversion_method VARCHAR(500)   ENCODE lzo
	,erp_material_completion_level NUMERIC(38,10)   ENCODE az64
	,erp_internal_object_number NUMERIC(38,10)   ENCODE az64
	,erp_valid_from_date DATE   ENCODE az64
	,erp_ean_upc VARCHAR(500)   ENCODE lzo
	,erp_purhcasing_value_key VARCHAR(500)   ENCODE lzo
	,erp_unit_of_weight_packaging VARCHAR(500)   ENCODE lzo
	,erp_allowed_packaging_weight NUMERIC(38,10)   ENCODE az64
	,erp_volume_unit VARCHAR(500)   ENCODE lzo
	,erp_allowed_packaging_volume NUMERIC(38,10)   ENCODE az64
	,erp_weight_unit VARCHAR(500)   ENCODE lzo
	,erp_size_dimensions VARCHAR(500)   ENCODE lzo
	,erp_height NUMERIC(38,10)   ENCODE az64
	,erp_material_group VARCHAR(500)   ENCODE lzo
	,erp_industry_sector VARCHAR(500)   ENCODE lzo
	,erp_material_type VARCHAR(500)   ENCODE lzo
	,erp_net_weight NUMERIC(38,10)   ENCODE az64
	,erp_product_hierarchy VARCHAR(500)   ENCODE lzo
	,erp_division VARCHAR(500)   ENCODE lzo
	,erp_hazardous_material_number VARCHAR(500)   ENCODE lzo
	,erp_transportation_group VARCHAR(500)   ENCODE lzo
	,erp_packaging_material_type VARCHAR(500)   ENCODE lzo
	,erp_global_product_hierarchy VARCHAR(500)   ENCODE lzo
	,erp_source VARCHAR(500) NOT NULL  ENCODE lzo
	,etl_crte_user VARCHAR(500)   ENCODE lzo
	,etl_crte_ts VARCHAR(500)   ENCODE lzo
	,hive_loaddatetime VARCHAR(500)   ENCODE lzo
	,PRIMARY KEY (erp_material_number, erp_source)
)
DISTSTYLE ALL
;
ALTER TABLE global_pl_stage.material_master owner to base_admin;


-- global_pl_stage.pl_trans_fact definition

-- Drop table

-- DROP TABLE global_pl_stage.pl_trans_fact;

--DROP TABLE global_pl_stage.pl_trans_fact;
CREATE TABLE IF NOT EXISTS global_pl_stage.pl_trans_fact
(
	bar_account VARCHAR(500)   ENCODE RAW
	,bar_amt_lc NUMERIC(38,10)   ENCODE az64
	,bar_brand VARCHAR(500)   ENCODE lzo
	,bar_bu VARCHAR(500)   ENCODE lzo
	,bar_currtype VARCHAR(500)   ENCODE lzo
	,bar_customer VARCHAR(500)   ENCODE lzo
	,bar_entity VARCHAR(500)   ENCODE RAW
	,bar_function VARCHAR(500)   ENCODE lzo
	,bar_period VARCHAR(500)   ENCODE RAW
	,bar_product VARCHAR(500)   ENCODE RAW
	,bar_scenario VARCHAR(500)   ENCODE lzo
	,bar_shipto VARCHAR(500)   ENCODE lzo
	,bar_year VARCHAR(500)   ENCODE RAW
	,bar_fiscal_period VARCHAR(500)   ENCODE lzo
	,erp_account VARCHAR(500)   ENCODE lzo
	,erp_brand_code VARCHAR(500)   ENCODE lzo
	,erp_business_area VARCHAR(500)   ENCODE lzo
	,erp_company_code VARCHAR(500)   ENCODE lzo
	,erp_cost_center VARCHAR(500)   ENCODE lzo
	,erp_doc_type VARCHAR(500)   ENCODE lzo
	,erp_doc_line_num VARCHAR(500)   ENCODE lzo
	,erp_doc_num VARCHAR(500)   ENCODE lzo
	,erp_document_text VARCHAR(1000)   ENCODE lzo
	,erp_vendor VARCHAR(500)   ENCODE lzo
	,erp_material VARCHAR(500)   ENCODE lzo
	,erp_customer_parent VARCHAR(500)   ENCODE lzo
	,erp_posting_date VARCHAR(500)   ENCODE lzo
	,erp_quantity NUMERIC(38,10)   ENCODE az64
	,erp_quantity_uom VARCHAR(500)   ENCODE lzo
	,erp_ref_doc_type VARCHAR(500)   ENCODE lzo
	,erp_ref_doc_line_num VARCHAR(500)   ENCODE lzo
	,erp_ref_doc_num VARCHAR(500)   ENCODE lzo
	,erp_profit_center VARCHAR(500)   ENCODE lzo
	,erp_sales_group VARCHAR(500)   ENCODE lzo
	,erp_sales_office VARCHAR(500)   ENCODE lzo
	,erp_customer_ship_to VARCHAR(500)   ENCODE lzo
	,erp_customer_sold_to VARCHAR(500)   ENCODE lzo
	,erp_plant VARCHAR(500)   ENCODE lzo
	,bar_bods_loaddatetime VARCHAR(500)   ENCODE lzo
	,erp_chartaccts VARCHAR(500)   ENCODE lzo
	,bar_bods_record_id VARCHAR(500) NOT NULL  ENCODE RAW
	,erp_source VARCHAR(500)   ENCODE RAW
	,bar_s_entity_currency VARCHAR(50)   ENCODE lzo
	,bar_s_curr_rate_actual NUMERIC(38,10)   ENCODE az64
	,bar_amt_usd NUMERIC(38,10)   ENCODE az64
	,etl_crte_user VARCHAR(50)   ENCODE lzo
	,etl_crte_ts VARCHAR(50)   ENCODE lzo
	,hive_loaddatetime VARCHAR(250)   ENCODE lzo
	,PRIMARY KEY (bar_bods_record_id)
)
DISTSTYLE KEY
 DISTKEY (bar_bods_record_id)
 SORTKEY (
	bar_account
	, bar_customer
	, bar_entity
	, bar_product
	, bar_period
	, bar_year
	, bar_bods_record_id
	, erp_source
	)
;
ALTER TABLE global_pl_stage.pl_trans_fact owner to base_admin;


-- global_pl_stage.t2 definition

-- Drop table

-- DROP TABLE global_pl_stage.t2;

--DROP TABLE global_pl_stage.t2;
CREATE TABLE IF NOT EXISTS global_pl_stage.t2
(
	id INTEGER   ENCODE az64
	,city VARCHAR(10)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE global_pl_stage.t2 owner to hcloperators;


-- global_pl_stage.temp_pl_trans_fact_stats definition

-- Drop table

-- DROP TABLE global_pl_stage.temp_pl_trans_fact_stats;

--DROP TABLE global_pl_stage.temp_pl_trans_fact_stats;
CREATE TABLE IF NOT EXISTS global_pl_stage.temp_pl_trans_fact_stats
(
	erp_source VARCHAR(500)   ENCODE RAW
	,bar_fiscal_period VARCHAR(500)   ENCODE RAW
	,bar_year VARCHAR(500)   ENCODE RAW
	,bar_period VARCHAR(500)   ENCODE RAW
	,cnt_records BIGINT   ENCODE az64
	,sum_bar_amt_lc NUMERIC(38,10)   ENCODE az64
	,sum_bar_amt_usd NUMERIC(38,10)   ENCODE az64
)
DISTSTYLE EVEN
 SORTKEY (
	erp_source
	, bar_fiscal_period
	, bar_year
	, bar_period
	)
;
ALTER TABLE global_pl_stage.temp_pl_trans_fact_stats owner to base_admin;


-- global_pl_stage.temp_pl_trans_fact_stats_1 definition

-- Drop table

-- DROP TABLE global_pl_stage.temp_pl_trans_fact_stats_1;

--DROP TABLE global_pl_stage.temp_pl_trans_fact_stats_1;
CREATE TABLE IF NOT EXISTS global_pl_stage.temp_pl_trans_fact_stats_1
(
	cal_date VARCHAR(15)   ENCODE RAW
	,erp_source_list VARCHAR(500)   ENCODE RAW
	,erp_source VARCHAR(500)   ENCODE lzo
	,bar_fiscal_period VARCHAR(500)   ENCODE lzo
	,bar_year VARCHAR(500)   ENCODE lzo
	,bar_period VARCHAR(500)   ENCODE lzo
	,cnt_records BIGINT   ENCODE az64
	,sum_bar_amt_lc NUMERIC(38,10)   ENCODE az64
	,sum_bar_amt_usd NUMERIC(38,10)   ENCODE az64
)
DISTSTYLE EVEN
 SORTKEY (
	erp_source_list
	, cal_date
	)
;
ALTER TABLE global_pl_stage.temp_pl_trans_fact_stats_1 owner to base_admin;


-- global_pl_stage.zz_pl_trans_fact definition

-- Drop table

-- DROP TABLE global_pl_stage.zz_pl_trans_fact;

--DROP TABLE global_pl_stage.zz_pl_trans_fact;
CREATE TABLE IF NOT EXISTS global_pl_stage.zz_pl_trans_fact
(
	bar_account VARCHAR(500)   ENCODE lzo
	,bar_amt_lc NUMERIC(38,10)   ENCODE az64
	,bar_brand VARCHAR(500)   ENCODE lzo
	,bar_bu VARCHAR(500)   ENCODE lzo
	,bar_currtype VARCHAR(500)   ENCODE lzo
	,bar_customer VARCHAR(500)   ENCODE lzo
	,bar_entity VARCHAR(500)   ENCODE lzo
	,bar_function VARCHAR(500)   ENCODE lzo
	,bar_period VARCHAR(500)   ENCODE lzo
	,bar_product VARCHAR(500)   ENCODE lzo
	,bar_scenario VARCHAR(500)   ENCODE lzo
	,bar_shipto VARCHAR(500)   ENCODE lzo
	,bar_year VARCHAR(500)   ENCODE lzo
	,bar_fiscal_period VARCHAR(500)   ENCODE lzo
	,erp_account VARCHAR(500)   ENCODE lzo
	,erp_brand_code VARCHAR(500)   ENCODE lzo
	,erp_business_area VARCHAR(500)   ENCODE lzo
	,erp_company_code VARCHAR(500)   ENCODE lzo
	,erp_cost_center VARCHAR(500)   ENCODE lzo
	,erp_doc_type VARCHAR(500)   ENCODE lzo
	,erp_doc_line_num VARCHAR(500)   ENCODE lzo
	,erp_doc_num VARCHAR(500)   ENCODE lzo
	,erp_document_text VARCHAR(1000)   ENCODE lzo
	,erp_vendor VARCHAR(500)   ENCODE lzo
	,erp_material VARCHAR(500)   ENCODE lzo
	,erp_customer_parent VARCHAR(500)   ENCODE lzo
	,erp_posting_date VARCHAR(500)   ENCODE lzo
	,erp_quantity NUMERIC(38,10)   ENCODE az64
	,erp_quantity_uom VARCHAR(500)   ENCODE lzo
	,erp_ref_doc_type VARCHAR(500)   ENCODE lzo
	,erp_ref_doc_line_num VARCHAR(500)   ENCODE lzo
	,erp_ref_doc_num VARCHAR(500)   ENCODE lzo
	,erp_profit_center VARCHAR(500)   ENCODE lzo
	,erp_sales_group VARCHAR(500)   ENCODE lzo
	,erp_sales_office VARCHAR(500)   ENCODE lzo
	,erp_customer_ship_to VARCHAR(500)   ENCODE lzo
	,erp_customer_sold_to VARCHAR(500)   ENCODE lzo
	,erp_plant VARCHAR(500)   ENCODE lzo
	,bar_bods_loaddatetime VARCHAR(500)   ENCODE lzo
	,erp_chartaccts VARCHAR(500)   ENCODE lzo
	,bar_bods_record_id VARCHAR(500)   ENCODE lzo
	,erp_source VARCHAR(500)   ENCODE lzo
	,bar_s_entity_currency VARCHAR(50)   ENCODE lzo
	,bar_s_curr_rate_actual NUMERIC(38,10)   ENCODE az64
	,bar_amt_usd NUMERIC(38,10)   ENCODE az64
	,etl_crte_user VARCHAR(50)   ENCODE lzo
	,etl_crte_ts VARCHAR(50)   ENCODE lzo
)
DISTSTYLE EVEN
;
ALTER TABLE global_pl_stage.zz_pl_trans_fact owner to base_admin;


-- global_pl_stage.zz_pl_trans_fact_1 definition

-- Drop table

-- DROP TABLE global_pl_stage.zz_pl_trans_fact_1;

--DROP TABLE global_pl_stage.zz_pl_trans_fact_1;
CREATE TABLE IF NOT EXISTS global_pl_stage.zz_pl_trans_fact_1
(
	bar_account VARCHAR(500)   ENCODE lzo
	,bar_amt_lc NUMERIC(38,10)   ENCODE az64
	,bar_brand VARCHAR(500)   ENCODE lzo
	,bar_bu VARCHAR(500)   ENCODE lzo
	,bar_currtype VARCHAR(500)   ENCODE lzo
	,bar_customer VARCHAR(500)   ENCODE lzo
	,bar_entity VARCHAR(500)   ENCODE lzo
	,bar_function VARCHAR(500)   ENCODE lzo
	,bar_period VARCHAR(500)   ENCODE lzo
	,bar_product VARCHAR(500)   ENCODE lzo
	,bar_scenario VARCHAR(500)   ENCODE lzo
	,bar_shipto VARCHAR(500)   ENCODE lzo
	,bar_year VARCHAR(500)   ENCODE lzo
	,bar_fiscal_period VARCHAR(500)   ENCODE lzo
	,erp_account VARCHAR(500)   ENCODE lzo
	,erp_brand_code VARCHAR(500)   ENCODE lzo
	,erp_business_area VARCHAR(500)   ENCODE lzo
	,erp_company_code VARCHAR(500)   ENCODE lzo
	,erp_cost_center VARCHAR(500)   ENCODE lzo
	,erp_doc_type VARCHAR(500)   ENCODE lzo
	,erp_doc_line_num VARCHAR(500)   ENCODE lzo
	,erp_doc_num VARCHAR(500)   ENCODE lzo
	,erp_document_text VARCHAR(1000)   ENCODE lzo
	,erp_vendor VARCHAR(500)   ENCODE lzo
	,erp_material VARCHAR(500)   ENCODE lzo
	,erp_customer_parent VARCHAR(500)   ENCODE lzo
	,erp_posting_date VARCHAR(500)   ENCODE lzo
	,erp_quantity NUMERIC(38,10)   ENCODE az64
	,erp_quantity_uom VARCHAR(500)   ENCODE lzo
	,erp_ref_doc_type VARCHAR(500)   ENCODE lzo
	,erp_ref_doc_line_num VARCHAR(500)   ENCODE lzo
	,erp_ref_doc_num VARCHAR(500)   ENCODE lzo
	,erp_profit_center VARCHAR(500)   ENCODE lzo
	,erp_sales_group VARCHAR(500)   ENCODE lzo
	,erp_sales_office VARCHAR(500)   ENCODE lzo
	,erp_customer_ship_to VARCHAR(500)   ENCODE lzo
	,erp_customer_sold_to VARCHAR(500)   ENCODE lzo
	,erp_plant VARCHAR(500)   ENCODE lzo
	,bar_bods_loaddatetime VARCHAR(500)   ENCODE lzo
	,erp_chartaccts VARCHAR(500)   ENCODE lzo
	,bar_bods_record_id VARCHAR(500)   ENCODE lzo
	,erp_source VARCHAR(500)   ENCODE lzo
	,bar_s_entity_currency VARCHAR(50)   ENCODE lzo
	,bar_s_curr_rate_actual NUMERIC(38,10)   ENCODE az64
	,bar_amt_usd NUMERIC(38,10)   ENCODE az64
	,etl_crte_user VARCHAR(50)   ENCODE lzo
	,etl_crte_ts VARCHAR(50)   ENCODE lzo
)
DISTSTYLE EVEN
;
ALTER TABLE global_pl_stage.zz_pl_trans_fact_1 owner to base_admin;