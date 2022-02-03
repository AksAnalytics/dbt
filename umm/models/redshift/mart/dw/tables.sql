-- global_pl.bar_acct_attr definition

-- Drop table

-- DROP TABLE global_pl.bar_acct_attr;

--DROP TABLE global_pl.bar_acct_attr;
CREATE TABLE IF NOT EXISTS global_pl.bar_acct_attr
(
	bar_account VARCHAR(250) NOT NULL  ENCODE lzo
	,bar_account_desc VARCHAR(250)   ENCODE lzo
	,bar_acct_type_lvl1 VARCHAR(250)   ENCODE lzo
	,bar_acct_type_lvl2 VARCHAR(250)   ENCODE lzo
	,bar_acct_type_lvl3 VARCHAR(250)   ENCODE lzo
	,bar_acct_type_lvl4 VARCHAR(250)   ENCODE lzo
	,indirect_flag VARCHAR(250)   ENCODE lzo
	,flipsign VARCHAR(250)   ENCODE lzo
	,etl_crte_user VARCHAR(100)   ENCODE lzo
	,etl_crte_ts DATE   ENCODE az64
	,etl_updt_user VARCHAR(100)   ENCODE lzo
	,etl_updt_ts DATE   ENCODE az64
	,PRIMARY KEY (bar_account)
)
DISTSTYLE ALL
;
ALTER TABLE global_pl.bar_acct_attr owner to hcloperators;


-- global_pl.bar_customer_attr definition

-- Drop table

-- DROP TABLE global_pl.bar_customer_attr;

--DROP TABLE global_pl.bar_customer_attr;
CREATE TABLE IF NOT EXISTS global_pl.bar_customer_attr
(
	bar_customer VARCHAR(250) NOT NULL  ENCODE lzo
	,bar_customer_desc VARCHAR(250)   ENCODE lzo
	,bar_customer_lvl1 VARCHAR(250)   ENCODE lzo
	,bar_customer_lvl2 VARCHAR(250)   ENCODE lzo
	,bar_customer_lvl3 VARCHAR(250)   ENCODE lzo
	,bar_customer_lvl4 VARCHAR(250)   ENCODE lzo
	,etl_crte_user VARCHAR(100)   ENCODE lzo
	,etl_crte_ts DATE   ENCODE az64
	,etl_updt_user VARCHAR(100)   ENCODE lzo
	,etl_updt_ts DATE   ENCODE az64
	,PRIMARY KEY (bar_customer)
)
DISTSTYLE ALL
;
ALTER TABLE global_pl.bar_customer_attr owner to hcloperators;


-- global_pl.bar_entity_attr definition

-- Drop table

-- DROP TABLE global_pl.bar_entity_attr;

--DROP TABLE global_pl.bar_entity_attr;
CREATE TABLE IF NOT EXISTS global_pl.bar_entity_attr
(
	bar_entity VARCHAR(250) NOT NULL  ENCODE lzo
	,bar_entity_desc VARCHAR(250)   ENCODE lzo
	,bar_entity_currency VARCHAR(250)   ENCODE lzo
	,bar_entity_lvl1 VARCHAR(250)   ENCODE lzo
	,bar_entity_lvl2 VARCHAR(250)   ENCODE lzo
	,bar_entity_lvl3 VARCHAR(250)   ENCODE lzo
	,bar_entity_lvl4 VARCHAR(250)   ENCODE lzo
	,bar_entity_region VARCHAR(250)   ENCODE lzo
	,etl_crte_user VARCHAR(100)   ENCODE lzo
	,etl_crte_ts DATE   ENCODE az64
	,etl_updt_user VARCHAR(100)   ENCODE lzo
	,etl_updt_ts DATE   ENCODE az64
	,PRIMARY KEY (bar_entity)
)
DISTSTYLE ALL
;
ALTER TABLE global_pl.bar_entity_attr owner to hcloperators;


-- global_pl.bar_entity_attr_temp definition

-- Drop table

-- DROP TABLE global_pl.bar_entity_attr_temp;

--DROP TABLE global_pl.bar_entity_attr_temp;
CREATE TABLE IF NOT EXISTS global_pl.bar_entity_attr_temp
(
	bar_entity VARCHAR(250)   ENCODE lzo
	,bar_entity_desc VARCHAR(250)   ENCODE lzo
	,bar_entity_currency VARCHAR(250)   ENCODE lzo
	,bar_entity_lvl1 VARCHAR(250)   ENCODE lzo
	,bar_entity_lvl2 VARCHAR(250)   ENCODE lzo
	,bar_entity_lvl3 VARCHAR(250)   ENCODE lzo
	,bar_entity_lvl4 VARCHAR(250)   ENCODE lzo
	,bar_entity_region VARCHAR(250)   ENCODE lzo
	,etl_crte_user VARCHAR(100)   ENCODE lzo
	,etl_crte_ts DATE   ENCODE az64
	,etl_updt_user VARCHAR(100)   ENCODE lzo
	,etl_updt_ts DATE   ENCODE az64
)
DISTSTYLE EVEN
;
ALTER TABLE global_pl.bar_entity_attr_temp owner to base_admin;


-- global_pl.bar_funct_attr definition

-- Drop table

-- DROP TABLE global_pl.bar_funct_attr;

--DROP TABLE global_pl.bar_funct_attr;
CREATE TABLE IF NOT EXISTS global_pl.bar_funct_attr
(
	bar_function VARCHAR(250) NOT NULL  ENCODE lzo
	,bar_function_grp VARCHAR(250)   ENCODE lzo
	,functiontype VARCHAR(250)   ENCODE lzo
	,etl_crte_user VARCHAR(100)   ENCODE lzo
	,etl_crte_ts DATE   ENCODE az64
	,etl_updt_user VARCHAR(100)   ENCODE lzo
	,etl_updt_ts DATE   ENCODE az64
	,PRIMARY KEY (bar_function)
)
DISTSTYLE ALL
;
ALTER TABLE global_pl.bar_funct_attr owner to hcloperators;


-- global_pl.bar_product_attr definition

-- Drop table

-- DROP TABLE global_pl.bar_product_attr;

--DROP TABLE global_pl.bar_product_attr;
CREATE TABLE IF NOT EXISTS global_pl.bar_product_attr
(
	bar_product VARCHAR(250) NOT NULL  ENCODE lzo
	,bar_product_desc VARCHAR(250)   ENCODE lzo
	,bar_product_lvl1 VARCHAR(250)   ENCODE lzo
	,bar_product_lvl2 VARCHAR(250)   ENCODE lzo
	,bar_product_lvl3 VARCHAR(250)   ENCODE lzo
	,bar_product_lvl4 VARCHAR(250)   ENCODE lzo
	,bar_product_lvl5 VARCHAR(250)   ENCODE lzo
	,etl_crte_user VARCHAR(100)   ENCODE lzo
	,etl_crte_ts DATE   ENCODE az64
	,etl_updt_user VARCHAR(100)   ENCODE lzo
	,etl_updt_ts DATE   ENCODE az64
	,PRIMARY KEY (bar_product)
)
DISTSTYLE ALL
;
ALTER TABLE global_pl.bar_product_attr owner to hcloperators;


-- global_pl.bar_product_attr_temp definition

-- Drop table

-- DROP TABLE global_pl.bar_product_attr_temp;

--DROP TABLE global_pl.bar_product_attr_temp;
CREATE TABLE IF NOT EXISTS global_pl.bar_product_attr_temp
(
	bar_product VARCHAR(250)   ENCODE lzo
	,bar_product_desc VARCHAR(250)   ENCODE lzo
	,bar_product_lvl1 VARCHAR(250)   ENCODE lzo
	,bar_product_lvl2 VARCHAR(250)   ENCODE lzo
	,bar_product_lvl3 VARCHAR(250)   ENCODE lzo
	,bar_product_lvl4 VARCHAR(250)   ENCODE lzo
	,bar_product_lvl5 VARCHAR(250)   ENCODE lzo
	,etl_crte_user VARCHAR(100)   ENCODE lzo
	,etl_crte_ts DATE   ENCODE az64
	,etl_updt_user VARCHAR(100)   ENCODE lzo
	,etl_updt_ts DATE   ENCODE az64
)
DISTSTYLE EVEN
;
ALTER TABLE global_pl.bar_product_attr_temp owner to base_admin;


-- global_pl.calendar definition

-- Drop table

-- DROP TABLE global_pl.calendar;

--DROP TABLE global_pl.calendar;
CREATE TABLE IF NOT EXISTS global_pl.calendar
(
	dy_id NUMERIC(18,0)   ENCODE az64
	,dy_dte TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,absolute_dy_nbr NUMERIC(18,0)   ENCODE az64
	,absolute_wk_nbr NUMERIC(18,0)   ENCODE az64
	,absolute_mth_nbr NUMERIC(18,0)   ENCODE az64
	,absolute_qtr_nbr NUMERIC(18,0)   ENCODE az64
	,dy_in_wk_nbr NUMERIC(18,0)   ENCODE az64
	,dy_in_wk_name VARCHAR(256)   ENCODE lzo
	,julian_dy_nbr NUMERIC(18,0)   ENCODE az64
	,clndr_wk_nbr NUMERIC(18,0)   ENCODE az64
	,clndr_wk_id NUMERIC(18,0)   ENCODE az64
	,clndr_mth_nbr NUMERIC(18,0)   ENCODE az64
	,clndr_mth_id VARCHAR(256)   ENCODE lzo
	,clndr_mth_name VARCHAR(256)   ENCODE lzo
	,clndr_qtr_nbr NUMERIC(18,0)   ENCODE az64
	,clndr_qtr_id VARCHAR(256)   ENCODE lzo
	,clndr_qtr_name VARCHAR(256)   ENCODE lzo
	,clndr_yr_id NUMERIC(18,0)   ENCODE az64
	,clndr_dy_in_mth_nbr NUMERIC(18,0)   ENCODE az64
	,is_first_dy_in_clndr_mth_flag VARCHAR(256)   ENCODE lzo
	,is_last_dy_in_clndr_mth_flag VARCHAR(256)   ENCODE lzo
	,wk_begin_dte TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,wk_end_dte NUMERIC(18,0)   ENCODE az64
	,fmth_begin_dte NUMERIC(18,0)   ENCODE az64
	,fmth_end_dte NUMERIC(18,0)   ENCODE az64
	,fqtr_begin_dte NUMERIC(18,0)   ENCODE az64
	,fqtr_end_dte NUMERIC(18,0)   ENCODE az64
	,fyr_begin_dte NUMERIC(18,0)   ENCODE az64
	,fyr_end_dte NUMERIC(18,0)   ENCODE az64
	,fwk_nbr NUMERIC(18,0)   ENCODE az64
	,fwk_id NUMERIC(18,0)   ENCODE az64
	,fwk_cd VARCHAR(256)   ENCODE lzo
	,fmth_nbr NUMERIC(18,0)   ENCODE az64
	,fmth_id NUMERIC(18,0)   ENCODE az64
	,fmth_cd VARCHAR(256)   ENCODE lzo
	,fmth_name VARCHAR(256)   ENCODE lzo
	,fmth_short_name VARCHAR(256)   ENCODE lzo
	,fqtr_nbr NUMERIC(18,0)   ENCODE az64
	,fqtr_id NUMERIC(18,0)   ENCODE az64
	,fqtr_cd VARCHAR(256)   ENCODE lzo
	,fqtr_name VARCHAR(256)   ENCODE lzo
	,fyr_id NUMERIC(18,0)   ENCODE az64
	,fdy_in_mth_nbr NUMERIC(18,0)   ENCODE az64
	,fscl_days_remaining_in_mth NUMERIC(18,0)   ENCODE az64
	,fdy_in_qtr_nbr NUMERIC(18,0)   ENCODE az64
	,fscl_days_remaining_in_qtr NUMERIC(18,0)   ENCODE az64
	,fdy_in_yr_nbr NUMERIC(18,0)   ENCODE az64
	,fscl_days_remaining_in_yr NUMERIC(18,0)   ENCODE az64
	,fwk_in_mth_nbr NUMERIC(18,0)   ENCODE az64
	,fwk_in_qtr NUMERIC(18,0)   ENCODE az64
	,is_wk_dy_flag VARCHAR(256)   ENCODE lzo
	,is_weekend_flag VARCHAR(256)   ENCODE lzo
	,is_first_dy_of_fwk_flag VARCHAR(256)   ENCODE lzo
	,is_last_dy_of_fwk_flag VARCHAR(256)   ENCODE lzo
	,is_first_dy_of_fmth_flag VARCHAR(256)   ENCODE lzo
	,is_last_dy_of_fmth_flag VARCHAR(256)   ENCODE lzo
	,is_first_dy_of_fqtr_flag VARCHAR(256)   ENCODE lzo
	,is_last_dy_of_fqtr_flag VARCHAR(256)   ENCODE lzo
	,is_first_dy_of_fyr_flag VARCHAR(256)   ENCODE lzo
	,is_last_dy_of_fyr_flag VARCHAR(256)   ENCODE lzo
	,season_name VARCHAR(256)   ENCODE lzo
	,holiday_name VARCHAR(256)   ENCODE lzo
	,holiday_season_name VARCHAR(256)   ENCODE lzo
	,holiday_observed_name VARCHAR(256)   ENCODE lzo
	,special_event_name VARCHAR(256)   ENCODE lzo
	,etl_batch_id NUMERIC(18,0)   ENCODE az64
)
DISTSTYLE AUTO
;
ALTER TABLE global_pl.calendar owner to base_admin;


-- global_pl.clndr_dim definition

-- Drop table

-- DROP TABLE global_pl.clndr_dim;

--DROP TABLE global_pl.clndr_dim;
CREATE TABLE IF NOT EXISTS global_pl.clndr_dim
(
	dy_id INTEGER NOT NULL  ENCODE RAW
	,dy_dte DATE   ENCODE az64
	,absolute_dy_nbr INTEGER   ENCODE az64
	,absolute_wk_nbr INTEGER   ENCODE az64
	,absolute_mth_nbr INTEGER   ENCODE az64
	,absolute_qtr_nbr INTEGER   ENCODE az64
	,dy_in_wk_nbr INTEGER   ENCODE az64
	,dy_in_wk_name VARCHAR(20)   ENCODE lzo
	,julian_dy_nbr INTEGER   ENCODE az64
	,clndr_wk_nbr INTEGER   ENCODE az64
	,clndr_wk_id INTEGER   ENCODE az64
	,clndr_mth_nbr INTEGER   ENCODE az64
	,clndr_mth_id VARCHAR(20)   ENCODE lzo
	,clndr_mth_name VARCHAR(20)   ENCODE lzo
	,clndr_qtr_nbr INTEGER   ENCODE az64
	,clndr_qtr_id VARCHAR(20)   ENCODE lzo
	,clndr_qtr_name VARCHAR(20)   ENCODE lzo
	,clndr_yr_id INTEGER   ENCODE az64
	,clndr_dy_in_mth_nbr INTEGER   ENCODE az64
	,is_first_dy_in_clndr_mth_flag VARCHAR(20)   ENCODE lzo
	,is_last_dy_in_clndr_mth_flag VARCHAR(20)   ENCODE lzo
	,wk_begin_dte DATE   ENCODE az64
	,wk_end_dte INTEGER   ENCODE az64
	,fmth_begin_dte INTEGER   ENCODE az64
	,fmth_end_dte INTEGER   ENCODE az64
	,fqtr_begin_dte INTEGER   ENCODE az64
	,fqtr_end_dte INTEGER   ENCODE az64
	,fyr_begin_dte INTEGER   ENCODE az64
	,fyr_end_dte INTEGER   ENCODE az64
	,fwk_nbr INTEGER   ENCODE az64
	,fwk_id INTEGER   ENCODE az64
	,fwk_cd VARCHAR(20)   ENCODE lzo
	,fmth_nbr INTEGER   ENCODE az64
	,fmth_id INTEGER   ENCODE az64
	,fmth_cd VARCHAR(20)   ENCODE lzo
	,fmth_name VARCHAR(20)   ENCODE lzo
	,fmth_short_name VARCHAR(20)   ENCODE lzo
	,fqtr_nbr INTEGER   ENCODE az64
	,fqtr_id INTEGER   ENCODE az64
	,fqtr_cd VARCHAR(20)   ENCODE lzo
	,fqtr_name VARCHAR(20)   ENCODE lzo
	,fyr_id INTEGER   ENCODE az64
	,fdy_in_mth_nbr INTEGER   ENCODE az64
	,fscl_days_remaining_in_mth INTEGER   ENCODE az64
	,fdy_in_qtr_nbr INTEGER   ENCODE az64
	,fscl_days_remaining_in_qtr INTEGER   ENCODE az64
	,fdy_in_yr_nbr INTEGER   ENCODE az64
	,fscl_days_remaining_in_yr INTEGER   ENCODE az64
	,fwk_in_mth_nbr INTEGER   ENCODE az64
	,fwk_in_qtr INTEGER   ENCODE az64
	,is_wk_dy_flag VARCHAR(20)   ENCODE lzo
	,is_weekend_flag VARCHAR(20)   ENCODE lzo
	,is_first_dy_of_fwk_flag VARCHAR(20)   ENCODE lzo
	,is_last_dy_of_fwk_flag VARCHAR(20)   ENCODE lzo
	,is_first_dy_of_fmth_flag VARCHAR(20)   ENCODE lzo
	,is_last_dy_of_fmth_flag VARCHAR(20)   ENCODE lzo
	,is_first_dy_of_fqtr_flag VARCHAR(20)   ENCODE lzo
	,is_last_dy_of_fqtr_flag VARCHAR(20)   ENCODE lzo
	,is_first_dy_of_fyr_flag VARCHAR(20)   ENCODE lzo
	,is_last_dy_of_fyr_flag VARCHAR(20)   ENCODE lzo
	,season_name VARCHAR(20)   ENCODE lzo
	,holiday_name VARCHAR(20)   ENCODE lzo
	,holiday_season_name VARCHAR(20)   ENCODE lzo
	,holiday_observed_name VARCHAR(20)   ENCODE lzo
	,special_event_name VARCHAR(20)   ENCODE lzo
	,etl_batch_id INTEGER   ENCODE az64
	,PRIMARY KEY (dy_id)
)
DISTSTYLE KEY
 DISTKEY (dy_id)
 SORTKEY (
	dy_id
	)
;
ALTER TABLE global_pl.clndr_dim owner to base_admin;


-- global_pl.customer_master definition

-- Drop table

-- DROP TABLE global_pl.customer_master;

--DROP TABLE global_pl.customer_master;
CREATE TABLE IF NOT EXISTS global_pl.customer_master
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
	,erp_source VARCHAR(50)   ENCODE lzo
	,erp_customer_address VARCHAR(500)   ENCODE lzo
	,etl_crte_ts VARCHAR(50)   ENCODE lzo
	,etl_crte_user VARCHAR(50)   ENCODE lzo
	,PRIMARY KEY (erp_customer_number)
)
DISTSTYLE ALL
 SORTKEY (
	erp_customer_number
	, erp_customer_name
	)
;
ALTER TABLE global_pl.customer_master owner to hcloperators;


-- global_pl.customer_master_temp definition

-- Drop table

-- DROP TABLE global_pl.customer_master_temp;

--DROP TABLE global_pl.customer_master_temp;
CREATE TABLE IF NOT EXISTS global_pl.customer_master_temp
(
	erp_customer_number VARCHAR(500)   ENCODE lzo
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
	,erp_source VARCHAR(50)   ENCODE lzo
	,erp_customer_address VARCHAR(500)   ENCODE lzo
	,etl_crte_ts VARCHAR(50)   ENCODE lzo
	,etl_crte_user VARCHAR(50)   ENCODE lzo
)
DISTSTYLE EVEN
;
ALTER TABLE global_pl.customer_master_temp owner to base_admin;


-- global_pl.hfm_temp definition

-- Drop table

-- DROP TABLE global_pl.hfm_temp;

--DROP TABLE global_pl.hfm_temp;
CREATE TABLE IF NOT EXISTS global_pl.hfm_temp
(
	bar_period VARCHAR(20)   ENCODE lzo
	,bar_year VARCHAR(20)   ENCODE lzo
	,bar_function VARCHAR(20)   ENCODE lzo
	,bar_amt NUMERIC(20,10)   ENCODE az64
)
DISTSTYLE AUTO
;
ALTER TABLE global_pl.hfm_temp owner to base_admin;


-- global_pl.job_master definition

-- Drop table

-- DROP TABLE global_pl.job_master;

--DROP TABLE global_pl.job_master;
CREATE TABLE IF NOT EXISTS global_pl.job_master
(
	job_id INTEGER NOT NULL  ENCODE az64
	,job_name VARCHAR(100)   ENCODE lzo
	,table_name VARCHAR(100)   ENCODE lzo
	,frequency VARCHAR(100)   ENCODE lzo
	,job_state VARCHAR(100)   ENCODE lzo
	,etl_crte_user VARCHAR(100)   ENCODE lzo
	,etl_crte_ts DATE   ENCODE az64
	,etl_updt_user VARCHAR(100)   ENCODE lzo
	,etl_updt_ts DATE   ENCODE az64
	,PRIMARY KEY (job_id)
)
DISTSTYLE ALL
;
ALTER TABLE global_pl.job_master owner to hcloperators;

-- Table Triggers

CREATE CONSTRAINT TRIGGER "RI_ConstraintTrigger_664873" AFTER
DELETE
    ON
    global_pl.job_master
FROM
    global_pl.job_history NOT DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE PROCEDURE "RI_FKey_noaction_del"('job_history_job_id_fkey',
    'job_history',
    'job_master',
    'UNSPECIFIED',
    'job_id',
    'job_id');
CREATE CONSTRAINT TRIGGER "RI_ConstraintTrigger_664874" AFTER
UPDATE
    ON
    global_pl.job_master
FROM
    global_pl.job_history NOT DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE PROCEDURE "RI_FKey_noaction_upd"('job_history_job_id_fkey',
    'job_history',
    'job_master',
    'UNSPECIFIED',
    'job_id',
    'job_id');


-- global_pl.material_master definition

-- Drop table

-- DROP TABLE global_pl.material_master;

--DROP TABLE global_pl.material_master;
CREATE TABLE IF NOT EXISTS global_pl.material_master
(
	erp_material_number VARCHAR(100) NOT NULL  ENCODE lzo
	,erp_material_description VARCHAR(100)   ENCODE lzo
	,erp_material_category VARCHAR(100)   ENCODE lzo
	,erp_container_requirements VARCHAR(100)   ENCODE lzo
	,erp_generic_material_with_logistical_variants VARCHAR(100)   ENCODE lzo
	,erp_old_material_number VARCHAR(100)   ENCODE lzo
	,erp_brand VARCHAR(100)   ENCODE lzo
	,erp_width NUMERIC(38,10)   ENCODE az64
	,erp_gross_weight NUMERIC(38,10)   ENCODE az64
	,erp_purchase_order_uom VARCHAR(100)   ENCODE lzo
	,erp_source_of_supply VARCHAR(100)   ENCODE lzo
	,erp_procurement_rule VARCHAR(100)   ENCODE lzo
	,erp_cad_indicator VARCHAR(100)   ENCODE lzo
	,erp_quality_conversion_method VARCHAR(100)   ENCODE lzo
	,erp_material_completion_level NUMERIC(38,10)   ENCODE az64
	,erp_internal_object_number NUMERIC(38,10)   ENCODE az64
	,erp_valid_from_date DATE   ENCODE az64
	,erp_ean_upc VARCHAR(100)   ENCODE lzo
	,erp_purhcasing_value_key VARCHAR(100)   ENCODE lzo
	,erp_unit_of_weight_packaging VARCHAR(100)   ENCODE lzo
	,erp_allowed_packaging_weight NUMERIC(38,10)   ENCODE az64
	,erp_volume_unit VARCHAR(100)   ENCODE lzo
	,erp_allowed_packaging_volume NUMERIC(38,10)   ENCODE az64
	,erp_weight_unit VARCHAR(100)   ENCODE lzo
	,erp_size_dimensions VARCHAR(100)   ENCODE lzo
	,erp_height NUMERIC(38,10)   ENCODE az64
	,erp_material_group VARCHAR(100)   ENCODE lzo
	,erp_industry_sector VARCHAR(100)   ENCODE lzo
	,erp_material_type VARCHAR(100)   ENCODE lzo
	,erp_net_weight NUMERIC(38,10)   ENCODE az64
	,erp_product_hierarchy VARCHAR(100)   ENCODE lzo
	,erp_division VARCHAR(100)   ENCODE lzo
	,erp_hazardous_material_number VARCHAR(100)   ENCODE lzo
	,erp_transportation_group VARCHAR(100)   ENCODE lzo
	,erp_packaging_material_type VARCHAR(100)   ENCODE lzo
	,erp_global_product_hierarchy VARCHAR(100)   ENCODE lzo
	,erp_source VARCHAR(100)   ENCODE lzo
	,etl_crte_user VARCHAR(50)   ENCODE lzo
	,etl_crte_ts VARCHAR(50)   ENCODE lzo
	,PRIMARY KEY (erp_material_number)
)
DISTSTYLE ALL
;
ALTER TABLE global_pl.material_master owner to base_admin;


-- global_pl.material_master_temp definition

-- Drop table

-- DROP TABLE global_pl.material_master_temp;

--DROP TABLE global_pl.material_master_temp;
CREATE TABLE IF NOT EXISTS global_pl.material_master_temp
(
	erp_material_number VARCHAR(100)   ENCODE lzo
	,erp_material_description VARCHAR(100)   ENCODE lzo
	,erp_material_category VARCHAR(100)   ENCODE lzo
	,erp_container_requirements VARCHAR(100)   ENCODE lzo
	,erp_generic_material_with_logistical_variants VARCHAR(100)   ENCODE lzo
	,erp_old_material_number VARCHAR(100)   ENCODE lzo
	,erp_brand VARCHAR(100)   ENCODE lzo
	,erp_width NUMERIC(38,10)   ENCODE az64
	,erp_gross_weight NUMERIC(38,10)   ENCODE az64
	,erp_purchase_order_uom VARCHAR(100)   ENCODE lzo
	,erp_source_of_supply VARCHAR(100)   ENCODE lzo
	,erp_procurement_rule VARCHAR(100)   ENCODE lzo
	,erp_cad_indicator VARCHAR(100)   ENCODE lzo
	,erp_quality_conversion_method VARCHAR(100)   ENCODE lzo
	,erp_material_completion_level NUMERIC(38,10)   ENCODE az64
	,erp_internal_object_number NUMERIC(38,10)   ENCODE az64
	,erp_valid_from_date DATE   ENCODE az64
	,erp_ean_upc VARCHAR(100)   ENCODE lzo
	,erp_purhcasing_value_key VARCHAR(100)   ENCODE lzo
	,erp_unit_of_weight_packaging VARCHAR(100)   ENCODE lzo
	,erp_allowed_packaging_weight NUMERIC(38,10)   ENCODE az64
	,erp_volume_unit VARCHAR(100)   ENCODE lzo
	,erp_allowed_packaging_volume NUMERIC(38,10)   ENCODE az64
	,erp_weight_unit VARCHAR(100)   ENCODE lzo
	,erp_size_dimensions VARCHAR(100)   ENCODE lzo
	,erp_height NUMERIC(38,10)   ENCODE az64
	,erp_material_group VARCHAR(100)   ENCODE lzo
	,erp_industry_sector VARCHAR(100)   ENCODE lzo
	,erp_material_type VARCHAR(100)   ENCODE lzo
	,erp_net_weight NUMERIC(38,10)   ENCODE az64
	,erp_product_hierarchy VARCHAR(100)   ENCODE lzo
	,erp_division VARCHAR(100)   ENCODE lzo
	,erp_hazardous_material_number VARCHAR(100)   ENCODE lzo
	,erp_transportation_group VARCHAR(100)   ENCODE lzo
	,erp_packaging_material_type VARCHAR(100)   ENCODE lzo
	,erp_global_product_hierarchy VARCHAR(100)   ENCODE lzo
	,erp_source VARCHAR(100)   ENCODE lzo
	,etl_crte_user VARCHAR(50)   ENCODE lzo
	,etl_crte_ts VARCHAR(50)   ENCODE lzo
)
DISTSTYLE EVEN
;
ALTER TABLE global_pl.material_master_temp owner to base_admin;


-- global_pl.pl_trans_fact definition

-- Drop table

-- DROP TABLE global_pl.pl_trans_fact;

--DROP TABLE global_pl.pl_trans_fact;
CREATE TABLE IF NOT EXISTS global_pl.pl_trans_fact
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
DISTSTYLE KEY
 DISTKEY (bar_bods_record_id)
;
ALTER TABLE global_pl.pl_trans_fact owner to base_admin;


-- global_pl.pl_trans_fact_20210502 definition

-- Drop table

-- DROP TABLE global_pl.pl_trans_fact_20210502;

--DROP TABLE global_pl.pl_trans_fact_20210502;
CREATE TABLE IF NOT EXISTS global_pl.pl_trans_fact_20210502
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
DISTSTYLE KEY
 DISTKEY (bar_bods_record_id)
;
ALTER TABLE global_pl.pl_trans_fact_20210502 owner to base_admin;


-- global_pl.pl_trans_fact_20210507 definition

-- Drop table

-- DROP TABLE global_pl.pl_trans_fact_20210507;

--DROP TABLE global_pl.pl_trans_fact_20210507;
CREATE TABLE IF NOT EXISTS global_pl.pl_trans_fact_20210507
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
DISTSTYLE KEY
 DISTKEY (bar_bods_record_id)
;
ALTER TABLE global_pl.pl_trans_fact_20210507 owner to base_admin;


-- global_pl.pl_trans_fact_temp definition

-- Drop table

-- DROP TABLE global_pl.pl_trans_fact_temp;

--DROP TABLE global_pl.pl_trans_fact_temp;
CREATE TABLE IF NOT EXISTS global_pl.pl_trans_fact_temp
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
DISTSTYLE KEY
 DISTKEY (bar_bods_record_id)
;
ALTER TABLE global_pl.pl_trans_fact_temp owner to base_admin;


-- global_pl.status_history definition

-- Drop table

-- DROP TABLE global_pl.status_history;

--DROP TABLE global_pl.status_history;
CREATE TABLE IF NOT EXISTS global_pl.status_history
(
	run_id INTEGER NOT NULL DEFAULT "identity"(911236, 0, '1,1'::text) ENCODE az64
	,job_nm VARCHAR(100)   ENCODE lzo
	,run_date DATE   ENCODE az64
	,run_seq INTEGER   ENCODE az64
	,job_status VARCHAR(100)   ENCODE lzo
	,curr_mon_rows_del INTEGER   ENCODE az64
	,curr_mon_rows_ins INTEGER   ENCODE az64
	,prev_mon_rows_del INTEGER   ENCODE az64
	,prev_mon_rows_ins INTEGER   ENCODE az64
	,start_time TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_time TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,PRIMARY KEY (run_id)
)
DISTSTYLE KEY
 DISTKEY (run_id)
;
ALTER TABLE global_pl.status_history owner to base_admin;


-- global_pl.status_history_1 definition

-- Drop table

-- DROP TABLE global_pl.status_history_1;

--DROP TABLE global_pl.status_history_1;
CREATE TABLE IF NOT EXISTS global_pl.status_history_1
(
	run_id INTEGER NOT NULL DEFAULT "identity"(911348, 0, '1,1'::text) ENCODE az64
	,job_nm VARCHAR(100)   ENCODE lzo
	,run_date DATE   ENCODE az64
	,run_seq INTEGER   ENCODE az64
	,job_status VARCHAR(100)   ENCODE lzo
	,curr_mon_rows_del INTEGER   ENCODE az64
	,curr_mon_rows_ins INTEGER   ENCODE az64
	,prev_mon_rows_del INTEGER   ENCODE az64
	,prev_mon_rows_ins INTEGER   ENCODE az64
	,start_time TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_time TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,PRIMARY KEY (run_id)
)
DISTSTYLE KEY
 DISTKEY (run_id)
;
ALTER TABLE global_pl.status_history_1 owner to base_admin;


-- global_pl.status_master definition

-- Drop table

-- DROP TABLE global_pl.status_master;

--DROP TABLE global_pl.status_master;
CREATE TABLE IF NOT EXISTS global_pl.status_master
(
	id INTEGER NOT NULL DEFAULT "identity"(911226, 0, '1,1'::text) ENCODE az64
	,tbl_type VARCHAR(100)   ENCODE lzo
	,job_nm VARCHAR(100) NOT NULL  ENCODE lzo
	,tgt_tbl_nm VARCHAR(100)   ENCODE lzo
	,src_tbl_nm VARCHAR(100)   ENCODE lzo
	,src_col_lst VARCHAR(65535)   ENCODE lzo
	,tgt_col_lst VARCHAR(65535)   ENCODE lzo
	,erp_source VARCHAR(100)   ENCODE lzo
	,frequency VARCHAR(100)   ENCODE lzo
	,job_state VARCHAR(100)   ENCODE lzo
	,manual_run VARCHAR(100)   ENCODE lzo
	,crte_user VARCHAR(100)   ENCODE lzo
	,crte_ts VARCHAR(100)   ENCODE lzo
	,PRIMARY KEY (id, job_nm)
)
DISTSTYLE ALL
;
ALTER TABLE global_pl.status_master owner to base_admin;


-- global_pl.status_master_1 definition

-- Drop table

-- DROP TABLE global_pl.status_master_1;

--DROP TABLE global_pl.status_master_1;
CREATE TABLE IF NOT EXISTS global_pl.status_master_1
(
	id INTEGER NOT NULL DEFAULT "identity"(911343, 0, '1,1'::text) ENCODE az64
	,tbl_type VARCHAR(100)   ENCODE lzo
	,job_nm VARCHAR(100) NOT NULL  ENCODE lzo
	,tgt_tbl_nm VARCHAR(100)   ENCODE lzo
	,src_tbl_nm VARCHAR(100)   ENCODE lzo
	,src_col_lst VARCHAR(65535)   ENCODE lzo
	,tgt_col_lst VARCHAR(65535)   ENCODE lzo
	,erp_source VARCHAR(100)   ENCODE lzo
	,frequency VARCHAR(100)   ENCODE lzo
	,job_state VARCHAR(100)   ENCODE lzo
	,manual_run VARCHAR(100)   ENCODE lzo
	,crte_user VARCHAR(100)   ENCODE lzo
	,crte_ts VARCHAR(100)   ENCODE lzo
	,PRIMARY KEY (id, job_nm)
)
DISTSTYLE ALL
;
ALTER TABLE global_pl.status_master_1 owner to base_admin;


-- global_pl.testsp definition

-- Drop table

-- DROP TABLE global_pl.testsp;

--DROP TABLE global_pl.testsp;
CREATE TABLE IF NOT EXISTS global_pl.testsp
(
	id INTEGER  DEFAULT "identity"(985663, 0, '1,1'::text) ENCODE az64
	,dtrun VARCHAR(100)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE global_pl.testsp owner to base_admin;


-- global_pl.job_history definition

-- Drop table

-- DROP TABLE global_pl.job_history;

--DROP TABLE global_pl.job_history;
CREATE TABLE IF NOT EXISTS global_pl.job_history
(
	run_id INTEGER NOT NULL  ENCODE az64
	,job_id INTEGER   ENCODE az64
	,job_name VARCHAR(100)   ENCODE lzo
	,table_name VARCHAR(100)   ENCODE lzo
	,run_date VARCHAR(100)   ENCODE lzo
	,run_seq INTEGER   ENCODE az64
	,job_status VARCHAR(100)   ENCODE lzo
	,etl_crte_user VARCHAR(100)   ENCODE lzo
	,etl_crte_ts DATE   ENCODE az64
	,etl_updt_user VARCHAR(100)   ENCODE lzo
	,etl_updt_ts DATE   ENCODE az64
	,PRIMARY KEY (run_id)
)
DISTSTYLE KEY
 DISTKEY (run_id)
;
ALTER TABLE global_pl.job_history owner to hcloperators;

-- Table Triggers

CREATE CONSTRAINT TRIGGER "RI_ConstraintTrigger_664872" AFTER
INSERT
    OR
UPDATE
    ON
    global_pl.job_history
FROM
    global_pl.job_master NOT DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE PROCEDURE "RI_FKey_check_ins"('job_history_job_id_fkey',
    'job_history',
    'job_master',
    'UNSPECIFIED',
    'job_id',
    'job_id');