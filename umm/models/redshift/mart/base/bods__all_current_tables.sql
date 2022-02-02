-- bods.acg_pl_trans_archive_current definition

-- Drop table

-- DROP TABLE bods.acg_pl_trans_archive_current;

--DROP TABLE bods.acg_pl_trans_archive_current;
CREATE TABLE IF NOT EXISTS bods.acg_pl_trans_archive_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id VARCHAR(65535)   ENCODE lzo
	,fiscper VARCHAR(65535)   ENCODE lzo
	,fyr_id VARCHAR(65535)   ENCODE lzo
	,fmth_nbr VARCHAR(65535)   ENCODE lzo
	,co_cd VARCHAR(65535)   ENCODE lzo
	,acct VARCHAR(65535)   ENCODE lzo
	,txn_id VARCHAR(65535)   ENCODE lzo
	,mvmnt_dte VARCHAR(65535)   ENCODE lzo
	,rcrdng_dte VARCHAR(65535)   ENCODE lzo
	,crncy_cd VARCHAR(65535)   ENCODE lzo
	,amt VARCHAR(65535)   ENCODE lzo
	,usd_amt VARCHAR(65535)   ENCODE lzo
	,int_functype VARCHAR(65535)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_amt VARCHAR(65535)   ENCODE lzo
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.acg_pl_trans_archive_current owner to base_admin;


-- bods.agresso_pl_trans_current definition

-- Drop table

-- DROP TABLE bods.agresso_pl_trans_current;

--DROP TABLE bods.agresso_pl_trans_current;
CREATE TABLE IF NOT EXISTS bods.agresso_pl_trans_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id VARCHAR(65535)   ENCODE lzo
	,fiscper VARCHAR(65535)   ENCODE lzo
	,country VARCHAR(65535)   ENCODE lzo
	,currency VARCHAR(65535)   ENCODE lzo
	,fiscal_year VARCHAR(65535)   ENCODE lzo
	,period VARCHAR(65535)   ENCODE lzo
	,project VARCHAR(65535)   ENCODE lzo
	,project_desc VARCHAR(65535)   ENCODE lzo
	,site_ref VARCHAR(65535)   ENCODE lzo
	,work_order VARCHAR(65535)   ENCODE lzo
	,work_order_desc VARCHAR(65535)   ENCODE lzo
	,client VARCHAR(65535)   ENCODE lzo
	,product_code VARCHAR(65535)   ENCODE lzo
	,product_code_desc VARCHAR(65535)   ENCODE lzo
	,invoice_num VARCHAR(65535)   ENCODE lzo
	,hfm_acct VARCHAR(65535)   ENCODE lzo
	,cust_num VARCHAR(65535)   ENCODE lzo
	,cust_num_desc VARCHAR(65535)   ENCODE lzo
	,project_cust_num VARCHAR(65535)   ENCODE lzo
	,project_cust_num_desc VARCHAR(65535)   ENCODE lzo
	,business_area VARCHAR(65535)   ENCODE lzo
	,profit_center VARCHAR(65535)   ENCODE lzo
	,"region" VARCHAR(65535)   ENCODE lzo
	,business_unit VARCHAR(65535)   ENCODE lzo
	,employee VARCHAR(65535)   ENCODE lzo
	,project_leader VARCHAR(65535)   ENCODE lzo
	,sales_unit VARCHAR(65535)   ENCODE lzo
	,sales_unit_detailed VARCHAR(65535)   ENCODE lzo
	,sales_rep VARCHAR(65535)   ENCODE lzo
	,amount NUMERIC(38,10)   ENCODE az64
	,org_num VARCHAR(65535)   ENCODE lzo
	,project_org_num VARCHAR(65535)   ENCODE lzo
	,harmonized_cust_higher_level VARCHAR(65535)   ENCODE lzo
	,harmonized_cust_lower_level VARCHAR(65535)   ENCODE lzo
	,key_accounts VARCHAR(65535)   ENCODE lzo
	,key_account_desc VARCHAR(65535)   ENCODE lzo
	,customer_verticals VARCHAR(65535)   ENCODE lzo
	,customer_veritical_desc VARCHAR(65535)   ENCODE lzo
	,customer_veritical_lkp VARCHAR(65535)   ENCODE lzo
	,project_product VARCHAR(65535)   ENCODE lzo
	,project_product_desc VARCHAR(65535)   ENCODE lzo
	,product_code_group VARCHAR(65535)   ENCODE lzo
	,project_product_group VARCHAR(65535)   ENCODE lzo
	,cost_type VARCHAR(65535)   ENCODE lzo
	,poc VARCHAR(65535)   ENCODE lzo
	,project_closing_month VARCHAR(65535)   ENCODE lzo
	,order_type VARCHAR(65535)   ENCODE lzo
	,project_type VARCHAR(65535)   ENCODE lzo
	,"account" VARCHAR(65535)   ENCODE lzo
	,account_desc VARCHAR(65535)   ENCODE lzo
	,cost_center VARCHAR(65535)   ENCODE lzo
	,cost_center_desc VARCHAR(65535)   ENCODE lzo
	,hfm_function VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.agresso_pl_trans_current owner to base_admin;


-- bods.baan_besco_cn_pl_trans_archive_current definition

-- Drop table

-- DROP TABLE bods.baan_besco_cn_pl_trans_archive_current;

--DROP TABLE bods.baan_besco_cn_pl_trans_archive_current;
CREATE TABLE IF NOT EXISTS bods.baan_besco_cn_pl_trans_archive_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id VARCHAR(65535)   ENCODE lzo
	,fiscper VARCHAR(65535)   ENCODE lzo
	,fyr_id VARCHAR(65535)   ENCODE lzo
	,fmth_nbr VARCHAR(65535)   ENCODE lzo
	,co_cd VARCHAR(65535)   ENCODE lzo
	,acct VARCHAR(65535)   ENCODE lzo
	,cost_cntr VARCHAR(65535)   ENCODE lzo
	,doc_nbr VARCHAR(65535)   ENCODE lzo
	,doc_ln_nbr VARCHAR(65535)   ENCODE lzo
	,seq_nbr VARCHAR(65535)   ENCODE lzo
	,bkgrnd_seq_nbr VARCHAR(65535)   ENCODE lzo
	,post_dte VARCHAR(65535)   ENCODE lzo
	,crncy_cd VARCHAR(65535)   ENCODE lzo
	,amt NUMERIC(38,10)   ENCODE az64
	,usd_amt NUMERIC(38,10)   ENCODE az64
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.baan_besco_cn_pl_trans_archive_current owner to base_admin;


-- bods.baan_besco_tw_pl_trans_current definition

-- Drop table

-- DROP TABLE bods.baan_besco_tw_pl_trans_current;

--DROP TABLE bods.baan_besco_tw_pl_trans_current;
CREATE TABLE IF NOT EXISTS bods.baan_besco_tw_pl_trans_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id VARCHAR(65535)   ENCODE lzo
	,fiscper VARCHAR(65535)   ENCODE lzo
	,fyr_id VARCHAR(65535)   ENCODE lzo
	,fmth_nbr VARCHAR(65535)   ENCODE lzo
	,co_cd VARCHAR(65535)   ENCODE lzo
	,acct VARCHAR(65535)   ENCODE lzo
	,cost_cntr VARCHAR(65535)   ENCODE lzo
	,doc_nbr VARCHAR(65535)   ENCODE lzo
	,doc_ln_nbr VARCHAR(65535)   ENCODE lzo
	,seq_nbr VARCHAR(65535)   ENCODE lzo
	,bkgrnd_seq_nbr VARCHAR(65535)   ENCODE lzo
	,post_dte VARCHAR(65535)   ENCODE lzo
	,crncy_cd VARCHAR(65535)   ENCODE lzo
	,amt NUMERIC(38,10)   ENCODE az64
	,usd_amt NUMERIC(38,10)   ENCODE az64
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,ins_dtm VARCHAR(65535)   ENCODE lzo
	,upd_dtm VARCHAR(65535)   ENCODE lzo
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.baan_besco_tw_pl_trans_current owner to base_admin;


-- bods.baan_powers_cn_pl_trans_archive_current definition

-- Drop table

-- DROP TABLE bods.baan_powers_cn_pl_trans_archive_current;

--DROP TABLE bods.baan_powers_cn_pl_trans_archive_current;
CREATE TABLE IF NOT EXISTS bods.baan_powers_cn_pl_trans_archive_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id BIGINT   ENCODE az64
	,fiscper BIGINT   ENCODE az64
	,"year" BIGINT   ENCODE az64
	,period BIGINT   ENCODE az64
	,cocode VARCHAR(65535)   ENCODE lzo
	,acct VARCHAR(65535)   ENCODE lzo
	,doc_num VARCHAR(65535)   ENCODE lzo
	,doc_line_num VARCHAR(65535)   ENCODE lzo
	,seq_num VARCHAR(65535)   ENCODE lzo
	,background_seq_num VARCHAR(65535)   ENCODE lzo
	,posting_date VARCHAR(65535)   ENCODE lzo
	,costctr VARCHAR(65535)   ENCODE lzo
	,amount NUMERIC(38,10)   ENCODE az64
	,usd_amount NUMERIC(38,10)   ENCODE az64
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_entity_description VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,runid BIGINT   ENCODE az64
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.baan_powers_cn_pl_trans_archive_current owner to base_admin;


-- bods.bar_acct_attr_current definition

-- Drop table

-- DROP TABLE bods.bar_acct_attr_current;

--DROP TABLE bods.bar_acct_attr_current;
CREATE TABLE IF NOT EXISTS bods.bar_acct_attr_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,bar_account VARCHAR(65535)   ENCODE lzo
	,bar_account_desc VARCHAR(65535)   ENCODE lzo
	,bar_acct_type_lvl1 VARCHAR(65535)   ENCODE lzo
	,bar_acct_type_lvl2 VARCHAR(65535)   ENCODE lzo
	,bar_acct_type_lvl3 VARCHAR(65535)   ENCODE lzo
	,bar_acct_type_lvl4 VARCHAR(65535)   ENCODE lzo
	,indirect_flag VARCHAR(65535)   ENCODE lzo
	,flipsign VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.bar_acct_attr_current owner to base_admin;


-- bods.bar_adjust_trans_current definition

-- Drop table

-- DROP TABLE bods.bar_adjust_trans_current;

--DROP TABLE bods.bar_adjust_trans_current;
CREATE TABLE IF NOT EXISTS bods.bar_adjust_trans_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id VARCHAR(65535)   ENCODE lzo
	,ticket VARCHAR(65535)   ENCODE lzo
	,note VARCHAR(65535)   ENCODE lzo
	,fiscper VARCHAR(65535)   ENCODE lzo
	,user_entity VARCHAR(65535)   ENCODE lzo
	,user_acct VARCHAR(65535)   ENCODE lzo
	,user_function VARCHAR(65535)   ENCODE lzo
	,user_custno VARCHAR(65535)   ENCODE lzo
	,user_product VARCHAR(65535)   ENCODE lzo
	,user_shipto VARCHAR(65535)   ENCODE lzo
	,user_brand VARCHAR(65535)   ENCODE lzo
	,user_amt NUMERIC(38,10)   ENCODE az64
	,data_group VARCHAR(65535)   ENCODE lzo
	,update_user VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.bar_adjust_trans_current owner to base_admin;


-- bods.bar_customer_attr_current definition

-- Drop table

-- DROP TABLE bods.bar_customer_attr_current;

--DROP TABLE bods.bar_customer_attr_current;
CREATE TABLE IF NOT EXISTS bods.bar_customer_attr_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,bar_customer VARCHAR(65535)   ENCODE lzo
	,bar_customer_desc VARCHAR(65535)   ENCODE lzo
	,bar_customer_lvl1 VARCHAR(65535)   ENCODE lzo
	,bar_customer_lvl2 VARCHAR(65535)   ENCODE lzo
	,bar_customer_lvl3 VARCHAR(65535)   ENCODE lzo
	,bar_customer_lvl4 VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.bar_customer_attr_current owner to base_admin;


-- bods.bar_entity_attr_current definition

-- Drop table

-- DROP TABLE bods.bar_entity_attr_current;

--DROP TABLE bods.bar_entity_attr_current;
CREATE TABLE IF NOT EXISTS bods.bar_entity_attr_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_entity_desc VARCHAR(65535)   ENCODE lzo
	,bar_entity_currency VARCHAR(65535)   ENCODE lzo
	,bar_entity_lvl1 VARCHAR(65535)   ENCODE lzo
	,bar_entity_lvl2 VARCHAR(65535)   ENCODE lzo
	,bar_entity_lvl3 VARCHAR(65535)   ENCODE lzo
	,bar_entity_lvl4 VARCHAR(65535)   ENCODE lzo
	,bar_entity_region VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.bar_entity_attr_current owner to base_admin;


-- bods.bar_funct_attr_current definition

-- Drop table

-- DROP TABLE bods.bar_funct_attr_current;

--DROP TABLE bods.bar_funct_attr_current;
CREATE TABLE IF NOT EXISTS bods.bar_funct_attr_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_function_grp VARCHAR(65535)   ENCODE lzo
	,functiontype VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.bar_funct_attr_current owner to base_admin;


-- bods.bar_product_attr_current definition

-- Drop table

-- DROP TABLE bods.bar_product_attr_current;

--DROP TABLE bods.bar_product_attr_current;
CREATE TABLE IF NOT EXISTS bods.bar_product_attr_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_product_desc VARCHAR(65535)   ENCODE lzo
	,bar_product_lvl1 VARCHAR(65535)   ENCODE lzo
	,bar_product_lvl2 VARCHAR(65535)   ENCODE lzo
	,bar_product_lvl3 VARCHAR(65535)   ENCODE lzo
	,bar_product_lvl4 VARCHAR(65535)   ENCODE lzo
	,bar_product_lvl5 VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.bar_product_attr_current owner to base_admin;


-- bods.byd_pl_trans_archive_current definition

-- Drop table

-- DROP TABLE bods.byd_pl_trans_archive_current;

--DROP TABLE bods.byd_pl_trans_archive_current;
CREATE TABLE IF NOT EXISTS bods.byd_pl_trans_archive_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id BIGINT   ENCODE az64
	,fiscper VARCHAR(65535)   ENCODE lzo
	,fiscal_year VARCHAR(65535)   ENCODE lzo
	,accounting_period VARCHAR(65535)   ENCODE lzo
	,company_id VARCHAR(65535)   ENCODE lzo
	,gl_account VARCHAR(65535)   ENCODE lzo
	,chart_of_accounts VARCHAR(65535)   ENCODE lzo
	,cost_center VARCHAR(65535)   ENCODE lzo
	,profit_center VARCHAR(65535)   ENCODE lzo
	,segment VARCHAR(65535)   ENCODE lzo
	,project_id VARCHAR(65535)   ENCODE lzo
	,ship_to_customer VARCHAR(65535)   ENCODE lzo
	,bill_to_customer VARCHAR(65535)   ENCODE lzo
	,payer VARCHAR(65535)   ENCODE lzo
	,product_id VARCHAR(65535)   ENCODE lzo
	,site VARCHAR(65535)   ENCODE lzo
	,ship_to_country VARCHAR(65535)   ENCODE lzo
	,cost_center_country VARCHAR(65535)   ENCODE lzo
	,gpp_div VARCHAR(65535)   ENCODE lzo
	,journal_entry VARCHAR(65535)   ENCODE lzo
	,journal_entry_item VARCHAR(65535)   ENCODE lzo
	,source_document_id VARCHAR(65535)   ENCODE lzo
	,document_type VARCHAR(65535)   ENCODE lzo
	,business_transaction_type VARCHAR(65535)   ENCODE lzo
	,posting_date VARCHAR(65535)   ENCODE lzo
	,company_currency VARCHAR(65535)   ENCODE lzo
	,amount_in_company_currency VARCHAR(38)   ENCODE lzo
	,customer_channel_code VARCHAR(65535)   ENCODE lzo
	,int_brandgrp VARCHAR(65535)   ENCODE lzo
	,int_functype VARCHAR(65535)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,bar_amt VARCHAR(38)   ENCODE lzo
	,invoiced_quantity VARCHAR(38)   ENCODE lzo
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.byd_pl_trans_archive_current owner to base_admin;


-- bods.c11_0customer_attr_current definition

-- Drop table

-- DROP TABLE bods.c11_0customer_attr_current;

--DROP TABLE bods.c11_0customer_attr_current;
CREATE TABLE IF NOT EXISTS bods.c11_0customer_attr_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,kunnr VARCHAR(65535)   ENCODE lzo
	,adrnr VARCHAR(65535)   ENCODE lzo
	,anred VARCHAR(65535)   ENCODE lzo
	,aufsd VARCHAR(65535)   ENCODE lzo
	,bahne VARCHAR(65535)   ENCODE lzo
	,bahns VARCHAR(65535)   ENCODE lzo
	,bbbnr NUMERIC(38,10)   ENCODE az64
	,bbsnr NUMERIC(38,10)   ENCODE az64
	,begru VARCHAR(65535)   ENCODE lzo
	,brsch VARCHAR(65535)   ENCODE lzo
	,bubkz NUMERIC(38,10)   ENCODE az64
	,datlt VARCHAR(65535)   ENCODE lzo
	,erdat VARCHAR(65535)   ENCODE lzo
	,ernam VARCHAR(65535)   ENCODE lzo
	,exabl VARCHAR(65535)   ENCODE lzo
	,faksd VARCHAR(65535)   ENCODE lzo
	,fiskn VARCHAR(65535)   ENCODE lzo
	,knazk VARCHAR(65535)   ENCODE lzo
	,knrza VARCHAR(65535)   ENCODE lzo
	,konzs VARCHAR(65535)   ENCODE lzo
	,ktokd VARCHAR(65535)   ENCODE lzo
	,kukla VARCHAR(65535)   ENCODE lzo
	,land1 VARCHAR(65535)   ENCODE lzo
	,lifnr VARCHAR(65535)   ENCODE lzo
	,lifsd VARCHAR(65535)   ENCODE lzo
	,locco VARCHAR(65535)   ENCODE lzo
	,loevm VARCHAR(65535)   ENCODE lzo
	,name1 VARCHAR(65535)   ENCODE lzo
	,name2 VARCHAR(65535)   ENCODE lzo
	,name3 VARCHAR(65535)   ENCODE lzo
	,name4 VARCHAR(65535)   ENCODE lzo
	,niels VARCHAR(65535)   ENCODE lzo
	,ort01 VARCHAR(65535)   ENCODE lzo
	,ort02 VARCHAR(65535)   ENCODE lzo
	,pfach VARCHAR(65535)   ENCODE lzo
	,pstl2 VARCHAR(65535)   ENCODE lzo
	,pstlz VARCHAR(65535)   ENCODE lzo
	,regio VARCHAR(65535)   ENCODE lzo
	,counc VARCHAR(65535)   ENCODE lzo
	,cityc VARCHAR(65535)   ENCODE lzo
	,rpmkr VARCHAR(65535)   ENCODE lzo
	,sortl VARCHAR(65535)   ENCODE lzo
	,sperr VARCHAR(65535)   ENCODE lzo
	,spras VARCHAR(65535)   ENCODE lzo
	,stcd1 VARCHAR(65535)   ENCODE lzo
	,stcd2 VARCHAR(65535)   ENCODE lzo
	,stkza VARCHAR(65535)   ENCODE lzo
	,stkzu VARCHAR(65535)   ENCODE lzo
	,stras VARCHAR(65535)   ENCODE lzo
	,telbx VARCHAR(65535)   ENCODE lzo
	,telf1 VARCHAR(65535)   ENCODE lzo
	,telf2 VARCHAR(65535)   ENCODE lzo
	,telfx VARCHAR(65535)   ENCODE lzo
	,teltx VARCHAR(65535)   ENCODE lzo
	,telx1 VARCHAR(65535)   ENCODE lzo
	,lzone VARCHAR(65535)   ENCODE lzo
	,xcpdk VARCHAR(65535)   ENCODE lzo
	,xzemp VARCHAR(65535)   ENCODE lzo
	,vbund VARCHAR(65535)   ENCODE lzo
	,stceg VARCHAR(65535)   ENCODE lzo
	,dear1 VARCHAR(65535)   ENCODE lzo
	,dear2 VARCHAR(65535)   ENCODE lzo
	,dear3 VARCHAR(65535)   ENCODE lzo
	,dear4 VARCHAR(65535)   ENCODE lzo
	,dear5 VARCHAR(65535)   ENCODE lzo
	,dear6 VARCHAR(65535)   ENCODE lzo
	,gform VARCHAR(65535)   ENCODE lzo
	,bran1 VARCHAR(65535)   ENCODE lzo
	,bran2 VARCHAR(65535)   ENCODE lzo
	,bran3 VARCHAR(65535)   ENCODE lzo
	,bran4 VARCHAR(65535)   ENCODE lzo
	,bran5 VARCHAR(65535)   ENCODE lzo
	,ekont VARCHAR(65535)   ENCODE lzo
	,umsat NUMERIC(38,10)   ENCODE az64
	,umjah NUMERIC(38,10)   ENCODE az64
	,uwaer VARCHAR(65535)   ENCODE lzo
	,jmzah NUMERIC(38,10)   ENCODE az64
	,jmjah NUMERIC(38,10)   ENCODE az64
	,katr1 VARCHAR(65535)   ENCODE lzo
	,katr2 VARCHAR(65535)   ENCODE lzo
	,katr3 VARCHAR(65535)   ENCODE lzo
	,katr4 VARCHAR(65535)   ENCODE lzo
	,katr5 VARCHAR(65535)   ENCODE lzo
	,katr6 VARCHAR(65535)   ENCODE lzo
	,katr7 VARCHAR(65535)   ENCODE lzo
	,katr8 VARCHAR(65535)   ENCODE lzo
	,katr9 VARCHAR(65535)   ENCODE lzo
	,katr10 VARCHAR(65535)   ENCODE lzo
	,stkzn VARCHAR(65535)   ENCODE lzo
	,umsa1 NUMERIC(38,10)   ENCODE az64
	,txjcd VARCHAR(65535)   ENCODE lzo
	,mcod1 VARCHAR(65535)   ENCODE lzo
	,mcod2 VARCHAR(65535)   ENCODE lzo
	,mcod3 VARCHAR(65535)   ENCODE lzo
	,periv VARCHAR(65535)   ENCODE lzo
	,abrvw VARCHAR(65535)   ENCODE lzo
	,inspbydebi VARCHAR(65535)   ENCODE lzo
	,inspatdebi VARCHAR(65535)   ENCODE lzo
	,ktocd VARCHAR(65535)   ENCODE lzo
	,pfort VARCHAR(65535)   ENCODE lzo
	,werks VARCHAR(65535)   ENCODE lzo
	,dtams VARCHAR(65535)   ENCODE lzo
	,dtaws VARCHAR(65535)   ENCODE lzo
	,duefl VARCHAR(65535)   ENCODE lzo
	,hzuor NUMERIC(38,10)   ENCODE az64
	,sperz VARCHAR(65535)   ENCODE lzo
	,etikg VARCHAR(65535)   ENCODE lzo
	,civve VARCHAR(65535)   ENCODE lzo
	,milve VARCHAR(65535)   ENCODE lzo
	,kdkg1 VARCHAR(65535)   ENCODE lzo
	,kdkg2 VARCHAR(65535)   ENCODE lzo
	,kdkg3 VARCHAR(65535)   ENCODE lzo
	,kdkg4 VARCHAR(65535)   ENCODE lzo
	,kdkg5 VARCHAR(65535)   ENCODE lzo
	,xknza VARCHAR(65535)   ENCODE lzo
	,fityp VARCHAR(65535)   ENCODE lzo
	,stcdt VARCHAR(65535)   ENCODE lzo
	,stcd3 VARCHAR(65535)   ENCODE lzo
	,stcd4 VARCHAR(65535)   ENCODE lzo
	,xicms VARCHAR(65535)   ENCODE lzo
	,xxipi VARCHAR(65535)   ENCODE lzo
	,xsubt VARCHAR(65535)   ENCODE lzo
	,cfopc VARCHAR(65535)   ENCODE lzo
	,txlw1 VARCHAR(65535)   ENCODE lzo
	,txlw2 VARCHAR(65535)   ENCODE lzo
	,ccc01 VARCHAR(65535)   ENCODE lzo
	,ccc02 VARCHAR(65535)   ENCODE lzo
	,ccc03 VARCHAR(65535)   ENCODE lzo
	,ccc04 VARCHAR(65535)   ENCODE lzo
	,cassd VARCHAR(65535)   ENCODE lzo
	,knurl VARCHAR(65535)   ENCODE lzo
	,zzemail VARCHAR(65535)   ENCODE lzo
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.c11_0customer_attr_current owner to base_admin;


-- bods.c11_0ec_pca3_current definition

-- Drop table

-- DROP TABLE bods.c11_0ec_pca3_current;

--DROP TABLE bods.c11_0ec_pca3_current;
CREATE TABLE IF NOT EXISTS bods.c11_0ec_pca3_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id BIGINT   ENCODE az64
	,"year" VARCHAR(65535)   ENCODE lzo
	,period VARCHAR(65535)   ENCODE lzo
	,cocode VARCHAR(65535)   ENCODE lzo
	,busarea VARCHAR(65535)   ENCODE lzo
	,costctr VARCHAR(65535)   ENCODE lzo
	,acct VARCHAR(65535)   ENCODE lzo
	,int_entitytype VARCHAR(65535)   ENCODE lzo
	,int_functype VARCHAR(65535)   ENCODE lzo
	,docct VARCHAR(65535)   ENCODE lzo
	,docno VARCHAR(65535)   ENCODE lzo
	,docline VARCHAR(65535)   ENCODE lzo
	,curtype VARCHAR(65535)   ENCODE lzo
	,postdate VARCHAR(65535)   ENCODE lzo
	,salesgrp VARCHAR(65535)   ENCODE lzo
	,salesoff VARCHAR(65535)   ENCODE lzo
	,material VARCHAR(65535)   ENCODE lzo
	,payer VARCHAR(65535)   ENCODE lzo
	,shiptocust VARCHAR(65535)   ENCODE lzo
	,soldtocust VARCHAR(65535)   ENCODE lzo
	,currkey VARCHAR(65535)   ENCODE lzo
	,amt NUMERIC(38,10)   ENCODE az64
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,quanunit VARCHAR(65535)   ENCODE lzo
	,quantity NUMERIC(38,10)   ENCODE az64
	,werks VARCHAR(65535)   ENCODE lzo
	,rprctr VARCHAR(65535)   ENCODE lzo
	,brand_code VARCHAR(65535)   ENCODE lzo
	,bwtar VARCHAR(65535)   ENCODE lzo
	,class_code VARCHAR(65535)   ENCODE lzo
	,credit NUMERIC(38,10)   ENCODE az64
	,debit NUMERIC(38,10)   ENCODE az64
	,distribution_channel VARCHAR(65535)   ENCODE lzo
	,hierarchy VARCHAR(65535)   ENCODE lzo
	,poper NUMERIC(38,10)   ENCODE az64
	,product VARCHAR(65535)   ENCODE lzo
	,refactiv VARCHAR(65535)   ENCODE lzo
	,refdocct VARCHAR(65535)   ENCODE lzo
	,refdocline VARCHAR(65535)   ENCODE lzo
	,refdocln VARCHAR(65535)   ENCODE lzo
	,refdocnr VARCHAR(65535)   ENCODE lzo
	,refdocnum VARCHAR(65535)   ENCODE lzo
	,refryear VARCHAR(65535)   ENCODE lzo
	,rtcur VARCHAR(65535)   ENCODE lzo
	,salesgrp_lkp VARCHAR(65535)   ENCODE lzo
	,salesoff_lkp VARCHAR(65535)   ENCODE lzo
	,salesorg VARCHAR(65535)   ENCODE lzo
	,zzdmdgroup VARCHAR(65535)   ENCODE lzo
	,zzhier VARCHAR(65535)   ENCODE lzo
	,zzitmcat VARCHAR(65535)   ENCODE lzo
	,zzorigsorg VARCHAR(65535)   ENCODE lzo
	,zzpayer VARCHAR(65535)   ENCODE lzo
	,zzshipto VARCHAR(65535)   ENCODE lzo
	,zzsoldto VARCHAR(65535)   ENCODE lzo
	,src_id VARCHAR(65535)   ENCODE lzo
	,rvers VARCHAR(65535)   ENCODE lzo
	,rhoart NUMERIC(38,10)   ENCODE az64
	,rfarea VARCHAR(65535)   ENCODE lzo
	,kokrs VARCHAR(65535)   ENCODE lzo
	,hrkft VARCHAR(65535)   ENCODE lzo
	,rassc VARCHAR(65535)   ENCODE lzo
	,eprctr VARCHAR(65535)   ENCODE lzo
	,activ VARCHAR(65535)   ENCODE lzo
	,afabe NUMERIC(38,10)   ENCODE az64
	,oclnt NUMERIC(38,10)   ENCODE az64
	,sbukrs VARCHAR(65535)   ENCODE lzo
	,sprctr VARCHAR(65535)   ENCODE lzo
	,shoart NUMERIC(38,10)   ENCODE az64
	,sfarea VARCHAR(65535)   ENCODE lzo
	,cpudt VARCHAR(65535)   ENCODE lzo
	,cputm VARCHAR(65535)   ENCODE lzo
	,usnam VARCHAR(65535)   ENCODE lzo
	,sgtxt VARCHAR(65535)   ENCODE lzo
	,autom VARCHAR(65535)   ENCODE lzo
	,docty VARCHAR(65535)   ENCODE lzo
	,bldat VARCHAR(65535)   ENCODE lzo
	,wsdat VARCHAR(65535)   ENCODE lzo
	,awtyp VARCHAR(65535)   ENCODE lzo
	,aworg VARCHAR(65535)   ENCODE lzo
	,lstar VARCHAR(65535)   ENCODE lzo
	,aufnr VARCHAR(65535)   ENCODE lzo
	,aufpl VARCHAR(65535)   ENCODE lzo
	,anln1 VARCHAR(65535)   ENCODE lzo
	,anln2 VARCHAR(65535)   ENCODE lzo
	,bwkey VARCHAR(65535)   ENCODE lzo
	,anbwa VARCHAR(65535)   ENCODE lzo
	,lifnr VARCHAR(65535)   ENCODE lzo
	,rmvct VARCHAR(65535)   ENCODE lzo
	,ebeln VARCHAR(65535)   ENCODE lzo
	,ebelp VARCHAR(65535)   ENCODE lzo
	,kstrg VARCHAR(65535)   ENCODE lzo
	,erkrs VARCHAR(65535)   ENCODE lzo
	,paobjnr VARCHAR(65535)   ENCODE lzo
	,pasubnr NUMERIC(38,10)   ENCODE az64
	,ps_psp_pnr NUMERIC(38,10)   ENCODE az64
	,kdauf VARCHAR(65535)   ENCODE lzo
	,kdpos VARCHAR(65535)   ENCODE lzo
	,fkart VARCHAR(65535)   ENCODE lzo
	,aubel VARCHAR(65535)   ENCODE lzo
	,aupos VARCHAR(65535)   ENCODE lzo
	,spart VARCHAR(65535)   ENCODE lzo
	,vbeln VARCHAR(65535)   ENCODE lzo
	,posnr VARCHAR(65535)   ENCODE lzo
	,vbund VARCHAR(65535)   ENCODE lzo
	,logsys VARCHAR(65535)   ENCODE lzo
	,alebn VARCHAR(65535)   ENCODE lzo
	,awsys VARCHAR(65535)   ENCODE lzo
	,versa VARCHAR(65535)   ENCODE lzo
	,stflg VARCHAR(65535)   ENCODE lzo
	,stokz VARCHAR(65535)   ENCODE lzo
	,rep_matnr VARCHAR(65535)   ENCODE lzo
	,co_prznr VARCHAR(65535)   ENCODE lzo
	,imkey VARCHAR(65535)   ENCODE lzo
	,dabrz VARCHAR(65535)   ENCODE lzo
	,valut VARCHAR(65535)   ENCODE lzo
	,rscope VARCHAR(65535)   ENCODE lzo
	,awref_rev VARCHAR(65535)   ENCODE lzo
	,aworg_rev VARCHAR(65535)   ENCODE lzo
	,bwart VARCHAR(65535)   ENCODE lzo
	,blart BIGINT   ENCODE az64
	,timestmp VARCHAR(65535)   ENCODE lzo
	,valuetype NUMERIC(38,10)   ENCODE az64
	,chartaccts VARCHAR(65535)   ENCODE lzo
	,upmod VARCHAR(65535)   ENCODE lzo
	,valutyp VARCHAR(65535)   ENCODE lzo
	,runid BIGINT   ENCODE az64
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.c11_0ec_pca3_current owner to base_admin;


-- bods.c11_0ec_pca3_current_20210423 definition

-- Drop table

-- DROP TABLE bods.c11_0ec_pca3_current_20210423;

--DROP TABLE bods.c11_0ec_pca3_current_20210423;
CREATE TABLE IF NOT EXISTS bods.c11_0ec_pca3_current_20210423
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id BIGINT   ENCODE az64
	,"year" VARCHAR(65535)   ENCODE lzo
	,period VARCHAR(65535)   ENCODE lzo
	,cocode VARCHAR(65535)   ENCODE lzo
	,busarea VARCHAR(65535)   ENCODE lzo
	,costctr VARCHAR(65535)   ENCODE lzo
	,acct VARCHAR(65535)   ENCODE lzo
	,int_entitytype VARCHAR(65535)   ENCODE lzo
	,int_functype VARCHAR(65535)   ENCODE lzo
	,docct VARCHAR(65535)   ENCODE lzo
	,docno VARCHAR(65535)   ENCODE lzo
	,docline VARCHAR(65535)   ENCODE lzo
	,curtype VARCHAR(65535)   ENCODE lzo
	,postdate VARCHAR(65535)   ENCODE lzo
	,salesgrp VARCHAR(65535)   ENCODE lzo
	,salesoff VARCHAR(65535)   ENCODE lzo
	,material VARCHAR(65535)   ENCODE lzo
	,payer VARCHAR(65535)   ENCODE lzo
	,shiptocust VARCHAR(65535)   ENCODE lzo
	,soldtocust VARCHAR(65535)   ENCODE lzo
	,currkey VARCHAR(65535)   ENCODE lzo
	,amt NUMERIC(38,10)   ENCODE az64
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,quanunit VARCHAR(65535)   ENCODE lzo
	,quantity NUMERIC(38,10)   ENCODE az64
	,werks VARCHAR(65535)   ENCODE lzo
	,rprctr VARCHAR(65535)   ENCODE lzo
	,brand_code VARCHAR(65535)   ENCODE lzo
	,bwtar VARCHAR(65535)   ENCODE lzo
	,class_code VARCHAR(65535)   ENCODE lzo
	,credit NUMERIC(38,10)   ENCODE az64
	,debit NUMERIC(38,10)   ENCODE az64
	,distribution_channel VARCHAR(65535)   ENCODE lzo
	,hierarchy VARCHAR(65535)   ENCODE lzo
	,poper NUMERIC(38,10)   ENCODE az64
	,product VARCHAR(65535)   ENCODE lzo
	,refactiv VARCHAR(65535)   ENCODE lzo
	,refdocct VARCHAR(65535)   ENCODE lzo
	,refdocline VARCHAR(65535)   ENCODE lzo
	,refdocln VARCHAR(65535)   ENCODE lzo
	,refdocnr VARCHAR(65535)   ENCODE lzo
	,refdocnum VARCHAR(65535)   ENCODE lzo
	,refryear VARCHAR(65535)   ENCODE lzo
	,rtcur VARCHAR(65535)   ENCODE lzo
	,salesgrp_lkp VARCHAR(65535)   ENCODE lzo
	,salesoff_lkp VARCHAR(65535)   ENCODE lzo
	,salesorg VARCHAR(65535)   ENCODE lzo
	,zzdmdgroup VARCHAR(65535)   ENCODE lzo
	,zzhier VARCHAR(65535)   ENCODE lzo
	,zzitmcat VARCHAR(65535)   ENCODE lzo
	,zzorigsorg VARCHAR(65535)   ENCODE lzo
	,zzpayer VARCHAR(65535)   ENCODE lzo
	,zzshipto VARCHAR(65535)   ENCODE lzo
	,zzsoldto VARCHAR(65535)   ENCODE lzo
	,src_id VARCHAR(65535)   ENCODE lzo
	,rvers VARCHAR(65535)   ENCODE lzo
	,rhoart NUMERIC(38,10)   ENCODE az64
	,rfarea VARCHAR(65535)   ENCODE lzo
	,kokrs VARCHAR(65535)   ENCODE lzo
	,hrkft VARCHAR(65535)   ENCODE lzo
	,rassc VARCHAR(65535)   ENCODE lzo
	,eprctr VARCHAR(65535)   ENCODE lzo
	,activ VARCHAR(65535)   ENCODE lzo
	,afabe NUMERIC(38,10)   ENCODE az64
	,oclnt NUMERIC(38,10)   ENCODE az64
	,sbukrs VARCHAR(65535)   ENCODE lzo
	,sprctr VARCHAR(65535)   ENCODE lzo
	,shoart NUMERIC(38,10)   ENCODE az64
	,sfarea VARCHAR(65535)   ENCODE lzo
	,cpudt VARCHAR(65535)   ENCODE lzo
	,cputm VARCHAR(65535)   ENCODE lzo
	,usnam VARCHAR(65535)   ENCODE lzo
	,sgtxt VARCHAR(65535)   ENCODE lzo
	,autom VARCHAR(65535)   ENCODE lzo
	,docty VARCHAR(65535)   ENCODE lzo
	,bldat VARCHAR(65535)   ENCODE lzo
	,wsdat VARCHAR(65535)   ENCODE lzo
	,awtyp VARCHAR(65535)   ENCODE lzo
	,aworg VARCHAR(65535)   ENCODE lzo
	,lstar VARCHAR(65535)   ENCODE lzo
	,aufnr VARCHAR(65535)   ENCODE lzo
	,aufpl VARCHAR(65535)   ENCODE lzo
	,anln1 VARCHAR(65535)   ENCODE lzo
	,anln2 VARCHAR(65535)   ENCODE lzo
	,bwkey VARCHAR(65535)   ENCODE lzo
	,anbwa VARCHAR(65535)   ENCODE lzo
	,lifnr VARCHAR(65535)   ENCODE lzo
	,rmvct VARCHAR(65535)   ENCODE lzo
	,ebeln VARCHAR(65535)   ENCODE lzo
	,ebelp VARCHAR(65535)   ENCODE lzo
	,kstrg VARCHAR(65535)   ENCODE lzo
	,erkrs VARCHAR(65535)   ENCODE lzo
	,paobjnr VARCHAR(65535)   ENCODE lzo
	,pasubnr NUMERIC(38,10)   ENCODE az64
	,ps_psp_pnr NUMERIC(38,10)   ENCODE az64
	,kdauf VARCHAR(65535)   ENCODE lzo
	,kdpos VARCHAR(65535)   ENCODE lzo
	,fkart VARCHAR(65535)   ENCODE lzo
	,aubel VARCHAR(65535)   ENCODE lzo
	,aupos VARCHAR(65535)   ENCODE lzo
	,spart VARCHAR(65535)   ENCODE lzo
	,vbeln VARCHAR(65535)   ENCODE lzo
	,posnr VARCHAR(65535)   ENCODE lzo
	,vbund VARCHAR(65535)   ENCODE lzo
	,logsys VARCHAR(65535)   ENCODE lzo
	,alebn VARCHAR(65535)   ENCODE lzo
	,awsys VARCHAR(65535)   ENCODE lzo
	,versa VARCHAR(65535)   ENCODE lzo
	,stflg VARCHAR(65535)   ENCODE lzo
	,stokz VARCHAR(65535)   ENCODE lzo
	,rep_matnr VARCHAR(65535)   ENCODE lzo
	,co_prznr VARCHAR(65535)   ENCODE lzo
	,imkey VARCHAR(65535)   ENCODE lzo
	,dabrz VARCHAR(65535)   ENCODE lzo
	,valut VARCHAR(65535)   ENCODE lzo
	,rscope VARCHAR(65535)   ENCODE lzo
	,awref_rev VARCHAR(65535)   ENCODE lzo
	,aworg_rev VARCHAR(65535)   ENCODE lzo
	,bwart VARCHAR(65535)   ENCODE lzo
	,blart BIGINT   ENCODE az64
	,timestmp VARCHAR(65535)   ENCODE lzo
	,valuetype NUMERIC(38,10)   ENCODE az64
	,chartaccts VARCHAR(65535)   ENCODE lzo
	,upmod VARCHAR(65535)   ENCODE lzo
	,valutyp VARCHAR(65535)   ENCODE lzo
	,runid BIGINT   ENCODE az64
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.c11_0ec_pca3_current_20210423 owner to base_admin;


-- bods.c11_0ec_pca3_current_t1 definition

-- Drop table

-- DROP TABLE bods.c11_0ec_pca3_current_t1;

--DROP TABLE bods.c11_0ec_pca3_current_t1;
CREATE TABLE IF NOT EXISTS bods.c11_0ec_pca3_current_t1
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id BIGINT   ENCODE az64
	,"year" VARCHAR(65535)   ENCODE lzo
	,period VARCHAR(65535)   ENCODE lzo
	,cocode VARCHAR(65535)   ENCODE lzo
	,busarea VARCHAR(65535)   ENCODE lzo
	,costctr VARCHAR(65535)   ENCODE lzo
	,acct VARCHAR(65535)   ENCODE lzo
	,int_entitytype VARCHAR(65535)   ENCODE lzo
	,int_functype VARCHAR(65535)   ENCODE lzo
	,docct VARCHAR(65535)   ENCODE lzo
	,docno VARCHAR(65535)   ENCODE lzo
	,docline VARCHAR(65535)   ENCODE lzo
	,curtype VARCHAR(65535)   ENCODE lzo
	,postdate VARCHAR(65535)   ENCODE lzo
	,salesgrp VARCHAR(65535)   ENCODE lzo
	,salesoff VARCHAR(65535)   ENCODE lzo
	,material VARCHAR(65535)   ENCODE lzo
	,payer VARCHAR(65535)   ENCODE lzo
	,shiptocust VARCHAR(65535)   ENCODE lzo
	,soldtocust VARCHAR(65535)   ENCODE lzo
	,currkey VARCHAR(65535)   ENCODE lzo
	,amt NUMERIC(38,10)   ENCODE az64
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,quanunit VARCHAR(65535)   ENCODE lzo
	,quantity NUMERIC(38,10)   ENCODE az64
	,werks VARCHAR(65535)   ENCODE lzo
	,rprctr VARCHAR(65535)   ENCODE lzo
	,brand_code VARCHAR(65535)   ENCODE lzo
	,bwtar VARCHAR(65535)   ENCODE lzo
	,class_code VARCHAR(65535)   ENCODE lzo
	,credit NUMERIC(38,10)   ENCODE az64
	,debit NUMERIC(38,10)   ENCODE az64
	,distribution_channel VARCHAR(65535)   ENCODE lzo
	,hierarchy VARCHAR(65535)   ENCODE lzo
	,poper NUMERIC(38,10)   ENCODE az64
	,product VARCHAR(65535)   ENCODE lzo
	,refactiv VARCHAR(65535)   ENCODE lzo
	,refdocct VARCHAR(65535)   ENCODE lzo
	,refdocline VARCHAR(65535)   ENCODE lzo
	,refdocln VARCHAR(65535)   ENCODE lzo
	,refdocnr VARCHAR(65535)   ENCODE lzo
	,refdocnum VARCHAR(65535)   ENCODE lzo
	,refryear VARCHAR(65535)   ENCODE lzo
	,rtcur VARCHAR(65535)   ENCODE lzo
	,salesgrp_lkp VARCHAR(65535)   ENCODE lzo
	,salesoff_lkp VARCHAR(65535)   ENCODE lzo
	,salesorg VARCHAR(65535)   ENCODE lzo
	,zzdmdgroup VARCHAR(65535)   ENCODE lzo
	,zzhier VARCHAR(65535)   ENCODE lzo
	,zzitmcat VARCHAR(65535)   ENCODE lzo
	,zzorigsorg VARCHAR(65535)   ENCODE lzo
	,zzpayer VARCHAR(65535)   ENCODE lzo
	,zzshipto VARCHAR(65535)   ENCODE lzo
	,zzsoldto VARCHAR(65535)   ENCODE lzo
	,src_id VARCHAR(65535)   ENCODE lzo
	,rvers VARCHAR(65535)   ENCODE lzo
	,rhoart NUMERIC(38,10)   ENCODE az64
	,rfarea VARCHAR(65535)   ENCODE lzo
	,kokrs VARCHAR(65535)   ENCODE lzo
	,hrkft VARCHAR(65535)   ENCODE lzo
	,rassc VARCHAR(65535)   ENCODE lzo
	,eprctr VARCHAR(65535)   ENCODE lzo
	,activ VARCHAR(65535)   ENCODE lzo
	,afabe NUMERIC(38,10)   ENCODE az64
	,oclnt NUMERIC(38,10)   ENCODE az64
	,sbukrs VARCHAR(65535)   ENCODE lzo
	,sprctr VARCHAR(65535)   ENCODE lzo
	,shoart NUMERIC(38,10)   ENCODE az64
	,sfarea VARCHAR(65535)   ENCODE lzo
	,cpudt VARCHAR(65535)   ENCODE lzo
	,cputm VARCHAR(65535)   ENCODE lzo
	,usnam VARCHAR(65535)   ENCODE lzo
	,sgtxt VARCHAR(65535)   ENCODE lzo
	,autom VARCHAR(65535)   ENCODE lzo
	,docty VARCHAR(65535)   ENCODE lzo
	,bldat VARCHAR(65535)   ENCODE lzo
	,wsdat VARCHAR(65535)   ENCODE lzo
	,awtyp VARCHAR(65535)   ENCODE lzo
	,aworg VARCHAR(65535)   ENCODE lzo
	,lstar VARCHAR(65535)   ENCODE lzo
	,aufnr VARCHAR(65535)   ENCODE lzo
	,aufpl VARCHAR(65535)   ENCODE lzo
	,anln1 VARCHAR(65535)   ENCODE lzo
	,anln2 VARCHAR(65535)   ENCODE lzo
	,bwkey VARCHAR(65535)   ENCODE lzo
	,anbwa VARCHAR(65535)   ENCODE lzo
	,lifnr VARCHAR(65535)   ENCODE lzo
	,rmvct VARCHAR(65535)   ENCODE lzo
	,ebeln VARCHAR(65535)   ENCODE lzo
	,ebelp VARCHAR(65535)   ENCODE lzo
	,kstrg VARCHAR(65535)   ENCODE lzo
	,erkrs VARCHAR(65535)   ENCODE lzo
	,paobjnr VARCHAR(65535)   ENCODE lzo
	,pasubnr NUMERIC(38,10)   ENCODE az64
	,ps_psp_pnr NUMERIC(38,10)   ENCODE az64
	,kdauf VARCHAR(65535)   ENCODE lzo
	,kdpos VARCHAR(65535)   ENCODE lzo
	,fkart VARCHAR(65535)   ENCODE lzo
	,aubel VARCHAR(65535)   ENCODE lzo
	,aupos VARCHAR(65535)   ENCODE lzo
	,spart VARCHAR(65535)   ENCODE lzo
	,vbeln VARCHAR(65535)   ENCODE lzo
	,posnr VARCHAR(65535)   ENCODE lzo
	,vbund VARCHAR(65535)   ENCODE lzo
	,logsys VARCHAR(65535)   ENCODE lzo
	,alebn VARCHAR(65535)   ENCODE lzo
	,awsys VARCHAR(65535)   ENCODE lzo
	,versa VARCHAR(65535)   ENCODE lzo
	,stflg VARCHAR(65535)   ENCODE lzo
	,stokz VARCHAR(65535)   ENCODE lzo
	,rep_matnr VARCHAR(65535)   ENCODE lzo
	,co_prznr VARCHAR(65535)   ENCODE lzo
	,imkey VARCHAR(65535)   ENCODE lzo
	,dabrz VARCHAR(65535)   ENCODE lzo
	,valut VARCHAR(65535)   ENCODE lzo
	,rscope VARCHAR(65535)   ENCODE lzo
	,awref_rev VARCHAR(65535)   ENCODE lzo
	,aworg_rev VARCHAR(65535)   ENCODE lzo
	,bwart VARCHAR(65535)   ENCODE lzo
	,blart BIGINT   ENCODE az64
	,timestmp VARCHAR(65535)   ENCODE lzo
	,valuetype NUMERIC(38,10)   ENCODE az64
	,chartaccts VARCHAR(65535)   ENCODE lzo
	,upmod VARCHAR(65535)   ENCODE lzo
	,valutyp VARCHAR(65535)   ENCODE lzo
	,runid BIGINT   ENCODE az64
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE EVEN
;
ALTER TABLE bods.c11_0ec_pca3_current_t1 owner to base_admin;


-- bods.c11_0material_attr_current definition

-- Drop table

-- DROP TABLE bods.c11_0material_attr_current;

--DROP TABLE bods.c11_0material_attr_current;
CREATE TABLE IF NOT EXISTS bods.c11_0material_attr_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,matnr VARCHAR(65535)   ENCODE lzo
	,ersda VARCHAR(65535)   ENCODE lzo
	,ernam VARCHAR(65535)   ENCODE lzo
	,laeda VARCHAR(65535)   ENCODE lzo
	,aenam VARCHAR(65535)   ENCODE lzo
	,vpsta VARCHAR(65535)   ENCODE lzo
	,pstat VARCHAR(65535)   ENCODE lzo
	,lvorm VARCHAR(65535)   ENCODE lzo
	,mtart VARCHAR(65535)   ENCODE lzo
	,mbrsh VARCHAR(65535)   ENCODE lzo
	,matkl VARCHAR(65535)   ENCODE lzo
	,bismt VARCHAR(65535)   ENCODE lzo
	,meins VARCHAR(65535)   ENCODE lzo
	,bstme VARCHAR(65535)   ENCODE lzo
	,zeinr VARCHAR(65535)   ENCODE lzo
	,zeiar VARCHAR(65535)   ENCODE lzo
	,zeivr VARCHAR(65535)   ENCODE lzo
	,zeifo VARCHAR(65535)   ENCODE lzo
	,aeszn VARCHAR(65535)   ENCODE lzo
	,blatt VARCHAR(65535)   ENCODE lzo
	,blanz NUMERIC(38,10)   ENCODE az64
	,ferth VARCHAR(65535)   ENCODE lzo
	,formt VARCHAR(65535)   ENCODE lzo
	,groes VARCHAR(65535)   ENCODE lzo
	,wrkst VARCHAR(65535)   ENCODE lzo
	,normt VARCHAR(65535)   ENCODE lzo
	,labor VARCHAR(65535)   ENCODE lzo
	,ekwsl VARCHAR(65535)   ENCODE lzo
	,brgew NUMERIC(38,10)   ENCODE az64
	,ntgew NUMERIC(38,10)   ENCODE az64
	,gewei VARCHAR(65535)   ENCODE lzo
	,volum NUMERIC(38,10)   ENCODE az64
	,voleh VARCHAR(65535)   ENCODE lzo
	,behvo VARCHAR(65535)   ENCODE lzo
	,raube VARCHAR(65535)   ENCODE lzo
	,tempb VARCHAR(65535)   ENCODE lzo
	,disst VARCHAR(65535)   ENCODE lzo
	,tragr VARCHAR(65535)   ENCODE lzo
	,stoff VARCHAR(65535)   ENCODE lzo
	,spart VARCHAR(65535)   ENCODE lzo
	,kunnr VARCHAR(65535)   ENCODE lzo
	,eannr VARCHAR(65535)   ENCODE lzo
	,wesch NUMERIC(38,10)   ENCODE az64
	,bwvor VARCHAR(65535)   ENCODE lzo
	,bwscl VARCHAR(65535)   ENCODE lzo
	,saiso VARCHAR(65535)   ENCODE lzo
	,etiar VARCHAR(65535)   ENCODE lzo
	,etifo VARCHAR(65535)   ENCODE lzo
	,entar VARCHAR(65535)   ENCODE lzo
	,ean11 VARCHAR(65535)   ENCODE lzo
	,numtp VARCHAR(65535)   ENCODE lzo
	,laeng NUMERIC(38,10)   ENCODE az64
	,breit NUMERIC(38,10)   ENCODE az64
	,hoehe NUMERIC(38,10)   ENCODE az64
	,meabm VARCHAR(65535)   ENCODE lzo
	,prdha VARCHAR(65535)   ENCODE lzo
	,aeklk VARCHAR(65535)   ENCODE lzo
	,cadkz VARCHAR(65535)   ENCODE lzo
	,qmpur VARCHAR(65535)   ENCODE lzo
	,ergew NUMERIC(38,10)   ENCODE az64
	,ergei VARCHAR(65535)   ENCODE lzo
	,ervol NUMERIC(38,10)   ENCODE az64
	,ervoe VARCHAR(65535)   ENCODE lzo
	,gewto NUMERIC(38,10)   ENCODE az64
	,volto NUMERIC(38,10)   ENCODE az64
	,vabme VARCHAR(65535)   ENCODE lzo
	,kzrev VARCHAR(65535)   ENCODE lzo
	,kzkfg VARCHAR(65535)   ENCODE lzo
	,xchpf VARCHAR(65535)   ENCODE lzo
	,vhart VARCHAR(65535)   ENCODE lzo
	,fuelg NUMERIC(38,10)   ENCODE az64
	,stfak NUMERIC(38,10)   ENCODE az64
	,magrv VARCHAR(65535)   ENCODE lzo
	,begru VARCHAR(65535)   ENCODE lzo
	,datab VARCHAR(65535)   ENCODE lzo
	,liqdt VARCHAR(65535)   ENCODE lzo
	,saisj VARCHAR(65535)   ENCODE lzo
	,plgtp VARCHAR(65535)   ENCODE lzo
	,mlgut VARCHAR(65535)   ENCODE lzo
	,extwg VARCHAR(65535)   ENCODE lzo
	,satnr VARCHAR(65535)   ENCODE lzo
	,attyp VARCHAR(65535)   ENCODE lzo
	,kzkup VARCHAR(65535)   ENCODE lzo
	,kznfm VARCHAR(65535)   ENCODE lzo
	,pmata VARCHAR(65535)   ENCODE lzo
	,mstae VARCHAR(65535)   ENCODE lzo
	,mstav VARCHAR(65535)   ENCODE lzo
	,mstde VARCHAR(65535)   ENCODE lzo
	,mstdv VARCHAR(65535)   ENCODE lzo
	,taklv VARCHAR(65535)   ENCODE lzo
	,rbnrm VARCHAR(65535)   ENCODE lzo
	,mhdrz NUMERIC(38,10)   ENCODE az64
	,mhdhb NUMERIC(38,10)   ENCODE az64
	,mhdlp NUMERIC(38,10)   ENCODE az64
	,inhme VARCHAR(65535)   ENCODE lzo
	,inhal NUMERIC(38,10)   ENCODE az64
	,vpreh NUMERIC(38,10)   ENCODE az64
	,etiag VARCHAR(65535)   ENCODE lzo
	,inhbr NUMERIC(38,10)   ENCODE az64
	,cmeth VARCHAR(65535)   ENCODE lzo
	,cuobf NUMERIC(38,10)   ENCODE az64
	,kzumw VARCHAR(65535)   ENCODE lzo
	,kosch VARCHAR(65535)   ENCODE lzo
	,sprof VARCHAR(65535)   ENCODE lzo
	,nrfhg VARCHAR(65535)   ENCODE lzo
	,mfrpn VARCHAR(65535)   ENCODE lzo
	,mfrnr VARCHAR(65535)   ENCODE lzo
	,bmatn VARCHAR(65535)   ENCODE lzo
	,mprof VARCHAR(65535)   ENCODE lzo
	,kzwsm VARCHAR(65535)   ENCODE lzo
	,saity VARCHAR(65535)   ENCODE lzo
	,profl VARCHAR(65535)   ENCODE lzo
	,ihivi VARCHAR(65535)   ENCODE lzo
	,iloos VARCHAR(65535)   ENCODE lzo
	,serlv VARCHAR(65535)   ENCODE lzo
	,kzgvh VARCHAR(65535)   ENCODE lzo
	,xgchp VARCHAR(65535)   ENCODE lzo
	,kzeff VARCHAR(65535)   ENCODE lzo
	,compl NUMERIC(38,10)   ENCODE az64
	,iprkz VARCHAR(65535)   ENCODE lzo
	,rdmhd VARCHAR(65535)   ENCODE lzo
	,przus VARCHAR(65535)   ENCODE lzo
	,mtpos_mara VARCHAR(65535)   ENCODE lzo
	,bflme VARCHAR(65535)   ENCODE lzo
	,zzmeins VARCHAR(65535)   ENCODE lzo
	,zmfgcell VARCHAR(65535)   ENCODE lzo
	,zmtsmto VARCHAR(65535)   ENCODE lzo
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.c11_0material_attr_current owner to base_admin;


-- bods.cont_pl_trans_current definition

-- Drop table

-- DROP TABLE bods.cont_pl_trans_current;

--DROP TABLE bods.cont_pl_trans_current;
CREATE TABLE IF NOT EXISTS bods.cont_pl_trans_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id BIGINT   ENCODE az64
	,fiscper VARCHAR(65535)   ENCODE lzo
	,fyr_id BIGINT   ENCODE az64
	,fmth_nbr BIGINT   ENCODE az64
	,co_cd VARCHAR(65535)   ENCODE lzo
	,acct VARCHAR(65535)   ENCODE lzo
	,txn_id VARCHAR(65535)   ENCODE lzo
	,seq_nbr VARCHAR(65535)   ENCODE lzo
	,post_dte VARCHAR(65535)   ENCODE lzo
	,crncy_cd VARCHAR(65535)   ENCODE lzo
	,amt NUMERIC(38,10)   ENCODE az64
	,usd_amt NUMERIC(38,10)   ENCODE az64
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,ins_dtm VARCHAR(65535)   ENCODE lzo
	,upd_dtm VARCHAR(65535)   ENCODE lzo
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.cont_pl_trans_current owner to base_admin;


-- bods.e03_0customer_attr_current definition

-- Drop table

-- DROP TABLE bods.e03_0customer_attr_current;

--DROP TABLE bods.e03_0customer_attr_current;
CREATE TABLE IF NOT EXISTS bods.e03_0customer_attr_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,kunnr VARCHAR(65535)   ENCODE lzo
	,adrnr VARCHAR(65535)   ENCODE lzo
	,anred VARCHAR(65535)   ENCODE lzo
	,aufsd VARCHAR(65535)   ENCODE lzo
	,bahne VARCHAR(65535)   ENCODE lzo
	,bahns VARCHAR(65535)   ENCODE lzo
	,bbbnr NUMERIC(38,10)   ENCODE az64
	,bbsnr NUMERIC(38,10)   ENCODE az64
	,begru VARCHAR(65535)   ENCODE lzo
	,brsch VARCHAR(65535)   ENCODE lzo
	,bubkz NUMERIC(38,10)   ENCODE az64
	,datlt VARCHAR(65535)   ENCODE lzo
	,erdat VARCHAR(65535)   ENCODE lzo
	,ernam VARCHAR(65535)   ENCODE lzo
	,exabl VARCHAR(65535)   ENCODE lzo
	,faksd VARCHAR(65535)   ENCODE lzo
	,fiskn VARCHAR(65535)   ENCODE lzo
	,knazk VARCHAR(65535)   ENCODE lzo
	,knrza VARCHAR(65535)   ENCODE lzo
	,konzs VARCHAR(65535)   ENCODE lzo
	,ktokd VARCHAR(65535)   ENCODE lzo
	,kukla VARCHAR(65535)   ENCODE lzo
	,land1 VARCHAR(65535)   ENCODE lzo
	,lifnr VARCHAR(65535)   ENCODE lzo
	,lifsd VARCHAR(65535)   ENCODE lzo
	,locco VARCHAR(65535)   ENCODE lzo
	,loevm VARCHAR(65535)   ENCODE lzo
	,name1 VARCHAR(65535)   ENCODE lzo
	,name2 VARCHAR(65535)   ENCODE lzo
	,name3 VARCHAR(65535)   ENCODE lzo
	,name4 VARCHAR(65535)   ENCODE lzo
	,niels VARCHAR(65535)   ENCODE lzo
	,ort01 VARCHAR(65535)   ENCODE lzo
	,ort02 VARCHAR(65535)   ENCODE lzo
	,pfach VARCHAR(65535)   ENCODE lzo
	,pstl2 VARCHAR(65535)   ENCODE lzo
	,pstlz VARCHAR(65535)   ENCODE lzo
	,regio VARCHAR(65535)   ENCODE lzo
	,counc VARCHAR(65535)   ENCODE lzo
	,cityc VARCHAR(65535)   ENCODE lzo
	,rpmkr VARCHAR(65535)   ENCODE lzo
	,sortl VARCHAR(65535)   ENCODE lzo
	,sperr VARCHAR(65535)   ENCODE lzo
	,spras VARCHAR(65535)   ENCODE lzo
	,stcd1 VARCHAR(65535)   ENCODE lzo
	,stcd2 VARCHAR(65535)   ENCODE lzo
	,stkza VARCHAR(65535)   ENCODE lzo
	,stkzu VARCHAR(65535)   ENCODE lzo
	,stras VARCHAR(65535)   ENCODE lzo
	,telbx VARCHAR(65535)   ENCODE lzo
	,telf1 VARCHAR(65535)   ENCODE lzo
	,telf2 VARCHAR(65535)   ENCODE lzo
	,telfx VARCHAR(65535)   ENCODE lzo
	,teltx VARCHAR(65535)   ENCODE lzo
	,telx1 VARCHAR(65535)   ENCODE lzo
	,lzone VARCHAR(65535)   ENCODE lzo
	,xcpdk VARCHAR(65535)   ENCODE lzo
	,xzemp VARCHAR(65535)   ENCODE lzo
	,vbund VARCHAR(65535)   ENCODE lzo
	,stceg VARCHAR(65535)   ENCODE lzo
	,dear1 VARCHAR(65535)   ENCODE lzo
	,dear2 VARCHAR(65535)   ENCODE lzo
	,dear3 VARCHAR(65535)   ENCODE lzo
	,dear4 VARCHAR(65535)   ENCODE lzo
	,dear5 VARCHAR(65535)   ENCODE lzo
	,dear6 VARCHAR(65535)   ENCODE lzo
	,gform VARCHAR(65535)   ENCODE lzo
	,bran1 VARCHAR(65535)   ENCODE lzo
	,bran2 VARCHAR(65535)   ENCODE lzo
	,bran3 VARCHAR(65535)   ENCODE lzo
	,bran4 VARCHAR(65535)   ENCODE lzo
	,bran5 VARCHAR(65535)   ENCODE lzo
	,ekont VARCHAR(65535)   ENCODE lzo
	,umsat NUMERIC(38,10)   ENCODE az64
	,umjah NUMERIC(38,10)   ENCODE az64
	,uwaer VARCHAR(65535)   ENCODE lzo
	,jmzah NUMERIC(38,10)   ENCODE az64
	,jmjah NUMERIC(38,10)   ENCODE az64
	,katr1 VARCHAR(65535)   ENCODE lzo
	,katr2 VARCHAR(65535)   ENCODE lzo
	,katr3 VARCHAR(65535)   ENCODE lzo
	,katr4 VARCHAR(65535)   ENCODE lzo
	,katr5 VARCHAR(65535)   ENCODE lzo
	,katr6 VARCHAR(65535)   ENCODE lzo
	,katr7 VARCHAR(65535)   ENCODE lzo
	,katr8 VARCHAR(65535)   ENCODE lzo
	,katr9 VARCHAR(65535)   ENCODE lzo
	,katr10 VARCHAR(65535)   ENCODE lzo
	,stkzn VARCHAR(65535)   ENCODE lzo
	,umsa1 NUMERIC(38,10)   ENCODE az64
	,txjcd VARCHAR(65535)   ENCODE lzo
	,mcod1 VARCHAR(65535)   ENCODE lzo
	,mcod2 VARCHAR(65535)   ENCODE lzo
	,mcod3 VARCHAR(65535)   ENCODE lzo
	,periv VARCHAR(65535)   ENCODE lzo
	,abrvw VARCHAR(65535)   ENCODE lzo
	,inspbydebi VARCHAR(65535)   ENCODE lzo
	,inspatdebi VARCHAR(65535)   ENCODE lzo
	,ktocd VARCHAR(65535)   ENCODE lzo
	,pfort VARCHAR(65535)   ENCODE lzo
	,werks VARCHAR(65535)   ENCODE lzo
	,dtams VARCHAR(65535)   ENCODE lzo
	,dtaws VARCHAR(65535)   ENCODE lzo
	,duefl VARCHAR(65535)   ENCODE lzo
	,hzuor NUMERIC(38,10)   ENCODE az64
	,sperz VARCHAR(65535)   ENCODE lzo
	,etikg VARCHAR(65535)   ENCODE lzo
	,civve VARCHAR(65535)   ENCODE lzo
	,milve VARCHAR(65535)   ENCODE lzo
	,kdkg1 VARCHAR(65535)   ENCODE lzo
	,kdkg2 VARCHAR(65535)   ENCODE lzo
	,kdkg3 VARCHAR(65535)   ENCODE lzo
	,kdkg4 VARCHAR(65535)   ENCODE lzo
	,kdkg5 VARCHAR(65535)   ENCODE lzo
	,xknza VARCHAR(65535)   ENCODE lzo
	,fityp VARCHAR(65535)   ENCODE lzo
	,stcdt VARCHAR(65535)   ENCODE lzo
	,stcd3 VARCHAR(65535)   ENCODE lzo
	,stcd4 VARCHAR(65535)   ENCODE lzo
	,xicms VARCHAR(65535)   ENCODE lzo
	,xxipi VARCHAR(65535)   ENCODE lzo
	,xsubt VARCHAR(65535)   ENCODE lzo
	,cfopc VARCHAR(65535)   ENCODE lzo
	,txlw1 VARCHAR(65535)   ENCODE lzo
	,txlw2 VARCHAR(65535)   ENCODE lzo
	,ccc01 VARCHAR(65535)   ENCODE lzo
	,ccc02 VARCHAR(65535)   ENCODE lzo
	,ccc03 VARCHAR(65535)   ENCODE lzo
	,ccc04 VARCHAR(65535)   ENCODE lzo
	,cassd VARCHAR(65535)   ENCODE lzo
	,knurl VARCHAR(65535)   ENCODE lzo
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.e03_0customer_attr_current owner to base_admin;


-- bods.e03_0material_attr_current definition

-- Drop table

-- DROP TABLE bods.e03_0material_attr_current;

--DROP TABLE bods.e03_0material_attr_current;
CREATE TABLE IF NOT EXISTS bods.e03_0material_attr_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,matnr VARCHAR(65535)   ENCODE lzo
	,ersda VARCHAR(65535)   ENCODE lzo
	,ernam VARCHAR(65535)   ENCODE lzo
	,laeda VARCHAR(65535)   ENCODE lzo
	,aenam VARCHAR(65535)   ENCODE lzo
	,vpsta VARCHAR(65535)   ENCODE lzo
	,pstat VARCHAR(65535)   ENCODE lzo
	,lvorm VARCHAR(65535)   ENCODE lzo
	,mtart VARCHAR(65535)   ENCODE lzo
	,mbrsh VARCHAR(65535)   ENCODE lzo
	,matkl VARCHAR(65535)   ENCODE lzo
	,bismt VARCHAR(65535)   ENCODE lzo
	,meins VARCHAR(65535)   ENCODE lzo
	,bstme VARCHAR(65535)   ENCODE lzo
	,zeinr VARCHAR(65535)   ENCODE lzo
	,zeiar VARCHAR(65535)   ENCODE lzo
	,zeivr VARCHAR(65535)   ENCODE lzo
	,zeifo VARCHAR(65535)   ENCODE lzo
	,aeszn VARCHAR(65535)   ENCODE lzo
	,blatt VARCHAR(65535)   ENCODE lzo
	,blanz NUMERIC(38,10)   ENCODE az64
	,ferth VARCHAR(65535)   ENCODE lzo
	,formt VARCHAR(65535)   ENCODE lzo
	,groes VARCHAR(65535)   ENCODE lzo
	,wrkst VARCHAR(65535)   ENCODE lzo
	,normt VARCHAR(65535)   ENCODE lzo
	,labor VARCHAR(65535)   ENCODE lzo
	,ekwsl VARCHAR(65535)   ENCODE lzo
	,brgew NUMERIC(38,10)   ENCODE az64
	,ntgew NUMERIC(38,10)   ENCODE az64
	,gewei VARCHAR(65535)   ENCODE lzo
	,volum NUMERIC(38,10)   ENCODE az64
	,voleh VARCHAR(65535)   ENCODE lzo
	,behvo VARCHAR(65535)   ENCODE lzo
	,raube VARCHAR(65535)   ENCODE lzo
	,tempb VARCHAR(65535)   ENCODE lzo
	,disst VARCHAR(65535)   ENCODE lzo
	,tragr VARCHAR(65535)   ENCODE lzo
	,stoff VARCHAR(65535)   ENCODE lzo
	,spart VARCHAR(65535)   ENCODE lzo
	,kunnr VARCHAR(65535)   ENCODE lzo
	,eannr VARCHAR(65535)   ENCODE lzo
	,wesch NUMERIC(38,10)   ENCODE az64
	,bwvor VARCHAR(65535)   ENCODE lzo
	,bwscl VARCHAR(65535)   ENCODE lzo
	,saiso VARCHAR(65535)   ENCODE lzo
	,etiar VARCHAR(65535)   ENCODE lzo
	,etifo VARCHAR(65535)   ENCODE lzo
	,entar VARCHAR(65535)   ENCODE lzo
	,ean11 VARCHAR(65535)   ENCODE lzo
	,numtp VARCHAR(65535)   ENCODE lzo
	,laeng NUMERIC(38,10)   ENCODE az64
	,breit NUMERIC(38,10)   ENCODE az64
	,hoehe NUMERIC(38,10)   ENCODE az64
	,meabm VARCHAR(65535)   ENCODE lzo
	,prdha VARCHAR(65535)   ENCODE lzo
	,aeklk VARCHAR(65535)   ENCODE lzo
	,cadkz VARCHAR(65535)   ENCODE lzo
	,qmpur VARCHAR(65535)   ENCODE lzo
	,ergew NUMERIC(38,10)   ENCODE az64
	,ergei VARCHAR(65535)   ENCODE lzo
	,ervol NUMERIC(38,10)   ENCODE az64
	,ervoe VARCHAR(65535)   ENCODE lzo
	,gewto NUMERIC(38,10)   ENCODE az64
	,volto NUMERIC(38,10)   ENCODE az64
	,vabme VARCHAR(65535)   ENCODE lzo
	,kzrev VARCHAR(65535)   ENCODE lzo
	,kzkfg VARCHAR(65535)   ENCODE lzo
	,xchpf VARCHAR(65535)   ENCODE lzo
	,vhart VARCHAR(65535)   ENCODE lzo
	,fuelg NUMERIC(38,10)   ENCODE az64
	,stfak NUMERIC(38,10)   ENCODE az64
	,magrv VARCHAR(65535)   ENCODE lzo
	,begru VARCHAR(65535)   ENCODE lzo
	,datab VARCHAR(65535)   ENCODE lzo
	,liqdt VARCHAR(65535)   ENCODE lzo
	,saisj VARCHAR(65535)   ENCODE lzo
	,plgtp VARCHAR(65535)   ENCODE lzo
	,mlgut VARCHAR(65535)   ENCODE lzo
	,extwg VARCHAR(65535)   ENCODE lzo
	,satnr VARCHAR(65535)   ENCODE lzo
	,attyp VARCHAR(65535)   ENCODE lzo
	,kzkup VARCHAR(65535)   ENCODE lzo
	,kznfm VARCHAR(65535)   ENCODE lzo
	,pmata VARCHAR(65535)   ENCODE lzo
	,mstae VARCHAR(65535)   ENCODE lzo
	,mstav VARCHAR(65535)   ENCODE lzo
	,mstde VARCHAR(65535)   ENCODE lzo
	,mstdv VARCHAR(65535)   ENCODE lzo
	,taklv VARCHAR(65535)   ENCODE lzo
	,rbnrm VARCHAR(65535)   ENCODE lzo
	,mhdrz NUMERIC(38,10)   ENCODE az64
	,mhdhb NUMERIC(38,10)   ENCODE az64
	,mhdlp NUMERIC(38,10)   ENCODE az64
	,inhme VARCHAR(65535)   ENCODE lzo
	,inhal NUMERIC(38,10)   ENCODE az64
	,vpreh NUMERIC(38,10)   ENCODE az64
	,etiag VARCHAR(65535)   ENCODE lzo
	,inhbr NUMERIC(38,10)   ENCODE az64
	,cmeth VARCHAR(65535)   ENCODE lzo
	,cuobf NUMERIC(38,10)   ENCODE az64
	,kzumw VARCHAR(65535)   ENCODE lzo
	,kosch VARCHAR(65535)   ENCODE lzo
	,sprof VARCHAR(65535)   ENCODE lzo
	,nrfhg VARCHAR(65535)   ENCODE lzo
	,mfrpn VARCHAR(65535)   ENCODE lzo
	,mfrnr VARCHAR(65535)   ENCODE lzo
	,bmatn VARCHAR(65535)   ENCODE lzo
	,mprof VARCHAR(65535)   ENCODE lzo
	,kzwsm VARCHAR(65535)   ENCODE lzo
	,saity VARCHAR(65535)   ENCODE lzo
	,profl VARCHAR(65535)   ENCODE lzo
	,ihivi VARCHAR(65535)   ENCODE lzo
	,iloos VARCHAR(65535)   ENCODE lzo
	,serlv VARCHAR(65535)   ENCODE lzo
	,kzgvh VARCHAR(65535)   ENCODE lzo
	,xgchp VARCHAR(65535)   ENCODE lzo
	,kzeff VARCHAR(65535)   ENCODE lzo
	,compl NUMERIC(38,10)   ENCODE az64
	,iprkz VARCHAR(65535)   ENCODE lzo
	,rdmhd VARCHAR(65535)   ENCODE lzo
	,przus VARCHAR(65535)   ENCODE lzo
	,mtpos_mara VARCHAR(65535)   ENCODE lzo
	,bflme VARCHAR(65535)   ENCODE lzo
	,zzmuvend VARCHAR(65535)   ENCODE lzo
	,zzmfpdgr VARCHAR(65535)   ENCODE lzo
	,zzpvend VARCHAR(65535)   ENCODE lzo
	,zzvndgrp VARCHAR(65535)   ENCODE lzo
	,zzsvend VARCHAR(65535)   ENCODE lzo
	,zzumrez NUMERIC(38,10)   ENCODE az64
	,zzumren NUMERIC(38,10)   ENCODE az64
	,zzprctr VARCHAR(65535)   ENCODE lzo
	,zznochpr VARCHAR(65535)   ENCODE lzo
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.e03_0material_attr_current owner to base_admin;


-- bods.e03_3fi_sl_h1_si_current definition

-- Drop table

-- DROP TABLE bods.e03_3fi_sl_h1_si_current;

--DROP TABLE bods.e03_3fi_sl_h1_si_current;
CREATE TABLE IF NOT EXISTS bods.e03_3fi_sl_h1_si_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id BIGINT   ENCODE az64
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,ver VARCHAR(65535)   ENCODE lzo
	,"year" NUMERIC(38,10)   ENCODE az64
	,currkey VARCHAR(65535)   ENCODE lzo
	,doctype VARCHAR(65535)   ENCODE lzo
	,gl_sirid VARCHAR(65535)   ENCODE lzo
	,docno VARCHAR(65535)   ENCODE lzo
	,docline VARCHAR(65535)   ENCODE lzo
	,hfmacct VARCHAR(65535)   ENCODE lzo
	,hfment VARCHAR(65535)   ENCODE lzo
	,hfmc1 VARCHAR(65535)   ENCODE lzo
	,hpent VARCHAR(65535)   ENCODE lzo
	,hpsbu VARCHAR(65535)   ENCODE lzo
	,cocode VARCHAR(65535)   ENCODE lzo
	,shiptoctry VARCHAR(65535)   ENCODE lzo
	,tradepart VARCHAR(65535)   ENCODE lzo
	,coarea VARCHAR(65535)   ENCODE lzo
	,profctr VARCHAR(65535)   ENCODE lzo
	,trantype VARCHAR(65535)   ENCODE lzo
	,costctr VARCHAR(65535)   ENCODE lzo
	,funarea VARCHAR(65535)   ENCODE lzo
	,acct VARCHAR(65535)   ENCODE lzo
	,sl_doctype VARCHAR(65535)   ENCODE lzo
	,plant VARCHAR(65535)   ENCODE lzo
	,salesoff VARCHAR(65535)   ENCODE lzo
	,indkey VARCHAR(65535)   ENCODE lzo
	,date_ VARCHAR(65535)   ENCODE lzo
	,refdoc VARCHAR(65535)   ENCODE lzo
	,refitm NUMERIC(38,10)   ENCODE az64
	,refdoccat VARCHAR(65535)   ENCODE lzo
	,refproc VARCHAR(65535)   ENCODE lzo
	,product VARCHAR(65535)   ENCODE lzo
	,ordno VARCHAR(65535)   ENCODE lzo
	,soldtocust VARCHAR(65535)   ENCODE lzo
	,pehgrp VARCHAR(65535)   ENCODE lzo
	,shiptocust VARCHAR(65535)   ENCODE lzo
	,payer VARCHAR(65535)   ENCODE lzo
	,salesdiv VARCHAR(65535)   ENCODE lzo
	,distchan VARCHAR(65535)   ENCODE lzo
	,gppport VARCHAR(65535)   ENCODE lzo
	,period VARCHAR(65535)   ENCODE lzo
	,valuetype NUMERIC(38,10)   ENCODE az64
	,yydiv VARCHAR(65535)   ENCODE lzo
	,vkorg VARCHAR(65535)   ENCODE lzo
	,cpudt VARCHAR(65535)   ENCODE lzo
	,cputm VARCHAR(65535)   ENCODE lzo
	,awtyp VARCHAR(65535)   ENCODE lzo
	,belnr VARCHAR(65535)   ENCODE lzo
	,buzei NUMERIC(38,10)   ENCODE az64
	,amt NUMERIC(38,10)   ENCODE az64
	,profctrgrp VARCHAR(65535)   ENCODE lzo
	,usertemp2 VARCHAR(65535)   ENCODE lzo
	,entitygrp BIGINT   ENCODE az64
	,int_entitytype VARCHAR(65535)   ENCODE lzo
	,int_brandgrp VARCHAR(65535)   ENCODE lzo
	,int_geogrp VARCHAR(65535)   ENCODE lzo
	,int_salesgrp VARCHAR(65535)   ENCODE lzo
	,int_saldistrict VARCHAR(65535)   ENCODE lzo
	,int_functype VARCHAR(65535)   ENCODE lzo
	,int_salesgrp2 VARCHAR(65535)   ENCODE lzo
	,quanunit VARCHAR(65535)   ENCODE lzo
	,quantity NUMERIC(38,10)   ENCODE az64
	,curtype VARCHAR(65535)   ENCODE lzo
	,poper NUMERIC(38,10)   ENCODE az64
	,rzz101 VARCHAR(65535)   ENCODE lzo
	,rzz107 VARCHAR(65535)   ENCODE lzo
	,rzz108 VARCHAR(65535)   ENCODE lzo
	,rzz111 VARCHAR(65535)   ENCODE lzo
	,rzz102 VARCHAR(65535)   ENCODE lzo
	,rzz113 VARCHAR(65535)   ENCODE lzo
	,logsys VARCHAR(65535)   ENCODE lzo
	,sgtxt VARCHAR(65535)   ENCODE lzo
	,activ VARCHAR(65535)   ENCODE lzo
	,yyhpact VARCHAR(65535)   ENCODE lzo
	,wsdat VARCHAR(65535)   ENCODE lzo
	,refryear NUMERIC(38,10)   ENCODE az64
	,usnam VARCHAR(65535)   ENCODE lzo
	,aworg VARCHAR(65535)   ENCODE lzo
	,rwcur VARCHAR(65535)   ENCODE lzo
	,xsplitmod VARCHAR(65535)   ENCODE lzo
	,erkrs VARCHAR(65535)   ENCODE lzo
	,paobjnr NUMERIC(38,10)   ENCODE az64
	,pasubnr NUMERIC(38,10)   ENCODE az64
	,ps_psp_pnr NUMERIC(38,10)   ENCODE az64
	,kdauf VARCHAR(65535)   ENCODE lzo
	,kdpos NUMERIC(38,10)   ENCODE az64
	,fiscvar VARCHAR(65535)   ENCODE lzo
	,chartaccts VARCHAR(65535)   ENCODE lzo
	,upmod VARCHAR(65535)   ENCODE lzo
	,currunit VARCHAR(65535)   ENCODE lzo
	,debit NUMERIC(38,10)   ENCODE az64
	,credit NUMERIC(38,10)   ENCODE az64
	,adquanunit VARCHAR(65535)   ENCODE lzo
	,adquantity NUMERIC(38,10)   ENCODE az64
	,rownum VARCHAR(65535)   ENCODE lzo
	,runid BIGINT   ENCODE az64
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.e03_3fi_sl_h1_si_current owner to base_admin;


-- bods.e03_3fi_sl_h1_si_current_2018_9m definition

-- Drop table

-- DROP TABLE bods.e03_3fi_sl_h1_si_current_2018_9m;

--DROP TABLE bods.e03_3fi_sl_h1_si_current_2018_9m;
CREATE TABLE IF NOT EXISTS bods.e03_3fi_sl_h1_si_current_2018_9m
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id BIGINT   ENCODE az64
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,ver VARCHAR(65535)   ENCODE lzo
	,"year" NUMERIC(38,10)   ENCODE az64
	,currkey VARCHAR(65535)   ENCODE lzo
	,doctype VARCHAR(65535)   ENCODE lzo
	,gl_sirid VARCHAR(65535)   ENCODE lzo
	,docno VARCHAR(65535)   ENCODE lzo
	,docline VARCHAR(65535)   ENCODE lzo
	,hfmacct VARCHAR(65535)   ENCODE lzo
	,hfment VARCHAR(65535)   ENCODE lzo
	,hfmc1 VARCHAR(65535)   ENCODE lzo
	,hpent VARCHAR(65535)   ENCODE lzo
	,hpsbu VARCHAR(65535)   ENCODE lzo
	,cocode VARCHAR(65535)   ENCODE lzo
	,shiptoctry VARCHAR(65535)   ENCODE lzo
	,tradepart VARCHAR(65535)   ENCODE lzo
	,coarea VARCHAR(65535)   ENCODE lzo
	,profctr VARCHAR(65535)   ENCODE lzo
	,trantype VARCHAR(65535)   ENCODE lzo
	,costctr VARCHAR(65535)   ENCODE lzo
	,funarea VARCHAR(65535)   ENCODE lzo
	,acct VARCHAR(65535)   ENCODE lzo
	,sl_doctype VARCHAR(65535)   ENCODE lzo
	,plant VARCHAR(65535)   ENCODE lzo
	,salesoff VARCHAR(65535)   ENCODE lzo
	,indkey VARCHAR(65535)   ENCODE lzo
	,date_ VARCHAR(65535)   ENCODE lzo
	,refdoc VARCHAR(65535)   ENCODE lzo
	,refitm NUMERIC(38,10)   ENCODE az64
	,refdoccat VARCHAR(65535)   ENCODE lzo
	,refproc VARCHAR(65535)   ENCODE lzo
	,product VARCHAR(65535)   ENCODE lzo
	,ordno VARCHAR(65535)   ENCODE lzo
	,soldtocust VARCHAR(65535)   ENCODE lzo
	,pehgrp VARCHAR(65535)   ENCODE lzo
	,shiptocust VARCHAR(65535)   ENCODE lzo
	,payer VARCHAR(65535)   ENCODE lzo
	,salesdiv VARCHAR(65535)   ENCODE lzo
	,distchan VARCHAR(65535)   ENCODE lzo
	,gppport VARCHAR(65535)   ENCODE lzo
	,period VARCHAR(65535)   ENCODE lzo
	,valuetype NUMERIC(38,10)   ENCODE az64
	,yydiv VARCHAR(65535)   ENCODE lzo
	,vkorg VARCHAR(65535)   ENCODE lzo
	,cpudt VARCHAR(65535)   ENCODE lzo
	,cputm VARCHAR(65535)   ENCODE lzo
	,awtyp VARCHAR(65535)   ENCODE lzo
	,belnr VARCHAR(65535)   ENCODE lzo
	,buzei NUMERIC(38,10)   ENCODE az64
	,amt NUMERIC(38,10)   ENCODE az64
	,profctrgrp VARCHAR(65535)   ENCODE lzo
	,usertemp2 VARCHAR(65535)   ENCODE lzo
	,entitygrp BIGINT   ENCODE az64
	,int_entitytype VARCHAR(65535)   ENCODE lzo
	,int_brandgrp VARCHAR(65535)   ENCODE lzo
	,int_geogrp VARCHAR(65535)   ENCODE lzo
	,int_salesgrp VARCHAR(65535)   ENCODE lzo
	,int_saldistrict VARCHAR(65535)   ENCODE lzo
	,int_functype VARCHAR(65535)   ENCODE lzo
	,int_salesgrp2 VARCHAR(65535)   ENCODE lzo
	,quanunit VARCHAR(65535)   ENCODE lzo
	,quantity NUMERIC(38,10)   ENCODE az64
	,curtype VARCHAR(65535)   ENCODE lzo
	,poper NUMERIC(38,10)   ENCODE az64
	,rzz101 VARCHAR(65535)   ENCODE lzo
	,rzz107 VARCHAR(65535)   ENCODE lzo
	,rzz108 VARCHAR(65535)   ENCODE lzo
	,rzz111 VARCHAR(65535)   ENCODE lzo
	,rzz102 VARCHAR(65535)   ENCODE lzo
	,rzz113 VARCHAR(65535)   ENCODE lzo
	,logsys VARCHAR(65535)   ENCODE lzo
	,sgtxt VARCHAR(65535)   ENCODE lzo
	,activ VARCHAR(65535)   ENCODE lzo
	,yyhpact VARCHAR(65535)   ENCODE lzo
	,wsdat VARCHAR(65535)   ENCODE lzo
	,refryear NUMERIC(38,10)   ENCODE az64
	,usnam VARCHAR(65535)   ENCODE lzo
	,aworg VARCHAR(65535)   ENCODE lzo
	,rwcur VARCHAR(65535)   ENCODE lzo
	,xsplitmod VARCHAR(65535)   ENCODE lzo
	,erkrs VARCHAR(65535)   ENCODE lzo
	,paobjnr NUMERIC(38,10)   ENCODE az64
	,pasubnr NUMERIC(38,10)   ENCODE az64
	,ps_psp_pnr NUMERIC(38,10)   ENCODE az64
	,kdauf VARCHAR(65535)   ENCODE lzo
	,kdpos NUMERIC(38,10)   ENCODE az64
	,fiscvar VARCHAR(65535)   ENCODE lzo
	,chartaccts VARCHAR(65535)   ENCODE lzo
	,upmod VARCHAR(65535)   ENCODE lzo
	,currunit VARCHAR(65535)   ENCODE lzo
	,debit NUMERIC(38,10)   ENCODE az64
	,credit NUMERIC(38,10)   ENCODE az64
	,adquanunit VARCHAR(65535)   ENCODE lzo
	,adquantity NUMERIC(38,10)   ENCODE az64
	,rownum VARCHAR(65535)   ENCODE lzo
	,runid BIGINT   ENCODE az64
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE EVEN
;
ALTER TABLE bods.e03_3fi_sl_h1_si_current_2018_9m owner to base_admin;


-- bods.e03_3fi_sl_h1_si_current_2018_july definition

-- Drop table

-- DROP TABLE bods.e03_3fi_sl_h1_si_current_2018_july;

--DROP TABLE bods.e03_3fi_sl_h1_si_current_2018_july;
CREATE TABLE IF NOT EXISTS bods.e03_3fi_sl_h1_si_current_2018_july
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id BIGINT   ENCODE az64
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,ver VARCHAR(65535)   ENCODE lzo
	,"year" NUMERIC(38,10)   ENCODE az64
	,currkey VARCHAR(65535)   ENCODE lzo
	,doctype VARCHAR(65535)   ENCODE lzo
	,gl_sirid VARCHAR(65535)   ENCODE lzo
	,docno VARCHAR(65535)   ENCODE lzo
	,docline VARCHAR(65535)   ENCODE lzo
	,hfmacct VARCHAR(65535)   ENCODE lzo
	,hfment VARCHAR(65535)   ENCODE lzo
	,hfmc1 VARCHAR(65535)   ENCODE lzo
	,hpent VARCHAR(65535)   ENCODE lzo
	,hpsbu VARCHAR(65535)   ENCODE lzo
	,cocode VARCHAR(65535)   ENCODE lzo
	,shiptoctry VARCHAR(65535)   ENCODE lzo
	,tradepart VARCHAR(65535)   ENCODE lzo
	,coarea VARCHAR(65535)   ENCODE lzo
	,profctr VARCHAR(65535)   ENCODE lzo
	,trantype VARCHAR(65535)   ENCODE lzo
	,costctr VARCHAR(65535)   ENCODE lzo
	,funarea VARCHAR(65535)   ENCODE lzo
	,acct VARCHAR(65535)   ENCODE lzo
	,sl_doctype VARCHAR(65535)   ENCODE lzo
	,plant VARCHAR(65535)   ENCODE lzo
	,salesoff VARCHAR(65535)   ENCODE lzo
	,indkey VARCHAR(65535)   ENCODE lzo
	,date_ VARCHAR(65535)   ENCODE lzo
	,refdoc VARCHAR(65535)   ENCODE lzo
	,refitm NUMERIC(38,10)   ENCODE az64
	,refdoccat VARCHAR(65535)   ENCODE lzo
	,refproc VARCHAR(65535)   ENCODE lzo
	,product VARCHAR(65535)   ENCODE lzo
	,ordno VARCHAR(65535)   ENCODE lzo
	,soldtocust VARCHAR(65535)   ENCODE lzo
	,pehgrp VARCHAR(65535)   ENCODE lzo
	,shiptocust VARCHAR(65535)   ENCODE lzo
	,payer VARCHAR(65535)   ENCODE lzo
	,salesdiv VARCHAR(65535)   ENCODE lzo
	,distchan VARCHAR(65535)   ENCODE lzo
	,gppport VARCHAR(65535)   ENCODE lzo
	,period VARCHAR(65535)   ENCODE lzo
	,valuetype NUMERIC(38,10)   ENCODE az64
	,yydiv VARCHAR(65535)   ENCODE lzo
	,vkorg VARCHAR(65535)   ENCODE lzo
	,cpudt VARCHAR(65535)   ENCODE lzo
	,cputm VARCHAR(65535)   ENCODE lzo
	,awtyp VARCHAR(65535)   ENCODE lzo
	,belnr VARCHAR(65535)   ENCODE lzo
	,buzei NUMERIC(38,10)   ENCODE az64
	,amt NUMERIC(38,10)   ENCODE az64
	,profctrgrp VARCHAR(65535)   ENCODE lzo
	,usertemp2 VARCHAR(65535)   ENCODE lzo
	,entitygrp BIGINT   ENCODE az64
	,int_entitytype VARCHAR(65535)   ENCODE lzo
	,int_brandgrp VARCHAR(65535)   ENCODE lzo
	,int_geogrp VARCHAR(65535)   ENCODE lzo
	,int_salesgrp VARCHAR(65535)   ENCODE lzo
	,int_saldistrict VARCHAR(65535)   ENCODE lzo
	,int_functype VARCHAR(65535)   ENCODE lzo
	,int_salesgrp2 VARCHAR(65535)   ENCODE lzo
	,quanunit VARCHAR(65535)   ENCODE lzo
	,quantity NUMERIC(38,10)   ENCODE az64
	,curtype VARCHAR(65535)   ENCODE lzo
	,poper NUMERIC(38,10)   ENCODE az64
	,rzz101 VARCHAR(65535)   ENCODE lzo
	,rzz107 VARCHAR(65535)   ENCODE lzo
	,rzz108 VARCHAR(65535)   ENCODE lzo
	,rzz111 VARCHAR(65535)   ENCODE lzo
	,rzz102 VARCHAR(65535)   ENCODE lzo
	,rzz113 VARCHAR(65535)   ENCODE lzo
	,logsys VARCHAR(65535)   ENCODE lzo
	,sgtxt VARCHAR(65535)   ENCODE lzo
	,activ VARCHAR(65535)   ENCODE lzo
	,yyhpact VARCHAR(65535)   ENCODE lzo
	,wsdat VARCHAR(65535)   ENCODE lzo
	,refryear NUMERIC(38,10)   ENCODE az64
	,usnam VARCHAR(65535)   ENCODE lzo
	,aworg VARCHAR(65535)   ENCODE lzo
	,rwcur VARCHAR(65535)   ENCODE lzo
	,xsplitmod VARCHAR(65535)   ENCODE lzo
	,erkrs VARCHAR(65535)   ENCODE lzo
	,paobjnr NUMERIC(38,10)   ENCODE az64
	,pasubnr NUMERIC(38,10)   ENCODE az64
	,ps_psp_pnr NUMERIC(38,10)   ENCODE az64
	,kdauf VARCHAR(65535)   ENCODE lzo
	,kdpos NUMERIC(38,10)   ENCODE az64
	,fiscvar VARCHAR(65535)   ENCODE lzo
	,chartaccts VARCHAR(65535)   ENCODE lzo
	,upmod VARCHAR(65535)   ENCODE lzo
	,currunit VARCHAR(65535)   ENCODE lzo
	,debit NUMERIC(38,10)   ENCODE az64
	,credit NUMERIC(38,10)   ENCODE az64
	,adquanunit VARCHAR(65535)   ENCODE lzo
	,adquantity NUMERIC(38,10)   ENCODE az64
	,rownum VARCHAR(65535)   ENCODE lzo
	,runid BIGINT   ENCODE az64
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE EVEN
;
ALTER TABLE bods.e03_3fi_sl_h1_si_current_2018_july owner to base_admin;


-- bods.e03_3fi_sl_h1_si_current_2019 definition

-- Drop table

-- DROP TABLE bods.e03_3fi_sl_h1_si_current_2019;

--DROP TABLE bods.e03_3fi_sl_h1_si_current_2019;
CREATE TABLE IF NOT EXISTS bods.e03_3fi_sl_h1_si_current_2019
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id BIGINT   ENCODE az64
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,ver VARCHAR(65535)   ENCODE lzo
	,"year" NUMERIC(38,10)   ENCODE az64
	,currkey VARCHAR(65535)   ENCODE lzo
	,doctype VARCHAR(65535)   ENCODE lzo
	,gl_sirid VARCHAR(65535)   ENCODE lzo
	,docno VARCHAR(65535)   ENCODE lzo
	,docline VARCHAR(65535)   ENCODE lzo
	,hfmacct VARCHAR(65535)   ENCODE lzo
	,hfment VARCHAR(65535)   ENCODE lzo
	,hfmc1 VARCHAR(65535)   ENCODE lzo
	,hpent VARCHAR(65535)   ENCODE lzo
	,hpsbu VARCHAR(65535)   ENCODE lzo
	,cocode VARCHAR(65535)   ENCODE lzo
	,shiptoctry VARCHAR(65535)   ENCODE lzo
	,tradepart VARCHAR(65535)   ENCODE lzo
	,coarea VARCHAR(65535)   ENCODE lzo
	,profctr VARCHAR(65535)   ENCODE lzo
	,trantype VARCHAR(65535)   ENCODE lzo
	,costctr VARCHAR(65535)   ENCODE lzo
	,funarea VARCHAR(65535)   ENCODE lzo
	,acct VARCHAR(65535)   ENCODE lzo
	,sl_doctype VARCHAR(65535)   ENCODE lzo
	,plant VARCHAR(65535)   ENCODE lzo
	,salesoff VARCHAR(65535)   ENCODE lzo
	,indkey VARCHAR(65535)   ENCODE lzo
	,date_ VARCHAR(65535)   ENCODE lzo
	,refdoc VARCHAR(65535)   ENCODE lzo
	,refitm NUMERIC(38,10)   ENCODE az64
	,refdoccat VARCHAR(65535)   ENCODE lzo
	,refproc VARCHAR(65535)   ENCODE lzo
	,product VARCHAR(65535)   ENCODE lzo
	,ordno VARCHAR(65535)   ENCODE lzo
	,soldtocust VARCHAR(65535)   ENCODE lzo
	,pehgrp VARCHAR(65535)   ENCODE lzo
	,shiptocust VARCHAR(65535)   ENCODE lzo
	,payer VARCHAR(65535)   ENCODE lzo
	,salesdiv VARCHAR(65535)   ENCODE lzo
	,distchan VARCHAR(65535)   ENCODE lzo
	,gppport VARCHAR(65535)   ENCODE lzo
	,period VARCHAR(65535)   ENCODE lzo
	,valuetype NUMERIC(38,10)   ENCODE az64
	,yydiv VARCHAR(65535)   ENCODE lzo
	,vkorg VARCHAR(65535)   ENCODE lzo
	,cpudt VARCHAR(65535)   ENCODE lzo
	,cputm VARCHAR(65535)   ENCODE lzo
	,awtyp VARCHAR(65535)   ENCODE lzo
	,belnr VARCHAR(65535)   ENCODE lzo
	,buzei NUMERIC(38,10)   ENCODE az64
	,amt NUMERIC(38,10)   ENCODE az64
	,profctrgrp VARCHAR(65535)   ENCODE lzo
	,usertemp2 VARCHAR(65535)   ENCODE lzo
	,entitygrp BIGINT   ENCODE az64
	,int_entitytype VARCHAR(65535)   ENCODE lzo
	,int_brandgrp VARCHAR(65535)   ENCODE lzo
	,int_geogrp VARCHAR(65535)   ENCODE lzo
	,int_salesgrp VARCHAR(65535)   ENCODE lzo
	,int_saldistrict VARCHAR(65535)   ENCODE lzo
	,int_functype VARCHAR(65535)   ENCODE lzo
	,int_salesgrp2 VARCHAR(65535)   ENCODE lzo
	,quanunit VARCHAR(65535)   ENCODE lzo
	,quantity NUMERIC(38,10)   ENCODE az64
	,curtype VARCHAR(65535)   ENCODE lzo
	,poper NUMERIC(38,10)   ENCODE az64
	,rzz101 VARCHAR(65535)   ENCODE lzo
	,rzz107 VARCHAR(65535)   ENCODE lzo
	,rzz108 VARCHAR(65535)   ENCODE lzo
	,rzz111 VARCHAR(65535)   ENCODE lzo
	,rzz102 VARCHAR(65535)   ENCODE lzo
	,rzz113 VARCHAR(65535)   ENCODE lzo
	,logsys VARCHAR(65535)   ENCODE lzo
	,sgtxt VARCHAR(65535)   ENCODE lzo
	,activ VARCHAR(65535)   ENCODE lzo
	,yyhpact VARCHAR(65535)   ENCODE lzo
	,wsdat VARCHAR(65535)   ENCODE lzo
	,refryear NUMERIC(38,10)   ENCODE az64
	,usnam VARCHAR(65535)   ENCODE lzo
	,aworg VARCHAR(65535)   ENCODE lzo
	,rwcur VARCHAR(65535)   ENCODE lzo
	,xsplitmod VARCHAR(65535)   ENCODE lzo
	,erkrs VARCHAR(65535)   ENCODE lzo
	,paobjnr NUMERIC(38,10)   ENCODE az64
	,pasubnr NUMERIC(38,10)   ENCODE az64
	,ps_psp_pnr NUMERIC(38,10)   ENCODE az64
	,kdauf VARCHAR(65535)   ENCODE lzo
	,kdpos NUMERIC(38,10)   ENCODE az64
	,fiscvar VARCHAR(65535)   ENCODE lzo
	,chartaccts VARCHAR(65535)   ENCODE lzo
	,upmod VARCHAR(65535)   ENCODE lzo
	,currunit VARCHAR(65535)   ENCODE lzo
	,debit NUMERIC(38,10)   ENCODE az64
	,credit NUMERIC(38,10)   ENCODE az64
	,adquanunit VARCHAR(65535)   ENCODE lzo
	,adquantity NUMERIC(38,10)   ENCODE az64
	,rownum VARCHAR(65535)   ENCODE lzo
	,runid BIGINT   ENCODE az64
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE EVEN
;
ALTER TABLE bods.e03_3fi_sl_h1_si_current_2019 owner to base_admin;


-- bods.e03_3fi_sl_h1_si_current_t1 definition

-- Drop table

-- DROP TABLE bods.e03_3fi_sl_h1_si_current_t1;

--DROP TABLE bods.e03_3fi_sl_h1_si_current_t1;
CREATE TABLE IF NOT EXISTS bods.e03_3fi_sl_h1_si_current_t1
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id BIGINT   ENCODE az64
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,ver VARCHAR(65535)   ENCODE lzo
	,"year" NUMERIC(38,10)   ENCODE az64
	,currkey VARCHAR(65535)   ENCODE lzo
	,doctype VARCHAR(65535)   ENCODE lzo
	,gl_sirid VARCHAR(65535)   ENCODE lzo
	,docno VARCHAR(65535)   ENCODE lzo
	,docline VARCHAR(65535)   ENCODE lzo
	,hfmacct VARCHAR(65535)   ENCODE lzo
	,hfment VARCHAR(65535)   ENCODE lzo
	,hfmc1 VARCHAR(65535)   ENCODE lzo
	,hpent VARCHAR(65535)   ENCODE lzo
	,hpsbu VARCHAR(65535)   ENCODE lzo
	,cocode VARCHAR(65535)   ENCODE lzo
	,shiptoctry VARCHAR(65535)   ENCODE lzo
	,tradepart VARCHAR(65535)   ENCODE lzo
	,coarea VARCHAR(65535)   ENCODE lzo
	,profctr VARCHAR(65535)   ENCODE lzo
	,trantype VARCHAR(65535)   ENCODE lzo
	,costctr VARCHAR(65535)   ENCODE lzo
	,funarea VARCHAR(65535)   ENCODE lzo
	,acct VARCHAR(65535)   ENCODE lzo
	,sl_doctype VARCHAR(65535)   ENCODE lzo
	,plant VARCHAR(65535)   ENCODE lzo
	,salesoff VARCHAR(65535)   ENCODE lzo
	,indkey VARCHAR(65535)   ENCODE lzo
	,date_ VARCHAR(65535)   ENCODE lzo
	,refdoc VARCHAR(65535)   ENCODE lzo
	,refitm NUMERIC(38,10)   ENCODE az64
	,refdoccat VARCHAR(65535)   ENCODE lzo
	,refproc VARCHAR(65535)   ENCODE lzo
	,product VARCHAR(65535)   ENCODE lzo
	,ordno VARCHAR(65535)   ENCODE lzo
	,soldtocust VARCHAR(65535)   ENCODE lzo
	,pehgrp VARCHAR(65535)   ENCODE lzo
	,shiptocust VARCHAR(65535)   ENCODE lzo
	,payer VARCHAR(65535)   ENCODE lzo
	,salesdiv VARCHAR(65535)   ENCODE lzo
	,distchan VARCHAR(65535)   ENCODE lzo
	,gppport VARCHAR(65535)   ENCODE lzo
	,period VARCHAR(65535)   ENCODE lzo
	,valuetype NUMERIC(38,10)   ENCODE az64
	,yydiv VARCHAR(65535)   ENCODE lzo
	,vkorg VARCHAR(65535)   ENCODE lzo
	,cpudt VARCHAR(65535)   ENCODE lzo
	,cputm VARCHAR(65535)   ENCODE lzo
	,awtyp VARCHAR(65535)   ENCODE lzo
	,belnr VARCHAR(65535)   ENCODE lzo
	,buzei NUMERIC(38,10)   ENCODE az64
	,amt NUMERIC(38,10)   ENCODE az64
	,profctrgrp VARCHAR(65535)   ENCODE lzo
	,usertemp2 VARCHAR(65535)   ENCODE lzo
	,entitygrp BIGINT   ENCODE az64
	,int_entitytype VARCHAR(65535)   ENCODE lzo
	,int_brandgrp VARCHAR(65535)   ENCODE lzo
	,int_geogrp VARCHAR(65535)   ENCODE lzo
	,int_salesgrp VARCHAR(65535)   ENCODE lzo
	,int_saldistrict VARCHAR(65535)   ENCODE lzo
	,int_functype VARCHAR(65535)   ENCODE lzo
	,int_salesgrp2 VARCHAR(65535)   ENCODE lzo
	,quanunit VARCHAR(65535)   ENCODE lzo
	,quantity NUMERIC(38,10)   ENCODE az64
	,curtype VARCHAR(65535)   ENCODE lzo
	,poper NUMERIC(38,10)   ENCODE az64
	,rzz101 VARCHAR(65535)   ENCODE lzo
	,rzz107 VARCHAR(65535)   ENCODE lzo
	,rzz108 VARCHAR(65535)   ENCODE lzo
	,rzz111 VARCHAR(65535)   ENCODE lzo
	,rzz102 VARCHAR(65535)   ENCODE lzo
	,rzz113 VARCHAR(65535)   ENCODE lzo
	,logsys VARCHAR(65535)   ENCODE lzo
	,sgtxt VARCHAR(65535)   ENCODE lzo
	,activ VARCHAR(65535)   ENCODE lzo
	,yyhpact VARCHAR(65535)   ENCODE lzo
	,wsdat VARCHAR(65535)   ENCODE lzo
	,refryear NUMERIC(38,10)   ENCODE az64
	,usnam VARCHAR(65535)   ENCODE lzo
	,aworg VARCHAR(65535)   ENCODE lzo
	,rwcur VARCHAR(65535)   ENCODE lzo
	,xsplitmod VARCHAR(65535)   ENCODE lzo
	,erkrs VARCHAR(65535)   ENCODE lzo
	,paobjnr NUMERIC(38,10)   ENCODE az64
	,pasubnr NUMERIC(38,10)   ENCODE az64
	,ps_psp_pnr NUMERIC(38,10)   ENCODE az64
	,kdauf VARCHAR(65535)   ENCODE lzo
	,kdpos NUMERIC(38,10)   ENCODE az64
	,fiscvar VARCHAR(65535)   ENCODE lzo
	,chartaccts VARCHAR(65535)   ENCODE lzo
	,upmod VARCHAR(65535)   ENCODE lzo
	,currunit VARCHAR(65535)   ENCODE lzo
	,debit NUMERIC(38,10)   ENCODE az64
	,credit NUMERIC(38,10)   ENCODE az64
	,adquanunit VARCHAR(65535)   ENCODE lzo
	,adquantity NUMERIC(38,10)   ENCODE az64
	,rownum VARCHAR(65535)   ENCODE lzo
	,runid BIGINT   ENCODE az64
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE EVEN
;
ALTER TABLE bods.e03_3fi_sl_h1_si_current_t1 owner to base_admin;


-- bods.exact_pl_trans_archive_current definition

-- Drop table

-- DROP TABLE bods.exact_pl_trans_archive_current;

--DROP TABLE bods.exact_pl_trans_archive_current;
CREATE TABLE IF NOT EXISTS bods.exact_pl_trans_archive_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id VARCHAR(65535)   ENCODE lzo
	,fiscper VARCHAR(65535)   ENCODE lzo
	,fyr_id VARCHAR(65535)   ENCODE lzo
	,fmth_nbr VARCHAR(65535)   ENCODE lzo
	,co_cd VARCHAR(65535)   ENCODE lzo
	,acct_grp VARCHAR(65535)   ENCODE lzo
	,acct VARCHAR(65535)   ENCODE lzo
	,txn_id VARCHAR(65535)   ENCODE lzo
	,txn_dte VARCHAR(65535)   ENCODE lzo
	,crncy_cd VARCHAR(65535)   ENCODE lzo
	,amt VARCHAR(65535)   ENCODE lzo
	,usd_amt VARCHAR(65535)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_amt VARCHAR(65535)   ENCODE lzo
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.exact_pl_trans_archive_current owner to base_admin;


-- bods.extr_baan_powers_cn_pl_current definition

-- Drop table

-- DROP TABLE bods.extr_baan_powers_cn_pl_current;

--DROP TABLE bods.extr_baan_powers_cn_pl_current;
CREATE TABLE IF NOT EXISTS bods.extr_baan_powers_cn_pl_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,extr_baan_powers_cn_pl_id BIGINT   ENCODE az64
	,fiscper BIGINT   ENCODE az64
	,"year" BIGINT   ENCODE az64
	,period BIGINT   ENCODE az64
	,co_code VARCHAR(65535)   ENCODE lzo
	,"account" VARCHAR(65535)   ENCODE lzo
	,doc_num VARCHAR(65535)   ENCODE lzo
	,doc_line_num BIGINT   ENCODE az64
	,seq_num BIGINT   ENCODE az64
	,background_seq_num BIGINT   ENCODE az64
	,posting_date VARCHAR(65535)   ENCODE lzo
	,cost_center VARCHAR(65535)   ENCODE lzo
	,currency VARCHAR(65535)   ENCODE lzo
	,amount NUMERIC(38,10)   ENCODE az64
	,usd_amount NUMERIC(38,10)   ENCODE az64
	,runid BIGINT   ENCODE az64
	,loaddatetime VARCHAR(65535)   ENCODE lzo
	,ins_dtm VARCHAR(65535)   ENCODE lzo
	,upd_dtm VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.extr_baan_powers_cn_pl_current owner to base_admin;


-- bods.extr_p10_0customer_attr_current definition

-- Drop table

-- DROP TABLE bods.extr_p10_0customer_attr_current;

--DROP TABLE bods.extr_p10_0customer_attr_current;
CREATE TABLE IF NOT EXISTS bods.extr_p10_0customer_attr_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id VARCHAR(65535)   ENCODE lzo
	,mandt VARCHAR(65535)   ENCODE lzo
	,kunnr VARCHAR(65535)   ENCODE lzo
	,adrnr VARCHAR(65535)   ENCODE lzo
	,anred VARCHAR(65535)   ENCODE lzo
	,aufsd VARCHAR(65535)   ENCODE lzo
	,bahne VARCHAR(65535)   ENCODE lzo
	,bahns VARCHAR(65535)   ENCODE lzo
	,bbbnr NUMERIC(38,10)   ENCODE az64
	,bbsnr NUMERIC(38,10)   ENCODE az64
	,begru VARCHAR(65535)   ENCODE lzo
	,brsch VARCHAR(65535)   ENCODE lzo
	,bubkz NUMERIC(38,10)   ENCODE az64
	,datlt VARCHAR(65535)   ENCODE lzo
	,erdat VARCHAR(65535)   ENCODE lzo
	,ernam VARCHAR(65535)   ENCODE lzo
	,exabl VARCHAR(65535)   ENCODE lzo
	,faksd VARCHAR(65535)   ENCODE lzo
	,fiskn VARCHAR(65535)   ENCODE lzo
	,knazk VARCHAR(65535)   ENCODE lzo
	,knrza VARCHAR(65535)   ENCODE lzo
	,konzs VARCHAR(65535)   ENCODE lzo
	,ktokd VARCHAR(65535)   ENCODE lzo
	,kukla VARCHAR(65535)   ENCODE lzo
	,land1 VARCHAR(65535)   ENCODE lzo
	,lifnr VARCHAR(65535)   ENCODE lzo
	,lifsd VARCHAR(65535)   ENCODE lzo
	,locco VARCHAR(65535)   ENCODE lzo
	,loevm VARCHAR(65535)   ENCODE lzo
	,name1 VARCHAR(65535)   ENCODE lzo
	,name2 VARCHAR(65535)   ENCODE lzo
	,name3 VARCHAR(65535)   ENCODE lzo
	,name4 VARCHAR(65535)   ENCODE lzo
	,niels VARCHAR(65535)   ENCODE lzo
	,ort01 VARCHAR(65535)   ENCODE lzo
	,ort02 VARCHAR(65535)   ENCODE lzo
	,pfach VARCHAR(65535)   ENCODE lzo
	,pstl2 VARCHAR(65535)   ENCODE lzo
	,pstlz VARCHAR(65535)   ENCODE lzo
	,regio VARCHAR(65535)   ENCODE lzo
	,counc VARCHAR(65535)   ENCODE lzo
	,cityc VARCHAR(65535)   ENCODE lzo
	,rpmkr VARCHAR(65535)   ENCODE lzo
	,sortl VARCHAR(65535)   ENCODE lzo
	,sperr VARCHAR(65535)   ENCODE lzo
	,spras VARCHAR(65535)   ENCODE lzo
	,stcd1 VARCHAR(65535)   ENCODE lzo
	,stcd2 VARCHAR(65535)   ENCODE lzo
	,stkza VARCHAR(65535)   ENCODE lzo
	,stkzu VARCHAR(65535)   ENCODE lzo
	,stras VARCHAR(65535)   ENCODE lzo
	,telbx VARCHAR(65535)   ENCODE lzo
	,telf1 VARCHAR(65535)   ENCODE lzo
	,telf2 VARCHAR(65535)   ENCODE lzo
	,telfx VARCHAR(65535)   ENCODE lzo
	,teltx VARCHAR(65535)   ENCODE lzo
	,telx1 VARCHAR(65535)   ENCODE lzo
	,lzone VARCHAR(65535)   ENCODE lzo
	,xcpdk VARCHAR(65535)   ENCODE lzo
	,xzemp VARCHAR(65535)   ENCODE lzo
	,vbund VARCHAR(65535)   ENCODE lzo
	,stceg VARCHAR(65535)   ENCODE lzo
	,dear1 VARCHAR(65535)   ENCODE lzo
	,dear2 VARCHAR(65535)   ENCODE lzo
	,dear3 VARCHAR(65535)   ENCODE lzo
	,dear4 VARCHAR(65535)   ENCODE lzo
	,dear5 VARCHAR(65535)   ENCODE lzo
	,dear6 VARCHAR(65535)   ENCODE lzo
	,gform VARCHAR(65535)   ENCODE lzo
	,bran1 VARCHAR(65535)   ENCODE lzo
	,bran2 VARCHAR(65535)   ENCODE lzo
	,bran3 VARCHAR(65535)   ENCODE lzo
	,bran4 VARCHAR(65535)   ENCODE lzo
	,bran5 VARCHAR(65535)   ENCODE lzo
	,ekont VARCHAR(65535)   ENCODE lzo
	,umsat VARCHAR(65535)   ENCODE lzo
	,umjah NUMERIC(38,10)   ENCODE az64
	,uwaer NUMERIC(38,10)   ENCODE az64
	,jmzah VARCHAR(65535)   ENCODE lzo
	,jmjah NUMERIC(38,10)   ENCODE az64
	,katr1 VARCHAR(65535)   ENCODE lzo
	,katr2 VARCHAR(65535)   ENCODE lzo
	,katr3 VARCHAR(65535)   ENCODE lzo
	,katr4 VARCHAR(65535)   ENCODE lzo
	,katr5 VARCHAR(65535)   ENCODE lzo
	,katr6 VARCHAR(65535)   ENCODE lzo
	,katr7 VARCHAR(65535)   ENCODE lzo
	,katr8 VARCHAR(65535)   ENCODE lzo
	,katr9 VARCHAR(65535)   ENCODE lzo
	,katr10 VARCHAR(65535)   ENCODE lzo
	,stkzn VARCHAR(65535)   ENCODE lzo
	,umsa1 NUMERIC(38,10)   ENCODE az64
	,txjcd VARCHAR(65535)   ENCODE lzo
	,mcod1 VARCHAR(65535)   ENCODE lzo
	,mcod2 VARCHAR(65535)   ENCODE lzo
	,mcod3 VARCHAR(65535)   ENCODE lzo
	,periv VARCHAR(65535)   ENCODE lzo
	,abrvw VARCHAR(65535)   ENCODE lzo
	,inspbydebi VARCHAR(65535)   ENCODE lzo
	,inspatdebi VARCHAR(65535)   ENCODE lzo
	,ktocd VARCHAR(65535)   ENCODE lzo
	,pfort VARCHAR(65535)   ENCODE lzo
	,werks VARCHAR(65535)   ENCODE lzo
	,dtams VARCHAR(65535)   ENCODE lzo
	,dtaws VARCHAR(65535)   ENCODE lzo
	,duefl VARCHAR(65535)   ENCODE lzo
	,hzuor NUMERIC(38,10)   ENCODE az64
	,sperz VARCHAR(65535)   ENCODE lzo
	,etikg VARCHAR(65535)   ENCODE lzo
	,civve VARCHAR(65535)   ENCODE lzo
	,milve VARCHAR(65535)   ENCODE lzo
	,kdkg1 VARCHAR(65535)   ENCODE lzo
	,kdkg2 VARCHAR(65535)   ENCODE lzo
	,kdkg3 VARCHAR(65535)   ENCODE lzo
	,kdkg4 VARCHAR(65535)   ENCODE lzo
	,kdkg5 VARCHAR(65535)   ENCODE lzo
	,xknza VARCHAR(65535)   ENCODE lzo
	,fityp VARCHAR(65535)   ENCODE lzo
	,stcdt VARCHAR(65535)   ENCODE lzo
	,stcd3 VARCHAR(65535)   ENCODE lzo
	,stcd4 VARCHAR(65535)   ENCODE lzo
	,xicms VARCHAR(65535)   ENCODE lzo
	,xxipi VARCHAR(65535)   ENCODE lzo
	,xsubt VARCHAR(65535)   ENCODE lzo
	,cfopc VARCHAR(65535)   ENCODE lzo
	,txlw1 VARCHAR(65535)   ENCODE lzo
	,txlw2 VARCHAR(65535)   ENCODE lzo
	,ccc01 VARCHAR(65535)   ENCODE lzo
	,ccc02 VARCHAR(65535)   ENCODE lzo
	,ccc03 VARCHAR(65535)   ENCODE lzo
	,ccc04 VARCHAR(65535)   ENCODE lzo
	,cassd VARCHAR(65535)   ENCODE lzo
	,knurl VARCHAR(65535)   ENCODE lzo
	,odq_changemode VARCHAR(65535)   ENCODE lzo
	,odq_entitycntr NUMERIC(38,10)   ENCODE az64
	,ins_dtm VARCHAR(65535)   ENCODE lzo
	,upd_dtm VARCHAR(65535)   ENCODE lzo
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.extr_p10_0customer_attr_current owner to base_admin;


-- bods.extr_p10_0material_attr_current definition

-- Drop table

-- DROP TABLE bods.extr_p10_0material_attr_current;

--DROP TABLE bods.extr_p10_0material_attr_current;
CREATE TABLE IF NOT EXISTS bods.extr_p10_0material_attr_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id BIGINT   ENCODE az64
	,mandt VARCHAR(65535)   ENCODE lzo
	,matnr VARCHAR(65535)   ENCODE lzo
	,ersda VARCHAR(65535)   ENCODE lzo
	,ernam VARCHAR(65535)   ENCODE lzo
	,laeda VARCHAR(65535)   ENCODE lzo
	,aenam VARCHAR(65535)   ENCODE lzo
	,vpsta VARCHAR(65535)   ENCODE lzo
	,pstat VARCHAR(65535)   ENCODE lzo
	,lvorm VARCHAR(65535)   ENCODE lzo
	,mtart VARCHAR(65535)   ENCODE lzo
	,mbrsh VARCHAR(65535)   ENCODE lzo
	,matkl VARCHAR(65535)   ENCODE lzo
	,bismt VARCHAR(65535)   ENCODE lzo
	,meins VARCHAR(65535)   ENCODE lzo
	,bstme VARCHAR(65535)   ENCODE lzo
	,zeinr VARCHAR(65535)   ENCODE lzo
	,zeiar VARCHAR(65535)   ENCODE lzo
	,zeivr VARCHAR(65535)   ENCODE lzo
	,zeifo VARCHAR(65535)   ENCODE lzo
	,aeszn VARCHAR(65535)   ENCODE lzo
	,blatt VARCHAR(65535)   ENCODE lzo
	,blanz NUMERIC(38,10)   ENCODE az64
	,ferth VARCHAR(65535)   ENCODE lzo
	,formt VARCHAR(65535)   ENCODE lzo
	,groes VARCHAR(65535)   ENCODE lzo
	,wrkst VARCHAR(65535)   ENCODE lzo
	,normt VARCHAR(65535)   ENCODE lzo
	,labor VARCHAR(65535)   ENCODE lzo
	,ekwsl VARCHAR(65535)   ENCODE lzo
	,brgew NUMERIC(38,10)   ENCODE az64
	,ntgew NUMERIC(38,10)   ENCODE az64
	,gewei VARCHAR(65535)   ENCODE lzo
	,volum NUMERIC(38,10)   ENCODE az64
	,voleh VARCHAR(65535)   ENCODE lzo
	,behvo VARCHAR(65535)   ENCODE lzo
	,raube VARCHAR(65535)   ENCODE lzo
	,tempb VARCHAR(65535)   ENCODE lzo
	,disst VARCHAR(65535)   ENCODE lzo
	,tragr VARCHAR(65535)   ENCODE lzo
	,stoff VARCHAR(65535)   ENCODE lzo
	,spart VARCHAR(65535)   ENCODE lzo
	,kunnr VARCHAR(65535)   ENCODE lzo
	,eannr NUMERIC(38,10)   ENCODE az64
	,wesch VARCHAR(65535)   ENCODE lzo
	,bwvor VARCHAR(65535)   ENCODE lzo
	,bwscl VARCHAR(65535)   ENCODE lzo
	,saiso VARCHAR(65535)   ENCODE lzo
	,etiar VARCHAR(65535)   ENCODE lzo
	,etifo VARCHAR(65535)   ENCODE lzo
	,entar VARCHAR(65535)   ENCODE lzo
	,ean11 VARCHAR(65535)   ENCODE lzo
	,numtp VARCHAR(65535)   ENCODE lzo
	,laeng NUMERIC(38,10)   ENCODE az64
	,breit NUMERIC(38,10)   ENCODE az64
	,hoehe NUMERIC(38,10)   ENCODE az64
	,meabm VARCHAR(65535)   ENCODE lzo
	,prdha VARCHAR(65535)   ENCODE lzo
	,aeklk VARCHAR(65535)   ENCODE lzo
	,cadkz VARCHAR(65535)   ENCODE lzo
	,qmpur VARCHAR(65535)   ENCODE lzo
	,ergew NUMERIC(38,10)   ENCODE az64
	,ergei VARCHAR(65535)   ENCODE lzo
	,ervol NUMERIC(38,10)   ENCODE az64
	,ervoe VARCHAR(65535)   ENCODE lzo
	,gewto NUMERIC(38,10)   ENCODE az64
	,volto NUMERIC(38,10)   ENCODE az64
	,vabme VARCHAR(65535)   ENCODE lzo
	,kzrev VARCHAR(65535)   ENCODE lzo
	,kzkfg VARCHAR(65535)   ENCODE lzo
	,xchpf VARCHAR(65535)   ENCODE lzo
	,vhart VARCHAR(65535)   ENCODE lzo
	,fuelg NUMERIC(38,10)   ENCODE az64
	,stfak NUMERIC(38,10)   ENCODE az64
	,magrv VARCHAR(65535)   ENCODE lzo
	,begru VARCHAR(65535)   ENCODE lzo
	,datab VARCHAR(65535)   ENCODE lzo
	,liqdt VARCHAR(65535)   ENCODE lzo
	,saisj VARCHAR(65535)   ENCODE lzo
	,plgtp VARCHAR(65535)   ENCODE lzo
	,mlgut VARCHAR(65535)   ENCODE lzo
	,extwg VARCHAR(65535)   ENCODE lzo
	,satnr VARCHAR(65535)   ENCODE lzo
	,attyp VARCHAR(65535)   ENCODE lzo
	,kzkup VARCHAR(65535)   ENCODE lzo
	,kznfm VARCHAR(65535)   ENCODE lzo
	,pmata VARCHAR(65535)   ENCODE lzo
	,mstae VARCHAR(65535)   ENCODE lzo
	,mstav VARCHAR(65535)   ENCODE lzo
	,mstde VARCHAR(65535)   ENCODE lzo
	,mstdv VARCHAR(65535)   ENCODE lzo
	,taklv VARCHAR(65535)   ENCODE lzo
	,rbnrm VARCHAR(65535)   ENCODE lzo
	,mhdrz NUMERIC(38,10)   ENCODE az64
	,mhdhb NUMERIC(38,10)   ENCODE az64
	,mhdlp NUMERIC(38,10)   ENCODE az64
	,inhme VARCHAR(65535)   ENCODE lzo
	,inhal NUMERIC(38,10)   ENCODE az64
	,vpreh NUMERIC(38,10)   ENCODE az64
	,etiag VARCHAR(65535)   ENCODE lzo
	,inhbr NUMERIC(38,10)   ENCODE az64
	,cmeth VARCHAR(65535)   ENCODE lzo
	,cuobf NUMERIC(38,10)   ENCODE az64
	,kzumw VARCHAR(65535)   ENCODE lzo
	,kosch VARCHAR(65535)   ENCODE lzo
	,sprof VARCHAR(65535)   ENCODE lzo
	,nrfhg VARCHAR(65535)   ENCODE lzo
	,mfrpn VARCHAR(65535)   ENCODE lzo
	,mfrnr VARCHAR(65535)   ENCODE lzo
	,bmatn VARCHAR(65535)   ENCODE lzo
	,mprof VARCHAR(65535)   ENCODE lzo
	,kzwsm VARCHAR(65535)   ENCODE lzo
	,saity VARCHAR(65535)   ENCODE lzo
	,profl VARCHAR(65535)   ENCODE lzo
	,ihivi VARCHAR(65535)   ENCODE lzo
	,iloos VARCHAR(65535)   ENCODE lzo
	,serlv VARCHAR(65535)   ENCODE lzo
	,kzgvh VARCHAR(65535)   ENCODE lzo
	,xgchp VARCHAR(65535)   ENCODE lzo
	,kzeff VARCHAR(65535)   ENCODE lzo
	,compl VARCHAR(65535)   ENCODE lzo
	,iprkz NUMERIC(38,10)   ENCODE az64
	,rdmhd VARCHAR(65535)   ENCODE lzo
	,przus VARCHAR(65535)   ENCODE lzo
	,mtpos_mara VARCHAR(65535)   ENCODE lzo
	,bflme VARCHAR(65535)   ENCODE lzo
	,color_atinn NUMERIC(38,10)   ENCODE az64
	,size1_atinn NUMERIC(38,10)   ENCODE az64
	,size2_atinn NUMERIC(38,10)   ENCODE az64
	,color VARCHAR(65535)   ENCODE lzo
	,size1 VARCHAR(65535)   ENCODE lzo
	,size2 VARCHAR(65535)   ENCODE lzo
	,free_char VARCHAR(65535)   ENCODE lzo
	,care_code VARCHAR(65535)   ENCODE lzo
	,brand_id VARCHAR(65535)   ENCODE lzo
	,fiber_code1 VARCHAR(65535)   ENCODE lzo
	,fiber_part1 NUMERIC(38,10)   ENCODE az64
	,fiber_code2 VARCHAR(65535)   ENCODE lzo
	,fiber_part2 NUMERIC(38,10)   ENCODE az64
	,fiber_code3 VARCHAR(65535)   ENCODE lzo
	,fiber_part3 NUMERIC(38,10)   ENCODE az64
	,fiber_code4 VARCHAR(65535)   ENCODE lzo
	,fiber_part4 NUMERIC(38,10)   ENCODE az64
	,fashgrd VARCHAR(65535)   ENCODE lzo
	,odq_changemode VARCHAR(65535)   ENCODE lzo
	,odq_entitycntr NUMERIC(38,10)   ENCODE az64
	,ins_dtm VARCHAR(65535)   ENCODE lzo
	,upd_dtm VARCHAR(65535)   ENCODE lzo
	,runid BIGINT   ENCODE az64
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.extr_p10_0material_attr_current owner to base_admin;


-- bods.extr_shp_customer_attr_current definition

-- Drop table

-- DROP TABLE bods.extr_shp_customer_attr_current;

--DROP TABLE bods.extr_shp_customer_attr_current;
CREATE TABLE IF NOT EXISTS bods.extr_shp_customer_attr_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,kunnr VARCHAR(65535)   ENCODE lzo
	,adrnr VARCHAR(65535)   ENCODE lzo
	,anred VARCHAR(65535)   ENCODE lzo
	,aufsd VARCHAR(65535)   ENCODE lzo
	,bahne VARCHAR(65535)   ENCODE lzo
	,bahns VARCHAR(65535)   ENCODE lzo
	,bbbnr NUMERIC(38,10)   ENCODE az64
	,bbsnr NUMERIC(38,10)   ENCODE az64
	,begru VARCHAR(65535)   ENCODE lzo
	,brsch VARCHAR(65535)   ENCODE lzo
	,bubkz NUMERIC(38,10)   ENCODE az64
	,datlt VARCHAR(65535)   ENCODE lzo
	,erdat VARCHAR(65535)   ENCODE lzo
	,ernam VARCHAR(65535)   ENCODE lzo
	,exabl VARCHAR(65535)   ENCODE lzo
	,faksd VARCHAR(65535)   ENCODE lzo
	,fiskn VARCHAR(65535)   ENCODE lzo
	,knazk VARCHAR(65535)   ENCODE lzo
	,knrza VARCHAR(65535)   ENCODE lzo
	,konzs VARCHAR(65535)   ENCODE lzo
	,ktokd VARCHAR(65535)   ENCODE lzo
	,kukla VARCHAR(65535)   ENCODE lzo
	,land1 VARCHAR(65535)   ENCODE lzo
	,lifnr VARCHAR(65535)   ENCODE lzo
	,lifsd VARCHAR(65535)   ENCODE lzo
	,locco VARCHAR(65535)   ENCODE lzo
	,loevm VARCHAR(65535)   ENCODE lzo
	,name1 VARCHAR(65535)   ENCODE lzo
	,name2 VARCHAR(65535)   ENCODE lzo
	,name3 VARCHAR(65535)   ENCODE lzo
	,name4 VARCHAR(65535)   ENCODE lzo
	,niels VARCHAR(65535)   ENCODE lzo
	,ort01 VARCHAR(65535)   ENCODE lzo
	,ort02 VARCHAR(65535)   ENCODE lzo
	,pfach VARCHAR(65535)   ENCODE lzo
	,pstl2 VARCHAR(65535)   ENCODE lzo
	,pstlz VARCHAR(65535)   ENCODE lzo
	,regio VARCHAR(65535)   ENCODE lzo
	,counc VARCHAR(65535)   ENCODE lzo
	,cityc VARCHAR(65535)   ENCODE lzo
	,rpmkr VARCHAR(65535)   ENCODE lzo
	,sortl VARCHAR(65535)   ENCODE lzo
	,sperr VARCHAR(65535)   ENCODE lzo
	,spras VARCHAR(65535)   ENCODE lzo
	,stcd1 VARCHAR(65535)   ENCODE lzo
	,stcd2 VARCHAR(65535)   ENCODE lzo
	,stkza VARCHAR(65535)   ENCODE lzo
	,stkzu VARCHAR(65535)   ENCODE lzo
	,stras VARCHAR(65535)   ENCODE lzo
	,telbx VARCHAR(65535)   ENCODE lzo
	,telf1 VARCHAR(65535)   ENCODE lzo
	,telf2 VARCHAR(65535)   ENCODE lzo
	,telfx VARCHAR(65535)   ENCODE lzo
	,teltx VARCHAR(65535)   ENCODE lzo
	,telx1 VARCHAR(65535)   ENCODE lzo
	,lzone VARCHAR(65535)   ENCODE lzo
	,xcpdk VARCHAR(65535)   ENCODE lzo
	,xzemp VARCHAR(65535)   ENCODE lzo
	,vbund VARCHAR(65535)   ENCODE lzo
	,stceg VARCHAR(65535)   ENCODE lzo
	,dear1 VARCHAR(65535)   ENCODE lzo
	,dear2 VARCHAR(65535)   ENCODE lzo
	,dear3 VARCHAR(65535)   ENCODE lzo
	,dear4 VARCHAR(65535)   ENCODE lzo
	,dear5 VARCHAR(65535)   ENCODE lzo
	,dear6 VARCHAR(65535)   ENCODE lzo
	,gform VARCHAR(65535)   ENCODE lzo
	,bran1 VARCHAR(65535)   ENCODE lzo
	,bran2 VARCHAR(65535)   ENCODE lzo
	,bran3 VARCHAR(65535)   ENCODE lzo
	,bran4 VARCHAR(65535)   ENCODE lzo
	,bran5 VARCHAR(65535)   ENCODE lzo
	,ekont VARCHAR(65535)   ENCODE lzo
	,umsat NUMERIC(38,10)   ENCODE az64
	,umjah NUMERIC(38,10)   ENCODE az64
	,uwaer VARCHAR(65535)   ENCODE lzo
	,jmzah NUMERIC(38,10)   ENCODE az64
	,jmjah NUMERIC(38,10)   ENCODE az64
	,katr1 VARCHAR(65535)   ENCODE lzo
	,katr2 VARCHAR(65535)   ENCODE lzo
	,katr3 VARCHAR(65535)   ENCODE lzo
	,katr4 VARCHAR(65535)   ENCODE lzo
	,katr5 VARCHAR(65535)   ENCODE lzo
	,katr6 VARCHAR(65535)   ENCODE lzo
	,katr7 VARCHAR(65535)   ENCODE lzo
	,katr8 VARCHAR(65535)   ENCODE lzo
	,katr9 VARCHAR(65535)   ENCODE lzo
	,katr10 VARCHAR(65535)   ENCODE lzo
	,stkzn VARCHAR(65535)   ENCODE lzo
	,umsa1 NUMERIC(38,10)   ENCODE az64
	,txjcd VARCHAR(65535)   ENCODE lzo
	,mcod1 VARCHAR(65535)   ENCODE lzo
	,mcod2 VARCHAR(65535)   ENCODE lzo
	,mcod3 VARCHAR(65535)   ENCODE lzo
	,periv VARCHAR(65535)   ENCODE lzo
	,abrvw VARCHAR(65535)   ENCODE lzo
	,inspbydebi VARCHAR(65535)   ENCODE lzo
	,inspatdebi VARCHAR(65535)   ENCODE lzo
	,ktocd VARCHAR(65535)   ENCODE lzo
	,pfort VARCHAR(65535)   ENCODE lzo
	,werks VARCHAR(65535)   ENCODE lzo
	,dtams VARCHAR(65535)   ENCODE lzo
	,dtaws VARCHAR(65535)   ENCODE lzo
	,duefl VARCHAR(65535)   ENCODE lzo
	,hzuor NUMERIC(38,10)   ENCODE az64
	,sperz VARCHAR(65535)   ENCODE lzo
	,etikg VARCHAR(65535)   ENCODE lzo
	,civve VARCHAR(65535)   ENCODE lzo
	,milve VARCHAR(65535)   ENCODE lzo
	,kdkg1 VARCHAR(65535)   ENCODE lzo
	,kdkg2 VARCHAR(65535)   ENCODE lzo
	,kdkg3 VARCHAR(65535)   ENCODE lzo
	,kdkg4 VARCHAR(65535)   ENCODE lzo
	,kdkg5 VARCHAR(65535)   ENCODE lzo
	,xknza VARCHAR(65535)   ENCODE lzo
	,fityp VARCHAR(65535)   ENCODE lzo
	,stcdt VARCHAR(65535)   ENCODE lzo
	,stcd3 VARCHAR(65535)   ENCODE lzo
	,stcd4 VARCHAR(65535)   ENCODE lzo
	,xicms VARCHAR(65535)   ENCODE lzo
	,xxipi VARCHAR(65535)   ENCODE lzo
	,xsubt VARCHAR(65535)   ENCODE lzo
	,cfopc VARCHAR(65535)   ENCODE lzo
	,txlw1 VARCHAR(65535)   ENCODE lzo
	,txlw2 VARCHAR(65535)   ENCODE lzo
	,ccc01 VARCHAR(65535)   ENCODE lzo
	,ccc02 VARCHAR(65535)   ENCODE lzo
	,ccc03 VARCHAR(65535)   ENCODE lzo
	,ccc04 VARCHAR(65535)   ENCODE lzo
	,cassd VARCHAR(65535)   ENCODE lzo
	,knurl VARCHAR(65535)   ENCODE lzo
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.extr_shp_customer_attr_current owner to base_admin;


-- bods.extr_shp_material_attr_current definition

-- Drop table

-- DROP TABLE bods.extr_shp_material_attr_current;

--DROP TABLE bods.extr_shp_material_attr_current;
CREATE TABLE IF NOT EXISTS bods.extr_shp_material_attr_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,matnr VARCHAR(65535)   ENCODE lzo
	,ersda VARCHAR(65535)   ENCODE lzo
	,ernam VARCHAR(65535)   ENCODE lzo
	,laeda VARCHAR(65535)   ENCODE lzo
	,aenam VARCHAR(65535)   ENCODE lzo
	,vpsta VARCHAR(65535)   ENCODE lzo
	,pstat VARCHAR(65535)   ENCODE lzo
	,lvorm VARCHAR(65535)   ENCODE lzo
	,mtart VARCHAR(65535)   ENCODE lzo
	,mbrsh VARCHAR(65535)   ENCODE lzo
	,matkl VARCHAR(65535)   ENCODE lzo
	,bismt VARCHAR(65535)   ENCODE lzo
	,meins VARCHAR(65535)   ENCODE lzo
	,bstme VARCHAR(65535)   ENCODE lzo
	,zeinr VARCHAR(65535)   ENCODE lzo
	,zeiar VARCHAR(65535)   ENCODE lzo
	,zeivr VARCHAR(65535)   ENCODE lzo
	,zeifo VARCHAR(65535)   ENCODE lzo
	,aeszn VARCHAR(65535)   ENCODE lzo
	,blatt VARCHAR(65535)   ENCODE lzo
	,blanz NUMERIC(38,10)   ENCODE az64
	,ferth VARCHAR(65535)   ENCODE lzo
	,formt VARCHAR(65535)   ENCODE lzo
	,groes VARCHAR(65535)   ENCODE lzo
	,wrkst VARCHAR(65535)   ENCODE lzo
	,normt VARCHAR(65535)   ENCODE lzo
	,labor VARCHAR(65535)   ENCODE lzo
	,ekwsl VARCHAR(65535)   ENCODE lzo
	,brgew NUMERIC(38,10)   ENCODE az64
	,ntgew NUMERIC(38,10)   ENCODE az64
	,gewei VARCHAR(65535)   ENCODE lzo
	,volum NUMERIC(38,10)   ENCODE az64
	,voleh VARCHAR(65535)   ENCODE lzo
	,behvo VARCHAR(65535)   ENCODE lzo
	,raube VARCHAR(65535)   ENCODE lzo
	,tempb VARCHAR(65535)   ENCODE lzo
	,disst VARCHAR(65535)   ENCODE lzo
	,tragr VARCHAR(65535)   ENCODE lzo
	,stoff VARCHAR(65535)   ENCODE lzo
	,spart VARCHAR(65535)   ENCODE lzo
	,kunnr VARCHAR(65535)   ENCODE lzo
	,eannr VARCHAR(65535)   ENCODE lzo
	,wesch NUMERIC(38,10)   ENCODE az64
	,bwvor VARCHAR(65535)   ENCODE lzo
	,bwscl VARCHAR(65535)   ENCODE lzo
	,saiso VARCHAR(65535)   ENCODE lzo
	,etiar VARCHAR(65535)   ENCODE lzo
	,etifo VARCHAR(65535)   ENCODE lzo
	,entar VARCHAR(65535)   ENCODE lzo
	,ean11 VARCHAR(65535)   ENCODE lzo
	,numtp VARCHAR(65535)   ENCODE lzo
	,laeng NUMERIC(38,10)   ENCODE az64
	,breit NUMERIC(38,10)   ENCODE az64
	,hoehe NUMERIC(38,10)   ENCODE az64
	,meabm VARCHAR(65535)   ENCODE lzo
	,prdha VARCHAR(65535)   ENCODE lzo
	,aeklk VARCHAR(65535)   ENCODE lzo
	,cadkz VARCHAR(65535)   ENCODE lzo
	,qmpur VARCHAR(65535)   ENCODE lzo
	,ergew NUMERIC(38,10)   ENCODE az64
	,ergei VARCHAR(65535)   ENCODE lzo
	,ervol NUMERIC(38,10)   ENCODE az64
	,ervoe VARCHAR(65535)   ENCODE lzo
	,gewto NUMERIC(38,10)   ENCODE az64
	,volto NUMERIC(38,10)   ENCODE az64
	,vabme VARCHAR(65535)   ENCODE lzo
	,kzrev VARCHAR(65535)   ENCODE lzo
	,kzkfg VARCHAR(65535)   ENCODE lzo
	,xchpf VARCHAR(65535)   ENCODE lzo
	,vhart VARCHAR(65535)   ENCODE lzo
	,fuelg NUMERIC(38,10)   ENCODE az64
	,stfak NUMERIC(38,10)   ENCODE az64
	,magrv VARCHAR(65535)   ENCODE lzo
	,begru VARCHAR(65535)   ENCODE lzo
	,datab VARCHAR(65535)   ENCODE lzo
	,liqdt VARCHAR(65535)   ENCODE lzo
	,saisj VARCHAR(65535)   ENCODE lzo
	,plgtp VARCHAR(65535)   ENCODE lzo
	,mlgut VARCHAR(65535)   ENCODE lzo
	,extwg VARCHAR(65535)   ENCODE lzo
	,satnr VARCHAR(65535)   ENCODE lzo
	,attyp VARCHAR(65535)   ENCODE lzo
	,kzkup VARCHAR(65535)   ENCODE lzo
	,kznfm VARCHAR(65535)   ENCODE lzo
	,pmata VARCHAR(65535)   ENCODE lzo
	,mstae VARCHAR(65535)   ENCODE lzo
	,mstav VARCHAR(65535)   ENCODE lzo
	,mstde VARCHAR(65535)   ENCODE lzo
	,mstdv VARCHAR(65535)   ENCODE lzo
	,taklv VARCHAR(65535)   ENCODE lzo
	,rbnrm VARCHAR(65535)   ENCODE lzo
	,mhdrz NUMERIC(38,10)   ENCODE az64
	,mhdhb NUMERIC(38,10)   ENCODE az64
	,mhdlp NUMERIC(38,10)   ENCODE az64
	,inhme VARCHAR(65535)   ENCODE lzo
	,inhal NUMERIC(38,10)   ENCODE az64
	,vpreh NUMERIC(38,10)   ENCODE az64
	,etiag VARCHAR(65535)   ENCODE lzo
	,inhbr NUMERIC(38,10)   ENCODE az64
	,cmeth VARCHAR(65535)   ENCODE lzo
	,cuobf NUMERIC(38,10)   ENCODE az64
	,kzumw VARCHAR(65535)   ENCODE lzo
	,kosch VARCHAR(65535)   ENCODE lzo
	,sprof VARCHAR(65535)   ENCODE lzo
	,nrfhg VARCHAR(65535)   ENCODE lzo
	,mfrpn VARCHAR(65535)   ENCODE lzo
	,mfrnr VARCHAR(65535)   ENCODE lzo
	,bmatn VARCHAR(65535)   ENCODE lzo
	,mprof VARCHAR(65535)   ENCODE lzo
	,kzwsm VARCHAR(65535)   ENCODE lzo
	,saity VARCHAR(65535)   ENCODE lzo
	,profl VARCHAR(65535)   ENCODE lzo
	,ihivi VARCHAR(65535)   ENCODE lzo
	,iloos VARCHAR(65535)   ENCODE lzo
	,serlv VARCHAR(65535)   ENCODE lzo
	,kzgvh VARCHAR(65535)   ENCODE lzo
	,xgchp VARCHAR(65535)   ENCODE lzo
	,kzeff VARCHAR(65535)   ENCODE lzo
	,compl NUMERIC(38,10)   ENCODE az64
	,iprkz VARCHAR(65535)   ENCODE lzo
	,rdmhd VARCHAR(65535)   ENCODE lzo
	,przus VARCHAR(65535)   ENCODE lzo
	,mtpos_mara VARCHAR(65535)   ENCODE lzo
	,bflme VARCHAR(65535)   ENCODE lzo
	,cwm_xcwmat VARCHAR(65535)   ENCODE lzo
	,cwm_valum VARCHAR(65535)   ENCODE lzo
	,cwm_tolgr VARCHAR(65535)   ENCODE lzo
	,cwm_tara VARCHAR(65535)   ENCODE lzo
	,cwm_tarum VARCHAR(65535)   ENCODE lzo
	,nsnid VARCHAR(65535)   ENCODE lzo
	,color_atinn NUMERIC(38,10)   ENCODE az64
	,size1_atinn NUMERIC(38,10)   ENCODE az64
	,size2_atinn NUMERIC(38,10)   ENCODE az64
	,color VARCHAR(65535)   ENCODE lzo
	,size1 VARCHAR(65535)   ENCODE lzo
	,size2 VARCHAR(65535)   ENCODE lzo
	,free_char VARCHAR(65535)   ENCODE lzo
	,care_code VARCHAR(65535)   ENCODE lzo
	,brand_id VARCHAR(65535)   ENCODE lzo
	,fiber_code1 VARCHAR(65535)   ENCODE lzo
	,fiber_part1 NUMERIC(38,10)   ENCODE az64
	,fiber_code2 VARCHAR(65535)   ENCODE lzo
	,fiber_part2 NUMERIC(38,10)   ENCODE az64
	,fiber_code3 VARCHAR(65535)   ENCODE lzo
	,fiber_part3 NUMERIC(38,10)   ENCODE az64
	,fiber_code4 VARCHAR(65535)   ENCODE lzo
	,fiber_part4 NUMERIC(38,10)   ENCODE az64
	,rpa_wgh1 VARCHAR(65535)   ENCODE lzo
	,rpa_wgh2 VARCHAR(65535)   ENCODE lzo
	,rpa_wgh3 VARCHAR(65535)   ENCODE lzo
	,rpa_wgh4 VARCHAR(65535)   ENCODE lzo
	,fashgrd VARCHAR(65535)   ENCODE lzo
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.extr_shp_material_attr_current owner to base_admin;


-- bods.hfm_vw_hfm_actual_trans_current definition

-- Drop table

-- DROP TABLE bods.hfm_vw_hfm_actual_trans_current;

--DROP TABLE bods.hfm_vw_hfm_actual_trans_current;
CREATE TABLE IF NOT EXISTS bods.hfm_vw_hfm_actual_trans_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id VARCHAR(65535)   ENCODE lzo
	,rectype VARCHAR(65535)   ENCODE lzo
	,"year" VARCHAR(65535)   ENCODE lzo
	,period VARCHAR(65535)   ENCODE lzo
	,entity VARCHAR(65535)   ENCODE lzo
	,acct VARCHAR(65535)   ENCODE lzo
	,custom1 VARCHAR(65535)   ENCODE lzo
	,custom2 VARCHAR(65535)   ENCODE lzo
	,currkey VARCHAR(65535)   ENCODE lzo
	,amt NUMERIC(38,10)   ENCODE az64
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,group_name VARCHAR(65535)   ENCODE lzo
	,runid BIGINT   ENCODE az64
	,loaddatetime VARCHAR(65535)   ENCODE lzo
	,period_partition VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.hfm_vw_hfm_actual_trans_current owner to base_admin;


-- bods.ifs_pl_trans_current definition

-- Drop table

-- DROP TABLE bods.ifs_pl_trans_current;

--DROP TABLE bods.ifs_pl_trans_current;
CREATE TABLE IF NOT EXISTS bods.ifs_pl_trans_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id VARCHAR(65535)   ENCODE lzo
	,fiscper VARCHAR(65535)   ENCODE lzo
	,"account" VARCHAR(65535)   ENCODE lzo
	,account_desc VARCHAR(65535)   ENCODE lzo
	,account_group VARCHAR(65535)   ENCODE lzo
	,account_type VARCHAR(65535)   ENCODE lzo
	,accounting_period VARCHAR(65535)   ENCODE lzo
	,accounting_year VARCHAR(65535)   ENCODE lzo
	,accounting_year_reference VARCHAR(65535)   ENCODE lzo
	,alloc_line_id NUMERIC(38,10)   ENCODE az64
	,allocation_id NUMERIC(38,10)   ENCODE az64
	,amount NUMERIC(38,10)   ENCODE az64
	,approved_by_user_group VARCHAR(65535)   ENCODE lzo
	,approved_by_userid VARCHAR(65535)   ENCODE lzo
	,aut_coding_parent_row NUMERIC(38,10)   ENCODE az64
	,aut_coding_rule VARCHAR(65535)   ENCODE lzo
	,aut_coding_seq NUMERIC(38,10)   ENCODE az64
	,auto_tax_vou_entry VARCHAR(38)   ENCODE lzo
	,autobook VARCHAR(65535)   ENCODE lzo
	,code_b VARCHAR(38)   ENCODE lzo
	,code_b_desc VARCHAR(65535)   ENCODE lzo
	,code_c VARCHAR(65535)   ENCODE lzo
	,code_c_desc VARCHAR(65535)   ENCODE lzo
	,code_d VARCHAR(65535)   ENCODE lzo
	,code_d_desc VARCHAR(65535)   ENCODE lzo
	,code_e VARCHAR(65535)   ENCODE lzo
	,code_e_desc VARCHAR(65535)   ENCODE lzo
	,code_f VARCHAR(65535)   ENCODE lzo
	,code_f_desc VARCHAR(65535)   ENCODE lzo
	,code_g VARCHAR(65535)   ENCODE lzo
	,code_g_desc VARCHAR(65535)   ENCODE lzo
	,code_h VARCHAR(65535)   ENCODE lzo
	,code_h_desc VARCHAR(65535)   ENCODE lzo
	,code_i VARCHAR(65535)   ENCODE lzo
	,code_i_desc VARCHAR(65535)   ENCODE lzo
	,code_j VARCHAR(65535)   ENCODE lzo
	,code_j_desc VARCHAR(65535)   ENCODE lzo
	,company VARCHAR(65535)   ENCODE lzo
	,conversion_factor NUMERIC(38,10)   ENCODE az64
	,corrected VARCHAR(65535)   ENCODE lzo
	,correction VARCHAR(65535)   ENCODE lzo
	,creator_desc VARCHAR(65535)   ENCODE lzo
	,credit_amount NUMERIC(38,10)   ENCODE az64
	,curr_accounting_db VARCHAR(65535)   ENCODE lzo
	,currency_amount NUMERIC(38,10)   ENCODE az64
	,currency_code VARCHAR(65535)   ENCODE lzo
	,currency_credit_amount NUMERIC(38,10)   ENCODE az64
	,currency_debet_amount NUMERIC(38,10)   ENCODE az64
	,currency_rate NUMERIC(38,10)   ENCODE az64
	,debet_amount NUMERIC(38,10)   ENCODE az64
	,deliv_type_id VARCHAR(65535)   ENCODE lzo
	,entered_by_user_group VARCHAR(65535)   ENCODE lzo
	,entry_date VARCHAR(65535)   ENCODE lzo
	,exclude_periodical_cap VARCHAR(65535)   ENCODE lzo
	,fiscalyearperiod VARCHAR(65535)   ENCODE lzo
	,function_group VARCHAR(65535)   ENCODE lzo
	,header_correction VARCHAR(65535)   ENCODE lzo
	,internal_accounting VARCHAR(65535)   ENCODE lzo
	,internal_seq_number NUMERIC(38,10)   ENCODE az64
	,is_multi_company_voucher VARCHAR(65535)   ENCODE lzo
	,journal_id VARCHAR(65535)   ENCODE lzo
	,matching_date VARCHAR(65535)   ENCODE lzo
	,matching_no VARCHAR(65535)   ENCODE lzo
	,matching_period NUMERIC(38,10)   ENCODE az64
	,matching_year NUMERIC(38,10)   ENCODE az64
	,mpccom_accounting_id NUMERIC(38,10)   ENCODE az64
	,multi_company_id VARCHAR(65535)   ENCODE lzo
	,ncf_settlement_date VARCHAR(65535)   ENCODE lzo
	,objkey VARCHAR(65535)   ENCODE lzo
	,objversion VARCHAR(65535)   ENCODE lzo
	,old_period NUMERIC(38,10)   ENCODE az64
	,old_row_no NUMERIC(38,10)   ENCODE az64
	,optional_code VARCHAR(65535)   ENCODE lzo
	,org_parent_row NUMERIC(38,10)   ENCODE az64
	,parallel_conversion_factor NUMERIC(38,10)   ENCODE az64
	,parallel_currency_rate NUMERIC(38,10)   ENCODE az64
	,parent_row NUMERIC(38,10)   ENCODE az64
	,party_type_id VARCHAR(65535)   ENCODE lzo
	,period_allocation VARCHAR(65535)   ENCODE lzo
	,posting_combination_id NUMERIC(38,10)   ENCODE az64
	,process_code VARCHAR(65535)   ENCODE lzo
	,project_accounting_db VARCHAR(65535)   ENCODE lzo
	,project_activity_id NUMERIC(38,10)   ENCODE az64
	,quantity NUMERIC(38,10)   ENCODE az64
	,reference_number VARCHAR(65535)   ENCODE lzo
	,reference_row_no VARCHAR(65535)   ENCODE lzo
	,reference_serie VARCHAR(65535)   ENCODE lzo
	,row_group_id NUMERIC(38,10)   ENCODE az64
	,row_no NUMERIC(38,10)   ENCODE az64
	,sequence_no NUMERIC(38,10)   ENCODE az64
	,simulation_voucher VARCHAR(65535)   ENCODE lzo
	,summerized_db VARCHAR(65535)   ENCODE lzo
	,text VARCHAR(65535)   ENCODE lzo
	,third_currency_amount NUMERIC(38,10)   ENCODE az64
	,third_currency_credit_amount NUMERIC(38,10)   ENCODE az64
	,third_currency_debit_amount NUMERIC(38,10)   ENCODE az64
	,trans_code VARCHAR(65535)   ENCODE lzo
	,transfer_id VARCHAR(65535)   ENCODE lzo
	,userid VARCHAR(65535)   ENCODE lzo
	,voucher_date VARCHAR(65535)   ENCODE lzo
	,voucher_no NUMERIC(38,10)   ENCODE az64
	,voucher_no_reference NUMERIC(38,10)   ENCODE az64
	,voucher_type VARCHAR(65535)   ENCODE lzo
	,voucher_type_reference VARCHAR(65535)   ENCODE lzo
	,year_period VARCHAR(65535)   ENCODE lzo
	,year_period_key NUMERIC(38,10)   ENCODE az64
	,int_entity_type VARCHAR(65535)   ENCODE lzo
	,currency VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.ifs_pl_trans_current owner to base_admin;


-- bods.jde_na_op_trans_current definition

-- Drop table

-- DROP TABLE bods.jde_na_op_trans_current;

--DROP TABLE bods.jde_na_op_trans_current;
CREATE TABLE IF NOT EXISTS bods.jde_na_op_trans_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id VARCHAR(65535)   ENCODE lzo
	,fiscper VARCHAR(65535)   ENCODE lzo
	,fisday VARCHAR(65535)   ENCODE lzo
	,fisweek VARCHAR(65535)   ENCODE lzo
	,order_date VARCHAR(65535)   ENCODE lzo
	,sched_pick_date VARCHAR(65535)   ENCODE lzo
	,order_num VARCHAR(65535)   ENCODE lzo
	,branch_plant VARCHAR(65535)   ENCODE lzo
	,cust_payment_term VARCHAR(65535)   ENCODE lzo
	,sold_to_name VARCHAR(65535)   ENCODE lzo
	,sold_to_id NUMERIC(38,10)   ENCODE az64
	,sold_to_country VARCHAR(65535)   ENCODE lzo
	,prod_class VARCHAR(65535)   ENCODE lzo
	,prod_family VARCHAR(65535)   ENCODE lzo
	,prod_type VARCHAR(65535)   ENCODE lzo
	,prod_line VARCHAR(65535)   ENCODE lzo
	,sec_item_num VARCHAR(65535)   ENCODE lzo
	,prod_sub_type VARCHAR(65535)   ENCODE lzo
	,prod_desc VARCHAR(65535)   ENCODE lzo
	,prod_uom VARCHAR(65535)   ENCODE lzo
	,std_cost NUMERIC(38,10)   ENCODE az64
	,open_order_qty NUMERIC(38,10)   ENCODE az64
	,extended_amt NUMERIC(38,10)   ENCODE az64
	,sgm NUMERIC(38,10)   ENCODE az64
	,admin_bus_unit VARCHAR(65535)   ENCODE lzo
	,order_type VARCHAR(65535)   ENCODE lzo
	,line_num NUMERIC(38,10)   ENCODE az64
	,order_comp VARCHAR(65535)   ENCODE lzo
	,order_co VARCHAR(65535)   ENCODE lzo
	,hd_cd VARCHAR(65535)   ENCODE lzo
	,desc_1 VARCHAR(65535)   ENCODE lzo
	,desc_line_2 VARCHAR(65535)   ENCODE lzo
	,next_status VARCHAR(65535)   ENCODE lzo
	,cust_po VARCHAR(65535)   ENCODE lzo
	,price_uom VARCHAR(65535)   ENCODE lzo
	,request_date VARCHAR(65535)   ENCODE lzo
	,gl_class_code VARCHAR(65535)   ENCODE lzo
	,line_type VARCHAR(65535)   ENCODE lzo
	,addr_num NUMERIC(38,10)   ENCODE az64
	,hyperion_code VARCHAR(65535)   ENCODE lzo
	,src_sys_cd VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
	,record_create_date VARCHAR(65535)   ENCODE lzo
	,sold_to NUMERIC(38,10)   ENCODE az64
	,scheduled_pick_date VARCHAR(65535)   ENCODE lzo
	,currency_code VARCHAR(65535)   ENCODE lzo
	,unit_price NUMERIC(38,10)   ENCODE az64
	,per_unit_cost NUMERIC(38,10)   ENCODE az64
	,units_prmary_qty_ord NUMERIC(38,10)   ENCODE az64
	,unit_of_measure_primary VARCHAR(65535)   ENCODE lzo
	,unit_of_measure_input VARCHAR(65535)   ENCODE lzo
	,gl_date VARCHAR(65535)   ENCODE lzo
	,gl_date_init VARCHAR(65535)   ENCODE lzo
	,order_type_code VARCHAR(65535)   ENCODE lzo
	,cancel_date VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.jde_na_op_trans_current owner to base_admin;


-- bods.jde_na_pl_trans_current definition

-- Drop table

-- DROP TABLE bods.jde_na_pl_trans_current;

--DROP TABLE bods.jde_na_pl_trans_current;
CREATE TABLE IF NOT EXISTS bods.jde_na_pl_trans_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id VARCHAR(65535)   ENCODE lzo
	,company VARCHAR(65535)   ENCODE lzo
	,"year" VARCHAR(65535)   ENCODE lzo
	,amount NUMERIC(38,10)   ENCODE az64
	,entity VARCHAR(65535)   ENCODE lzo
	,fiscper VARCHAR(65535)   ENCODE lzo
	,period VARCHAR(65535)   ENCODE lzo
	,"account" VARCHAR(65535)   ENCODE lzo
	,account_sub VARCHAR(65535)   ENCODE lzo
	,leg_num VARCHAR(65535)   ENCODE lzo
	,hyperion VARCHAR(65535)   ENCODE lzo
	,hyperion_code VARCHAR(65535)   ENCODE lzo
	,plant VARCHAR(65535)   ENCODE lzo
	,sold_to_customer NUMERIC(38,10)   ENCODE az64
	,end_customer VARCHAR(65535)   ENCODE lzo
	,customer NUMERIC(38,10)   ENCODE az64
	,parent NUMERIC(38,10)   ENCODE az64
	,used_customer NUMERIC(38,10)   ENCODE az64
	,short_id VARCHAR(65535)   ENCODE lzo
	,product NUMERIC(38,10)   ENCODE az64
	,ship_to VARCHAR(65535)   ENCODE lzo
	,brand VARCHAR(65535)   ENCODE lzo
	,quantity NUMERIC(38,10)   ENCODE az64
	,currency_code VARCHAR(65535)   ENCODE lzo
	,transaction_date VARCHAR(65535)   ENCODE lzo
	,posting_date VARCHAR(65535)   ENCODE lzo
	,document_type VARCHAR(65535)   ENCODE lzo
	,document_id VARCHAR(65535)   ENCODE lzo
	,usd_amount NUMERIC(38,10)   ENCODE az64
	,int_functype VARCHAR(65535)   ENCODE lzo
	,product_class VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.jde_na_pl_trans_current owner to base_admin;


-- bods.lawson_mac_pl_trans_current definition

-- Drop table

-- DROP TABLE bods.lawson_mac_pl_trans_current;

--DROP TABLE bods.lawson_mac_pl_trans_current;
CREATE TABLE IF NOT EXISTS bods.lawson_mac_pl_trans_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id VARCHAR(65535)   ENCODE lzo
	,fiscper VARCHAR(65535)   ENCODE lzo
	,fyr_id VARCHAR(65535)   ENCODE lzo
	,fmth_nbr VARCHAR(65535)   ENCODE lzo
	,co_cd VARCHAR(65535)   ENCODE lzo
	,acct VARCHAR(65535)   ENCODE lzo
	,sub_acct VARCHAR(65535)   ENCODE lzo
	,acct_unit VARCHAR(65535)   ENCODE lzo
	,cust_nbr VARCHAR(65535)   ENCODE lzo
	,dist_co_cd VARCHAR(65535)   ENCODE lzo
	,dist_nbr VARCHAR(65535)   ENCODE lzo
	,lkp_co_cd VARCHAR(65535)   ENCODE lzo
	,lkp_cust_nbr VARCHAR(65535)   ENCODE lzo
	,prod_cd VARCHAR(65535)   ENCODE lzo
	,sys_cd VARCHAR(65535)   ENCODE lzo
	,sys_name VARCHAR(65535)   ENCODE lzo
	,je_nbr VARCHAR(65535)   ENCODE lzo
	,je_ln_nbr VARCHAR(65535)   ENCODE lzo
	,post_doc_ref_nbr VARCHAR(65535)   ENCODE lzo
	,post_doc_ref_ln_nbr VARCHAR(65535)   ENCODE lzo
	,src_doc_nbr VARCHAR(65535)   ENCODE lzo
	,src_doc_typ VARCHAR(65535)   ENCODE lzo
	,post_dte VARCHAR(65535)   ENCODE lzo
	,crncy_cd VARCHAR(65535)   ENCODE lzo
	,cust_mgr_cd VARCHAR(65535)   ENCODE lzo
	,cust_regn_nbr VARCHAR(65535)   ENCODE lzo
	,lvl4_prod_hier_cd VARCHAR(65535)   ENCODE lzo
	,brand_cd VARCHAR(65535)   ENCODE lzo
	,lkp_regn_nbr VARCHAR(65535)   ENCODE lzo
	,lkp_mgr_cd VARCHAR(65535)   ENCODE lzo
	,amt VARCHAR(65535)   ENCODE lzo
	,usd_amt VARCHAR(65535)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,quantity VARCHAR(65535)   ENCODE lzo
	,ins_dtm VARCHAR(65535)   ENCODE lzo
	,upd_dtm VARCHAR(65535)   ENCODE lzo
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.lawson_mac_pl_trans_current owner to base_admin;


-- bods.movex_pl_trans_current definition

-- Drop table

-- DROP TABLE bods.movex_pl_trans_current;

--DROP TABLE bods.movex_pl_trans_current;
CREATE TABLE IF NOT EXISTS bods.movex_pl_trans_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id BIGINT   ENCODE az64
	,fiscper BIGINT   ENCODE az64
	,fyr_id BIGINT   ENCODE az64
	,fmth_nbr BIGINT   ENCODE az64
	,co_cd VARCHAR(65535)   ENCODE lzo
	,acct_grp VARCHAR(65535)   ENCODE lzo
	,acct VARCHAR(65535)   ENCODE lzo
	,dept VARCHAR(65535)   ENCODE lzo
	,txn_id VARCHAR(65535)   ENCODE lzo
	,txn_dte VARCHAR(65535)   ENCODE lzo
	,crncy_cd VARCHAR(65535)   ENCODE lzo
	,amt NUMERIC(38,10)   ENCODE az64
	,usd_amt NUMERIC(38,10)   ENCODE az64
	,int_functype VARCHAR(65535)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,cust_nbr VARCHAR(65535)   ENCODE lzo
	,ins_dtm VARCHAR(65535)   ENCODE lzo
	,upd_dtm VARCHAR(65535)   ENCODE lzo
	,runid BIGINT   ENCODE az64
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.movex_pl_trans_current owner to base_admin;


-- bods.nav_assm_pl_trans_current definition

-- Drop table

-- DROP TABLE bods.nav_assm_pl_trans_current;

--DROP TABLE bods.nav_assm_pl_trans_current;
CREATE TABLE IF NOT EXISTS bods.nav_assm_pl_trans_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id VARCHAR(65535)   ENCODE lzo
	,"account" VARCHAR(65535)   ENCODE lzo
	,period VARCHAR(65535)   ENCODE lzo
	,department_exp VARCHAR(65535)   ENCODE lzo
	,expenses_type VARCHAR(65535)   ENCODE lzo
	,sold_to_customer VARCHAR(65535)   ENCODE lzo
	,sold_to_customer_name VARCHAR(65535)   ENCODE lzo
	,sold_to_customer__channel VARCHAR(65535)   ENCODE lzo
	,bill_to_customer VARCHAR(65535)   ENCODE lzo
	,product VARCHAR(65535)   ENCODE lzo
	,ship_to_customer VARCHAR(65535)   ENCODE lzo
	,qty NUMERIC(38,10)   ENCODE az64
	,currency_code VARCHAR(65535)   ENCODE lzo
	,transaction_date VARCHAR(65535)   ENCODE lzo
	,job_number VARCHAR(65535)   ENCODE lzo
	,document_type VARCHAR(65535)   ENCODE lzo
	,document_id VARCHAR(65535)   ENCODE lzo
	,amount NUMERIC(38,10)   ENCODE az64
	,fiscper VARCHAR(65535)   ENCODE lzo
	,int_functype VARCHAR(65535)   ENCODE lzo
	,product_group_code VARCHAR(65535)   ENCODE lzo
	,dimension_code_prod VARCHAR(65535)   ENCODE lzo
	,dimension_code_cust VARCHAR(65535)   ENCODE lzo
	,customer_group_code VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.nav_assm_pl_trans_current owner to base_admin;


-- bods.nav_eur_pl_trans_current definition

-- Drop table

-- DROP TABLE bods.nav_eur_pl_trans_current;

--DROP TABLE bods.nav_eur_pl_trans_current;
CREATE TABLE IF NOT EXISTS bods.nav_eur_pl_trans_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id VARCHAR(65535)   ENCODE lzo
	,fiscper VARCHAR(65535)   ENCODE lzo
	,fyr_id VARCHAR(65535)   ENCODE lzo
	,fmth_nbr VARCHAR(65535)   ENCODE lzo
	,co_cd VARCHAR(65535)   ENCODE lzo
	,acct VARCHAR(65535)   ENCODE lzo
	,doc_nbr VARCHAR(65535)   ENCODE lzo
	,txn_id VARCHAR(65535)   ENCODE lzo
	,post_dte VARCHAR(65535)   ENCODE lzo
	,crncy_cd VARCHAR(65535)   ENCODE lzo
	,amt VARCHAR(38)   ENCODE lzo
	,usd_amt VARCHAR(38)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_amt VARCHAR(38)   ENCODE lzo
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,ins_dtm VARCHAR(65535)   ENCODE lzo
	,upd_dtm VARCHAR(65535)   ENCODE lzo
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.nav_eur_pl_trans_current owner to base_admin;


-- bods.nav_storage_pl_trans_current definition

-- Drop table

-- DROP TABLE bods.nav_storage_pl_trans_current;

--DROP TABLE bods.nav_storage_pl_trans_current;
CREATE TABLE IF NOT EXISTS bods.nav_storage_pl_trans_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id VARCHAR(65535)   ENCODE lzo
	,fiscper VARCHAR(65535)   ENCODE lzo
	,fiscal_year NUMERIC(38,10)   ENCODE az64
	,fiscal_close_date VARCHAR(65535)   ENCODE lzo
	,cocode VARCHAR(65535)   ENCODE lzo
	,entry_no VARCHAR(65535)   ENCODE lzo
	,acct VARCHAR(65535)   ENCODE lzo
	,gl_account VARCHAR(65535)   ENCODE lzo
	,origin_account VARCHAR(65535)   ENCODE lzo
	,posting_date VARCHAR(65535)   ENCODE lzo
	,department VARCHAR(65535)   ENCODE lzo
	,expense_type VARCHAR(65535)   ENCODE lzo
	,prime VARCHAR(65535)   ENCODE lzo
	,affiliate VARCHAR(65535)   ENCODE lzo
	,"region" VARCHAR(65535)   ENCODE lzo
	,document_type VARCHAR(65535)   ENCODE lzo
	,document_no VARCHAR(65535)   ENCODE lzo
	,transaction_no VARCHAR(65535)   ENCODE lzo
	,dep_dim_val_code VARCHAR(65535)   ENCODE lzo
	,exptype_dim_val_code VARCHAR(65535)   ENCODE lzo
	,aff_dim_val_code VARCHAR(65535)   ENCODE lzo
	,reg_dim_val_code VARCHAR(65535)   ENCODE lzo
	,bu_dim_val_code VARCHAR(65535)   ENCODE lzo
	,hfmcode1 VARCHAR(65535)   ENCODE lzo
	,incomebalance VARCHAR(65535)   ENCODE lzo
	,dep_consolidation_code VARCHAR(65535)   ENCODE lzo
	,dep_consolidation_code_2 VARCHAR(65535)   ENCODE lzo
	,exp_consolidation_code VARCHAR(65535)   ENCODE lzo
	,exp_consolidation_code_2 VARCHAR(65535)   ENCODE lzo
	,reg_consolidation_code VARCHAR(65535)   ENCODE lzo
	,reg_consolidation_code_2 VARCHAR(65535)   ENCODE lzo
	,currency_code VARCHAR(65535)   ENCODE lzo
	,amount NUMERIC(38,10)   ENCODE az64
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,company_code VARCHAR(65535)   ENCODE lzo
	,ins_dtm VARCHAR(65535)   ENCODE lzo
	,upd_dtm VARCHAR(65535)   ENCODE lzo
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.nav_storage_pl_trans_current owner to base_admin;


-- bods.navision_actuals_trans_current definition

-- Drop table

-- DROP TABLE bods.navision_actuals_trans_current;

--DROP TABLE bods.navision_actuals_trans_current;
CREATE TABLE IF NOT EXISTS bods.navision_actuals_trans_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id NUMERIC(38,10)   ENCODE az64
	,rectype VARCHAR(65535)   ENCODE lzo
	,"year" VARCHAR(65535)   ENCODE lzo
	,period VARCHAR(65535)   ENCODE lzo
	,accttype VARCHAR(65535)   ENCODE lzo
	,entity VARCHAR(65535)   ENCODE lzo
	,acct VARCHAR(65535)   ENCODE lzo
	,func VARCHAR(65535)   ENCODE lzo
	,currkey VARCHAR(65535)   ENCODE lzo
	,amt NUMERIC(38,10)   ENCODE az64
	,int_functype VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,posting_date VARCHAR(65535)   ENCODE lzo
	,customer VARCHAR(65535)   ENCODE lzo
	,product VARCHAR(65535)   ENCODE lzo
	,ship_to VARCHAR(65535)   ENCODE lzo
	,brand VARCHAR(65535)   ENCODE lzo
	,usd_amount VARCHAR(65535)   ENCODE lzo
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
	,period_partition VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.navision_actuals_trans_current owner to base_admin;


-- bods.navision_actuals_trans_current_bkp definition

-- Drop table

-- DROP TABLE bods.navision_actuals_trans_current_bkp;

--DROP TABLE bods.navision_actuals_trans_current_bkp;
CREATE TABLE IF NOT EXISTS bods.navision_actuals_trans_current_bkp
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id NUMERIC(38,10)   ENCODE az64
	,rectype VARCHAR(65535)   ENCODE lzo
	,"year" VARCHAR(65535)   ENCODE lzo
	,period VARCHAR(65535)   ENCODE lzo
	,accttype VARCHAR(65535)   ENCODE lzo
	,entity VARCHAR(65535)   ENCODE lzo
	,acct VARCHAR(65535)   ENCODE lzo
	,func VARCHAR(65535)   ENCODE lzo
	,currkey VARCHAR(65535)   ENCODE lzo
	,amt NUMERIC(38,10)   ENCODE az64
	,int_functype VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,posting_date VARCHAR(65535)   ENCODE lzo
	,customer VARCHAR(65535)   ENCODE lzo
	,product VARCHAR(65535)   ENCODE lzo
	,ship_to VARCHAR(65535)   ENCODE lzo
	,brand VARCHAR(65535)   ENCODE lzo
	,usd_amount VARCHAR(65535)   ENCODE lzo
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.navision_actuals_trans_current_bkp owner to base_admin;


-- bods.nelson_asmp_pl_trans_current definition

-- Drop table

-- DROP TABLE bods.nelson_asmp_pl_trans_current;

--DROP TABLE bods.nelson_asmp_pl_trans_current;
CREATE TABLE IF NOT EXISTS bods.nelson_asmp_pl_trans_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id BIGINT   ENCODE az64
	,fiscper VARCHAR(65535)   ENCODE lzo
	,company VARCHAR(65535)   ENCODE lzo
	,fiscal_year VARCHAR(65535)   ENCODE lzo
	,fiscal_period VARCHAR(65535)   ENCODE lzo
	,posted_date VARCHAR(65535)   ENCODE lzo
	,je_date VARCHAR(65535)   ENCODE lzo
	,posted_by VARCHAR(65535)   ENCODE lzo
	,posted VARCHAR(65535)   ENCODE lzo
	,journal_num VARCHAR(65535)   ENCODE lzo
	,journal_line VARCHAR(65535)   ENCODE lzo
	,description VARCHAR(65535)   ENCODE lzo
	,debit_amount NUMERIC(38,10)   ENCODE az64
	,credit_amount NUMERIC(38,10)   ENCODE az64
	,ar_invoice_num VARCHAR(65535)   ENCODE lzo
	,gl_account VARCHAR(65535)   ENCODE lzo
	,account_desc VARCHAR(65535)   ENCODE lzo
	,groupid VARCHAR(65535)   ENCODE lzo
	,source_module VARCHAR(65535)   ENCODE lzo
	,journal_code VARCHAR(65535)   ENCODE lzo
	,statistical VARCHAR(65535)   ENCODE lzo
	,rundate_4 VARCHAR(65535)   ENCODE lzo
	,bar_geo VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,runid BIGINT   ENCODE az64
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.nelson_asmp_pl_trans_current owner to base_admin;


-- bods.nelson_lx_pl_trans_current definition

-- Drop table

-- DROP TABLE bods.nelson_lx_pl_trans_current;

--DROP TABLE bods.nelson_lx_pl_trans_current;
CREATE TABLE IF NOT EXISTS bods.nelson_lx_pl_trans_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id VARCHAR(65535)   ENCODE lzo
	,fiscper VARCHAR(65535)   ENCODE lzo
	,fiscal_period VARCHAR(65535)   ENCODE lzo
	,nelson_company VARCHAR(65535)   ENCODE lzo
	,fiscal_year VARCHAR(65535)   ENCODE lzo
	,nelson_profit_cntr_depart VARCHAR(65535)   ENCODE lzo
	,nelson_acct VARCHAR(65535)   ENCODE lzo
	,nelson_sub_account VARCHAR(65535)   ENCODE lzo
	,nelson_project VARCHAR(65535)   ENCODE lzo
	,transaction_date VARCHAR(65535)   ENCODE lzo
	,gl_posting_date VARCHAR(65535)   ENCODE lzo
	,book_curr VARCHAR(65535)   ENCODE lzo
	,book_amt NUMERIC(38,10)   ENCODE az64
	,exchange_rate NUMERIC(38,10)   ENCODE az64
	,trans_curr VARCHAR(65535)   ENCODE lzo
	,trans_amt NUMERIC(38,10)   ENCODE az64
	,journal_type VARCHAR(65535)   ENCODE lzo
	,journal_desc VARCHAR(65535)   ENCODE lzo
	,nelson_journal_num VARCHAR(65535)   ENCODE lzo
	,journal_line VARCHAR(65535)   ENCODE lzo
	,delivery_num VARCHAR(65535)   ENCODE lzo
	,delivery_line VARCHAR(65535)   ENCODE lzo
	,invoice_prefix VARCHAR(65535)   ENCODE lzo
	,invoice_num VARCHAR(65535)   ENCODE lzo
	,invoice_type VARCHAR(65535)   ENCODE lzo
	,invoice_line VARCHAR(65535)   ENCODE lzo
	,invoice_company VARCHAR(65535)   ENCODE lzo
	,invoice_date VARCHAR(65535)   ENCODE lzo
	,invoice_line_type VARCHAR(65535)   ENCODE lzo
	,order_num VARCHAR(65535)   ENCODE lzo
	,order_line VARCHAR(65535)   ENCODE lzo
	,market_code VARCHAR(65535)   ENCODE lzo
	,market_code_desc VARCHAR(65535)   ENCODE lzo
	,order_type VARCHAR(65535)   ENCODE lzo
	,order_source VARCHAR(65535)   ENCODE lzo
	,sold_to_cust_no VARCHAR(65535)   ENCODE lzo
	,sold_to_name VARCHAR(65535)   ENCODE lzo
	,sold_to_state VARCHAR(65535)   ENCODE lzo
	,sold_to_country VARCHAR(65535)   ENCODE lzo
	,ship_to_num VARCHAR(65535)   ENCODE lzo
	,ship_to_state VARCHAR(65535)   ENCODE lzo
	,ship_to_country VARCHAR(65535)   ENCODE lzo
	,primary_sales_rep VARCHAR(65535)   ENCODE lzo
	,corporate_cust_no VARCHAR(65535)   ENCODE lzo
	,global_partner VARCHAR(65535)   ENCODE lzo
	,warehouse VARCHAR(65535)   ENCODE lzo
	,transaction_terms_code VARCHAR(65535)   ENCODE lzo
	,transaction_terms_desc VARCHAR(65535)   ENCODE lzo
	,cust_terms_code VARCHAR(65535)   ENCODE lzo
	,credit_limit NUMERIC(38,10)   ENCODE az64
	,flash_material_type VARCHAR(65535)   ENCODE lzo
	,item_num VARCHAR(65535)   ENCODE lzo
	,item_class VARCHAR(65535)   ENCODE lzo
	,item_class_desc VARCHAR(65535)   ENCODE lzo
	,product_hierarchy_1 VARCHAR(65535)   ENCODE lzo
	,product_hierarchy_1_desc VARCHAR(65535)   ENCODE lzo
	,product_hierarchy_2 VARCHAR(65535)   ENCODE lzo
	,product_hierarchy_2_desc VARCHAR(65535)   ENCODE lzo
	,product_hierarchy_3 VARCHAR(65535)   ENCODE lzo
	,product_hierarchy_3_desc VARCHAR(65535)   ENCODE lzo
	,product_hierarchy_4 VARCHAR(65535)   ENCODE lzo
	,product_hierarchy_4_desc VARCHAR(65535)   ENCODE lzo
	,product_hierarchy_5 VARCHAR(65535)   ENCODE lzo
	,product_hierarchy_5_desc VARCHAR(65535)   ENCODE lzo
	,cust_item_num VARCHAR(65535)   ENCODE lzo
	,selling_uom VARCHAR(65535)   ENCODE lzo
	,qty_shipped NUMERIC(38,10)   ENCODE az64
	,std_cost_per_unit NUMERIC(38,10)   ENCODE az64
	,shipped_amt NUMERIC(38,10)   ENCODE az64
	,std_cost_amt NUMERIC(38,10)   ENCODE az64
	,margin_amt NUMERIC(38,10)   ENCODE az64
	,margin NUMERIC(38,10)   ENCODE az64
	,ibp_customer_id VARCHAR(65535)   ENCODE lzo
	,ibp_product_hierarchy_3 VARCHAR(65535)   ENCODE lzo
	,ibp_product_hierarchy_4 VARCHAR(65535)   ENCODE lzo
	,ibp_product_hierarchy_5 VARCHAR(65535)   ENCODE lzo
	,ibp_product_hierarchy_6 VARCHAR(65535)   ENCODE lzo
	,ledger VARCHAR(65535)   ENCODE lzo
	,book VARCHAR(65535)   ENCODE lzo
	,journal_line_seq VARCHAR(65535)   ENCODE lzo
	,bar_geo VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.nelson_lx_pl_trans_current owner to base_admin;


-- bods.nelson_lx_pl_trans_current_bkp definition

-- Drop table

-- DROP TABLE bods.nelson_lx_pl_trans_current_bkp;

--DROP TABLE bods.nelson_lx_pl_trans_current_bkp;
CREATE TABLE IF NOT EXISTS bods.nelson_lx_pl_trans_current_bkp
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id VARCHAR(65535)   ENCODE lzo
	,fiscper VARCHAR(65535)   ENCODE lzo
	,fiscal_period VARCHAR(65535)   ENCODE lzo
	,nelson_company VARCHAR(65535)   ENCODE lzo
	,fiscal_year VARCHAR(65535)   ENCODE lzo
	,nelson_profit_cntr_depart VARCHAR(65535)   ENCODE lzo
	,nelson_acct VARCHAR(65535)   ENCODE lzo
	,nelson_sub_account VARCHAR(65535)   ENCODE lzo
	,nelson_project VARCHAR(65535)   ENCODE lzo
	,transaction_date VARCHAR(65535)   ENCODE lzo
	,gl_posting_date VARCHAR(65535)   ENCODE lzo
	,book_curr VARCHAR(65535)   ENCODE lzo
	,book_amt NUMERIC(38,10)   ENCODE az64
	,exchange_rate NUMERIC(38,10)   ENCODE az64
	,trans_curr VARCHAR(65535)   ENCODE lzo
	,trans_amt NUMERIC(38,10)   ENCODE az64
	,journal_type VARCHAR(65535)   ENCODE lzo
	,journal_desc VARCHAR(65535)   ENCODE lzo
	,nelson_journal_num VARCHAR(65535)   ENCODE lzo
	,journal_line VARCHAR(65535)   ENCODE lzo
	,delivery_num VARCHAR(65535)   ENCODE lzo
	,delivery_line VARCHAR(65535)   ENCODE lzo
	,invoice_prefix VARCHAR(65535)   ENCODE lzo
	,invoice_num VARCHAR(65535)   ENCODE lzo
	,invoice_type VARCHAR(65535)   ENCODE lzo
	,invoice_line VARCHAR(65535)   ENCODE lzo
	,invoice_company VARCHAR(65535)   ENCODE lzo
	,invoice_date VARCHAR(65535)   ENCODE lzo
	,invoice_line_type VARCHAR(65535)   ENCODE lzo
	,order_num VARCHAR(65535)   ENCODE lzo
	,order_line VARCHAR(65535)   ENCODE lzo
	,market_code VARCHAR(65535)   ENCODE lzo
	,market_code_desc VARCHAR(65535)   ENCODE lzo
	,order_type VARCHAR(65535)   ENCODE lzo
	,order_source VARCHAR(65535)   ENCODE lzo
	,sold_to_cust_no VARCHAR(65535)   ENCODE lzo
	,sold_to_name VARCHAR(65535)   ENCODE lzo
	,sold_to_state VARCHAR(65535)   ENCODE lzo
	,sold_to_country VARCHAR(65535)   ENCODE lzo
	,ship_to_num VARCHAR(65535)   ENCODE lzo
	,ship_to_state VARCHAR(65535)   ENCODE lzo
	,ship_to_country VARCHAR(65535)   ENCODE lzo
	,primary_sales_rep VARCHAR(65535)   ENCODE lzo
	,corporate_cust_no VARCHAR(65535)   ENCODE lzo
	,global_partner VARCHAR(65535)   ENCODE lzo
	,warehouse VARCHAR(65535)   ENCODE lzo
	,transaction_terms_code VARCHAR(65535)   ENCODE lzo
	,transaction_terms_desc VARCHAR(65535)   ENCODE lzo
	,cust_terms_code VARCHAR(65535)   ENCODE lzo
	,credit_limit NUMERIC(38,10)   ENCODE az64
	,flash_material_type VARCHAR(65535)   ENCODE lzo
	,item_num VARCHAR(65535)   ENCODE lzo
	,item_class VARCHAR(65535)   ENCODE lzo
	,item_class_desc VARCHAR(65535)   ENCODE lzo
	,product_hierarchy_1 VARCHAR(65535)   ENCODE lzo
	,product_hierarchy_1_desc VARCHAR(65535)   ENCODE lzo
	,product_hierarchy_2 VARCHAR(65535)   ENCODE lzo
	,product_hierarchy_2_desc VARCHAR(65535)   ENCODE lzo
	,product_hierarchy_3 VARCHAR(65535)   ENCODE lzo
	,product_hierarchy_3_desc VARCHAR(65535)   ENCODE lzo
	,product_hierarchy_4 VARCHAR(65535)   ENCODE lzo
	,product_hierarchy_4_desc VARCHAR(65535)   ENCODE lzo
	,product_hierarchy_5 VARCHAR(65535)   ENCODE lzo
	,product_hierarchy_5_desc VARCHAR(65535)   ENCODE lzo
	,cust_item_num VARCHAR(65535)   ENCODE lzo
	,selling_uom VARCHAR(65535)   ENCODE lzo
	,qty_shipped NUMERIC(38,10)   ENCODE az64
	,std_cost_per_unit NUMERIC(38,10)   ENCODE az64
	,shipped_amt NUMERIC(38,10)   ENCODE az64
	,std_cost_amt NUMERIC(38,10)   ENCODE az64
	,margin_amt NUMERIC(38,10)   ENCODE az64
	,margin NUMERIC(38,10)   ENCODE az64
	,ibp_customer_id VARCHAR(65535)   ENCODE lzo
	,ibp_product_hierarchy_3 VARCHAR(65535)   ENCODE lzo
	,ibp_product_hierarchy_4 VARCHAR(65535)   ENCODE lzo
	,ibp_product_hierarchy_5 VARCHAR(65535)   ENCODE lzo
	,ibp_product_hierarchy_6 VARCHAR(65535)   ENCODE lzo
	,ledger VARCHAR(65535)   ENCODE lzo
	,book VARCHAR(65535)   ENCODE lzo
	,journal_line_seq VARCHAR(65535)   ENCODE lzo
	,bar_geo VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE EVEN
;
ALTER TABLE bods.nelson_lx_pl_trans_current_bkp owner to base_admin;


-- bods.orch_bgi_pl_trans_current definition

-- Drop table

-- DROP TABLE bods.orch_bgi_pl_trans_current;

--DROP TABLE bods.orch_bgi_pl_trans_current;
CREATE TABLE IF NOT EXISTS bods.orch_bgi_pl_trans_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id VARCHAR(65535)   ENCODE lzo
	,fiscper VARCHAR(65535)   ENCODE lzo
	,fyr_id VARCHAR(65535)   ENCODE lzo
	,fmth_nbr VARCHAR(65535)   ENCODE lzo
	,co_cd VARCHAR(65535)   ENCODE lzo
	,acct VARCHAR(65535)   ENCODE lzo
	,icp_cd VARCHAR(65535)   ENCODE lzo
	,txn_id VARCHAR(65535)   ENCODE lzo
	,post_dte VARCHAR(65535)   ENCODE lzo
	,crncy_cd VARCHAR(65535)   ENCODE lzo
	,amt VARCHAR(65535)   ENCODE lzo
	,usd_amt VARCHAR(65535)   ENCODE lzo
	,int_functype VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_amt VARCHAR(65535)   ENCODE lzo
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.orch_bgi_pl_trans_current owner to base_admin;


-- bods.orch_ppe_pl_trans_current definition

-- Drop table

-- DROP TABLE bods.orch_ppe_pl_trans_current;

--DROP TABLE bods.orch_ppe_pl_trans_current;
CREATE TABLE IF NOT EXISTS bods.orch_ppe_pl_trans_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id BIGINT   ENCODE az64
	,fiscper BIGINT   ENCODE az64
	,fyr_id BIGINT   ENCODE az64
	,fmth_nbr BIGINT   ENCODE az64
	,co_key VARCHAR(65535)   ENCODE lzo
	,co_cd VARCHAR(65535)   ENCODE lzo
	,acct VARCHAR(65535)   ENCODE lzo
	,icp_cd VARCHAR(65535)   ENCODE lzo
	,txn_id VARCHAR(65535)   ENCODE lzo
	,post_dte VARCHAR(65535)   ENCODE lzo
	,crncy_cd VARCHAR(65535)   ENCODE lzo
	,amt NUMERIC(38,10)   ENCODE az64
	,usd_amt NUMERIC(38,10)   ENCODE az64
	,int_functype VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype BIGINT   ENCODE az64
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,runid BIGINT   ENCODE az64
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.orch_ppe_pl_trans_current owner to base_admin;


-- bods.orcl_lista_pl_trans_current definition

-- Drop table

-- DROP TABLE bods.orcl_lista_pl_trans_current;

--DROP TABLE bods.orcl_lista_pl_trans_current;
CREATE TABLE IF NOT EXISTS bods.orcl_lista_pl_trans_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id VARCHAR(65535)   ENCODE lzo
	,fiscper VARCHAR(65535)   ENCODE lzo
	,fyr_id VARCHAR(65535)   ENCODE lzo
	,fmth_nbr VARCHAR(65535)   ENCODE lzo
	,co_cd VARCHAR(65535)   ENCODE lzo
	,acct VARCHAR(65535)   ENCODE lzo
	,cost_cntr VARCHAR(65535)   ENCODE lzo
	,batch VARCHAR(65535)   ENCODE lzo
	,post_dte VARCHAR(65535)   ENCODE lzo
	,crncy_cd VARCHAR(65535)   ENCODE lzo
	,amt VARCHAR(38)   ENCODE lzo
	,usd_amt VARCHAR(38)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_amt VARCHAR(38)   ENCODE lzo
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.orcl_lista_pl_trans_current owner to base_admin;


-- bods.p02_pl_trans_current definition

-- Drop table

-- DROP TABLE bods.p02_pl_trans_current;

--DROP TABLE bods.p02_pl_trans_current;
CREATE TABLE IF NOT EXISTS bods.p02_pl_trans_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id VARCHAR(65535)   ENCODE lzo
	,fyr_id VARCHAR(65535)   ENCODE lzo
	,crncy_cd VARCHAR(65535)   ENCODE lzo
	,fmth_nbr VARCHAR(65535)   ENCODE lzo
	,docct VARCHAR(65535)   ENCODE lzo
	,docnr VARCHAR(65535)   ENCODE lzo
	,docln VARCHAR(65535)   ENCODE lzo
	,co_cd VARCHAR(65535)   ENCODE lzo
	,profit_cntr VARCHAR(65535)   ENCODE lzo
	,func_area VARCHAR(65535)   ENCODE lzo
	,ctrl_area VARCHAR(65535)   ENCODE lzo
	,acct VARCHAR(65535)   ENCODE lzo
	,cpudt VARCHAR(65535)   ENCODE lzo
	,cputm VARCHAR(65535)   ENCODE lzo
	,sgtxt VARCHAR(65535)   ENCODE lzo
	,refdocnr VARCHAR(65535)   ENCODE lzo
	,refryear VARCHAR(65535)   ENCODE lzo
	,refdocln VARCHAR(65535)   ENCODE lzo
	,refdocct VARCHAR(65535)   ENCODE lzo
	,plant VARCHAR(65535)   ENCODE lzo
	,bus_area VARCHAR(65535)   ENCODE lzo
	,cost_cntr VARCHAR(65535)   ENCODE lzo
	,material VARCHAR(65535)   ENCODE lzo
	,shipto_cust_nbr VARCHAR(65535)   ENCODE lzo
	,vendor_id VARCHAR(65535)   ENCODE lzo
	,doctype VARCHAR(65535)   ENCODE lzo
	,quanunit VARCHAR(65535)   ENCODE lzo
	,fiscper VARCHAR(65535)   ENCODE lzo
	,currunit VARCHAR(65535)   ENCODE lzo
	,amt NUMERIC(38,10)   ENCODE az64
	,quantity NUMERIC(38,10)   ENCODE az64
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,rvers VARCHAR(65535)   ENCODE lzo
	,rhoart VARCHAR(65535)   ENCODE lzo
	,hrkft VARCHAR(65535)   ENCODE lzo
	,rassc VARCHAR(65535)   ENCODE lzo
	,eprctr VARCHAR(65535)   ENCODE lzo
	,activ VARCHAR(65535)   ENCODE lzo
	,afabe VARCHAR(65535)   ENCODE lzo
	,oclnt VARCHAR(65535)   ENCODE lzo
	,sbukrs VARCHAR(65535)   ENCODE lzo
	,sprctr VARCHAR(65535)   ENCODE lzo
	,shoart VARCHAR(65535)   ENCODE lzo
	,sfarea VARCHAR(65535)   ENCODE lzo
	,usnam VARCHAR(65535)   ENCODE lzo
	,autom VARCHAR(65535)   ENCODE lzo
	,docty VARCHAR(65535)   ENCODE lzo
	,bldat VARCHAR(65535)   ENCODE lzo
	,budat VARCHAR(65535)   ENCODE lzo
	,wsdat VARCHAR(65535)   ENCODE lzo
	,refactiv VARCHAR(65535)   ENCODE lzo
	,awtyp VARCHAR(65535)   ENCODE lzo
	,aworg VARCHAR(65535)   ENCODE lzo
	,lstar VARCHAR(65535)   ENCODE lzo
	,aufnr VARCHAR(65535)   ENCODE lzo
	,aufpl VARCHAR(65535)   ENCODE lzo
	,anln1 VARCHAR(65535)   ENCODE lzo
	,anln2 VARCHAR(65535)   ENCODE lzo
	,bwkey VARCHAR(65535)   ENCODE lzo
	,bwtar VARCHAR(65535)   ENCODE lzo
	,anbwa VARCHAR(65535)   ENCODE lzo
	,rmvct VARCHAR(65535)   ENCODE lzo
	,ebeln VARCHAR(65535)   ENCODE lzo
	,ebelp VARCHAR(65535)   ENCODE lzo
	,kstrg VARCHAR(65535)   ENCODE lzo
	,erkrs VARCHAR(65535)   ENCODE lzo
	,paobjnr VARCHAR(65535)   ENCODE lzo
	,pasubnr VARCHAR(65535)   ENCODE lzo
	,ps_psp_pnr VARCHAR(65535)   ENCODE lzo
	,kdauf VARCHAR(65535)   ENCODE lzo
	,kdpos VARCHAR(65535)   ENCODE lzo
	,fkart VARCHAR(65535)   ENCODE lzo
	,vkorg VARCHAR(65535)   ENCODE lzo
	,vtweg VARCHAR(65535)   ENCODE lzo
	,aubel VARCHAR(65535)   ENCODE lzo
	,aupos VARCHAR(65535)   ENCODE lzo
	,spart VARCHAR(65535)   ENCODE lzo
	,vbeln VARCHAR(65535)   ENCODE lzo
	,posnr VARCHAR(65535)   ENCODE lzo
	,vkgrp VARCHAR(65535)   ENCODE lzo
	,vkbur VARCHAR(65535)   ENCODE lzo
	,vbund VARCHAR(65535)   ENCODE lzo
	,logsys VARCHAR(65535)   ENCODE lzo
	,alebn VARCHAR(65535)   ENCODE lzo
	,awsys VARCHAR(65535)   ENCODE lzo
	,versa VARCHAR(65535)   ENCODE lzo
	,stflg VARCHAR(65535)   ENCODE lzo
	,stokz VARCHAR(65535)   ENCODE lzo
	,rep_matnr VARCHAR(65535)   ENCODE lzo
	,co_prznr VARCHAR(65535)   ENCODE lzo
	,imkey VARCHAR(65535)   ENCODE lzo
	,dabrz VARCHAR(65535)   ENCODE lzo
	,valut VARCHAR(65535)   ENCODE lzo
	,rscope VARCHAR(65535)   ENCODE lzo
	,awref_rev VARCHAR(65535)   ENCODE lzo
	,aworg_rev VARCHAR(65535)   ENCODE lzo
	,bwart VARCHAR(65535)   ENCODE lzo
	,timestmp VARCHAR(65535)   ENCODE lzo
	,valuetype VARCHAR(65535)   ENCODE lzo
	,curtype VARCHAR(65535)   ENCODE lzo
	,fiscvar VARCHAR(65535)   ENCODE lzo
	,chartaccts VARCHAR(65535)   ENCODE lzo
	,upmod VARCHAR(65535)   ENCODE lzo
	,valutyp VARCHAR(65535)   ENCODE lzo
	,debit NUMERIC(38,10)   ENCODE az64
	,credit NUMERIC(38,10)   ENCODE az64
	,ps_posid VARCHAR(65535)   ENCODE lzo
	,zzacount VARCHAR(65535)   ENCODE lzo
	,zzsalesre VARCHAR(65535)   ENCODE lzo
	,zzobj_id VARCHAR(65535)   ENCODE lzo
	,zzibase VARCHAR(65535)   ENCODE lzo
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.p02_pl_trans_current owner to base_admin;


-- bods.p02_pl_trans_current_temp definition

-- Drop table

-- DROP TABLE bods.p02_pl_trans_current_temp;

--DROP TABLE bods.p02_pl_trans_current_temp;
CREATE TABLE IF NOT EXISTS bods.p02_pl_trans_current_temp
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id VARCHAR(65535)   ENCODE lzo
	,fyr_id NUMERIC(38,10)   ENCODE az64
	,crncy_cd VARCHAR(65535)   ENCODE lzo
	,fmth_nbr NUMERIC(38,10)   ENCODE az64
	,docct VARCHAR(65535)   ENCODE lzo
	,docnr VARCHAR(65535)   ENCODE lzo
	,docln VARCHAR(65535)   ENCODE lzo
	,co_cd VARCHAR(65535)   ENCODE lzo
	,profit_cntr VARCHAR(65535)   ENCODE lzo
	,func_area VARCHAR(65535)   ENCODE lzo
	,ctrl_area VARCHAR(65535)   ENCODE lzo
	,acct VARCHAR(65535)   ENCODE lzo
	,cpudt VARCHAR(65535)   ENCODE lzo
	,cputm VARCHAR(65535)   ENCODE lzo
	,sgtxt VARCHAR(65535)   ENCODE lzo
	,refdocnr VARCHAR(65535)   ENCODE lzo
	,refryear NUMERIC(38,10)   ENCODE az64
	,refdocln NUMERIC(38,10)   ENCODE az64
	,refdocct VARCHAR(65535)   ENCODE lzo
	,plant VARCHAR(65535)   ENCODE lzo
	,bus_area VARCHAR(65535)   ENCODE lzo
	,cost_cntr VARCHAR(65535)   ENCODE lzo
	,material VARCHAR(65535)   ENCODE lzo
	,shipto_cust_nbr VARCHAR(65535)   ENCODE lzo
	,vendor_id VARCHAR(65535)   ENCODE lzo
	,doctype VARCHAR(65535)   ENCODE lzo
	,quanunit VARCHAR(65535)   ENCODE lzo
	,fiscper NUMERIC(38,10)   ENCODE az64
	,currunit VARCHAR(65535)   ENCODE lzo
	,amt NUMERIC(38,10)   ENCODE az64
	,quantity NUMERIC(38,10)   ENCODE az64
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,rvers VARCHAR(65535)   ENCODE lzo
	,rhoart NUMERIC(38,10)   ENCODE az64
	,hrkft VARCHAR(65535)   ENCODE lzo
	,rassc VARCHAR(65535)   ENCODE lzo
	,eprctr VARCHAR(65535)   ENCODE lzo
	,activ VARCHAR(65535)   ENCODE lzo
	,afabe NUMERIC(38,10)   ENCODE az64
	,oclnt NUMERIC(38,10)   ENCODE az64
	,sbukrs VARCHAR(65535)   ENCODE lzo
	,sprctr VARCHAR(65535)   ENCODE lzo
	,shoart NUMERIC(38,10)   ENCODE az64
	,sfarea VARCHAR(65535)   ENCODE lzo
	,usnam VARCHAR(65535)   ENCODE lzo
	,autom VARCHAR(65535)   ENCODE lzo
	,docty VARCHAR(65535)   ENCODE lzo
	,bldat VARCHAR(65535)   ENCODE lzo
	,budat VARCHAR(65535)   ENCODE lzo
	,wsdat VARCHAR(65535)   ENCODE lzo
	,refactiv VARCHAR(65535)   ENCODE lzo
	,awtyp VARCHAR(65535)   ENCODE lzo
	,aworg VARCHAR(65535)   ENCODE lzo
	,lstar VARCHAR(65535)   ENCODE lzo
	,aufnr VARCHAR(65535)   ENCODE lzo
	,aufpl NUMERIC(38,10)   ENCODE az64
	,anln1 VARCHAR(65535)   ENCODE lzo
	,anln2 VARCHAR(65535)   ENCODE lzo
	,bwkey VARCHAR(65535)   ENCODE lzo
	,bwtar VARCHAR(65535)   ENCODE lzo
	,anbwa VARCHAR(65535)   ENCODE lzo
	,rmvct VARCHAR(65535)   ENCODE lzo
	,ebeln VARCHAR(65535)   ENCODE lzo
	,ebelp NUMERIC(38,10)   ENCODE az64
	,kstrg VARCHAR(65535)   ENCODE lzo
	,erkrs VARCHAR(65535)   ENCODE lzo
	,paobjnr NUMERIC(38,10)   ENCODE az64
	,pasubnr NUMERIC(38,10)   ENCODE az64
	,ps_psp_pnr NUMERIC(38,10)   ENCODE az64
	,kdauf VARCHAR(65535)   ENCODE lzo
	,kdpos NUMERIC(38,10)   ENCODE az64
	,fkart VARCHAR(65535)   ENCODE lzo
	,vkorg VARCHAR(65535)   ENCODE lzo
	,vtweg VARCHAR(65535)   ENCODE lzo
	,aubel VARCHAR(65535)   ENCODE lzo
	,aupos NUMERIC(38,10)   ENCODE az64
	,spart VARCHAR(65535)   ENCODE lzo
	,vbeln VARCHAR(65535)   ENCODE lzo
	,posnr NUMERIC(38,10)   ENCODE az64
	,vkgrp VARCHAR(65535)   ENCODE lzo
	,vkbur VARCHAR(65535)   ENCODE lzo
	,vbund VARCHAR(65535)   ENCODE lzo
	,logsys VARCHAR(65535)   ENCODE lzo
	,alebn VARCHAR(65535)   ENCODE lzo
	,awsys VARCHAR(65535)   ENCODE lzo
	,versa VARCHAR(65535)   ENCODE lzo
	,stflg VARCHAR(65535)   ENCODE lzo
	,stokz VARCHAR(65535)   ENCODE lzo
	,rep_matnr VARCHAR(65535)   ENCODE lzo
	,co_prznr VARCHAR(65535)   ENCODE lzo
	,imkey VARCHAR(65535)   ENCODE lzo
	,dabrz VARCHAR(65535)   ENCODE lzo
	,valut VARCHAR(65535)   ENCODE lzo
	,rscope VARCHAR(65535)   ENCODE lzo
	,awref_rev VARCHAR(65535)   ENCODE lzo
	,aworg_rev VARCHAR(65535)   ENCODE lzo
	,bwart VARCHAR(65535)   ENCODE lzo
	,timestmp NUMERIC(38,10)   ENCODE az64
	,valuetype NUMERIC(38,10)   ENCODE az64
	,curtype VARCHAR(65535)   ENCODE lzo
	,fiscvar VARCHAR(65535)   ENCODE lzo
	,chartaccts VARCHAR(65535)   ENCODE lzo
	,upmod VARCHAR(65535)   ENCODE lzo
	,valutyp NUMERIC(38,10)   ENCODE az64
	,debit NUMERIC(38,10)   ENCODE az64
	,credit NUMERIC(38,10)   ENCODE az64
	,ps_posid VARCHAR(65535)   ENCODE lzo
	,zzacount VARCHAR(65535)   ENCODE lzo
	,zzsalesre NUMERIC(38,10)   ENCODE az64
	,zzobj_id VARCHAR(65535)   ENCODE lzo
	,zzibase VARCHAR(65535)   ENCODE lzo
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE EVEN
;
ALTER TABLE bods.p02_pl_trans_current_temp owner to base_admin;


-- bods.p10_0ec_pca_3_trans_current definition

-- Drop table

-- DROP TABLE bods.p10_0ec_pca_3_trans_current;

--DROP TABLE bods.p10_0ec_pca_3_trans_current;
CREATE TABLE IF NOT EXISTS bods.p10_0ec_pca_3_trans_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id BIGINT   ENCODE az64
	,fiscper BIGINT   ENCODE az64
	,fyr_id BIGINT   ENCODE az64
	,fmth_nbr BIGINT   ENCODE az64
	,docct VARCHAR(65535)   ENCODE lzo
	,docnr VARCHAR(65535)   ENCODE lzo
	,docln VARCHAR(65535)   ENCODE lzo
	,co_cd VARCHAR(65535)   ENCODE lzo
	,bus_area VARCHAR(65535)   ENCODE lzo
	,func_area VARCHAR(65535)   ENCODE lzo
	,dept VARCHAR(65535)   ENCODE lzo
	,func_area_lkp VARCHAR(65535)   ENCODE lzo
	,ctrl_area VARCHAR(65535)   ENCODE lzo
	,cost_cntr VARCHAR(65535)   ENCODE lzo
	,acct VARCHAR(65535)   ENCODE lzo
	,profit_cntr VARCHAR(65535)   ENCODE lzo
	,crncy_cd VARCHAR(65535)   ENCODE lzo
	,prod_hier_cd VARCHAR(65535)   ENCODE lzo
	,lvl1_prod_hier_cd VARCHAR(65535)   ENCODE lzo
	,prod_cd VARCHAR(65535)   ENCODE lzo
	,brand_cd VARCHAR(65535)   ENCODE lzo
	,shipto_cust_nbr VARCHAR(65535)   ENCODE lzo
	,shipto_ind_cd VARCHAR(65535)   ENCODE lzo
	,sgtxt VARCHAR(65535)   ENCODE lzo
	,amt NUMERIC(38,10)   ENCODE az64
	,int_functype VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,int_acctgrp VARCHAR(65535)   ENCODE lzo
	,quanunit VARCHAR(65535)   ENCODE lzo
	,quantity NUMERIC(38,10)   ENCODE az64
	,lvl2_prod_hier_cd VARCHAR(65535)   ENCODE lzo
	,lvl3_prod_hier_cd VARCHAR(65535)   ENCODE lzo
	,lvl4_prod_hier_cd VARCHAR(65535)   ENCODE lzo
	,lvl5_prod_hier_cd VARCHAR(65535)   ENCODE lzo
	,int_entity_type VARCHAR(65535)   ENCODE lzo
	,cust_act_grp VARCHAR(65535)   ENCODE lzo
	,cust_no VARCHAR(65535)   ENCODE lzo
	,cust_grp3 VARCHAR(65535)   ENCODE lzo
	,higher_lvl_cust VARCHAR(65535)   ENCODE lzo
	,rvers VARCHAR(65535)   ENCODE lzo
	,rtcur VARCHAR(65535)   ENCODE lzo
	,poper NUMERIC(38,10)   ENCODE az64
	,rhoart NUMERIC(38,10)   ENCODE az64
	,hrkft VARCHAR(65535)   ENCODE lzo
	,rassc VARCHAR(65535)   ENCODE lzo
	,eprctr VARCHAR(65535)   ENCODE lzo
	,activ VARCHAR(65535)   ENCODE lzo
	,afabe NUMERIC(38,10)   ENCODE az64
	,oclnt NUMERIC(38,10)   ENCODE az64
	,sbukrs VARCHAR(65535)   ENCODE lzo
	,sprctr VARCHAR(65535)   ENCODE lzo
	,shoart NUMERIC(38,10)   ENCODE az64
	,sfarea VARCHAR(65535)   ENCODE lzo
	,cpudt VARCHAR(65535)   ENCODE lzo
	,cputm VARCHAR(65535)   ENCODE lzo
	,usnam VARCHAR(65535)   ENCODE lzo
	,autom VARCHAR(65535)   ENCODE lzo
	,docty VARCHAR(65535)   ENCODE lzo
	,bldat VARCHAR(65535)   ENCODE lzo
	,budat VARCHAR(65535)   ENCODE lzo
	,wsdat VARCHAR(65535)   ENCODE lzo
	,refdocnr VARCHAR(65535)   ENCODE lzo
	,refryear NUMERIC(38,10)   ENCODE az64
	,refdocln NUMERIC(38,10)   ENCODE az64
	,refdocct VARCHAR(65535)   ENCODE lzo
	,refactiv VARCHAR(65535)   ENCODE lzo
	,awtyp VARCHAR(65535)   ENCODE lzo
	,aworg VARCHAR(65535)   ENCODE lzo
	,werks VARCHAR(65535)   ENCODE lzo
	,lstar VARCHAR(65535)   ENCODE lzo
	,aufnr VARCHAR(65535)   ENCODE lzo
	,aufpl NUMERIC(38,10)   ENCODE az64
	,anln1 VARCHAR(65535)   ENCODE lzo
	,anln2 VARCHAR(65535)   ENCODE lzo
	,bwkey VARCHAR(65535)   ENCODE lzo
	,bwtar VARCHAR(65535)   ENCODE lzo
	,anbwa VARCHAR(65535)   ENCODE lzo
	,kunnr VARCHAR(65535)   ENCODE lzo
	,lifnr VARCHAR(65535)   ENCODE lzo
	,rmvct VARCHAR(65535)   ENCODE lzo
	,ebeln VARCHAR(65535)   ENCODE lzo
	,ebelp NUMERIC(38,10)   ENCODE az64
	,kstrg VARCHAR(65535)   ENCODE lzo
	,erkrs VARCHAR(65535)   ENCODE lzo
	,paobjnr NUMERIC(38,10)   ENCODE az64
	,pasubnr NUMERIC(38,10)   ENCODE az64
	,ps_psp_pnr NUMERIC(38,10)   ENCODE az64
	,kdauf VARCHAR(65535)   ENCODE lzo
	,kdpos NUMERIC(38,10)   ENCODE az64
	,fkart VARCHAR(65535)   ENCODE lzo
	,vkorg VARCHAR(65535)   ENCODE lzo
	,vtweg VARCHAR(65535)   ENCODE lzo
	,aubel VARCHAR(65535)   ENCODE lzo
	,aupos NUMERIC(38,10)   ENCODE az64
	,spart VARCHAR(65535)   ENCODE lzo
	,vbeln VARCHAR(65535)   ENCODE lzo
	,posnr NUMERIC(38,10)   ENCODE az64
	,vkgrp VARCHAR(65535)   ENCODE lzo
	,vkbur VARCHAR(65535)   ENCODE lzo
	,vbund VARCHAR(65535)   ENCODE lzo
	,logsys VARCHAR(65535)   ENCODE lzo
	,alebn VARCHAR(65535)   ENCODE lzo
	,awsys VARCHAR(65535)   ENCODE lzo
	,versa VARCHAR(65535)   ENCODE lzo
	,stflg VARCHAR(65535)   ENCODE lzo
	,stokz VARCHAR(65535)   ENCODE lzo
	,rep_matnr VARCHAR(65535)   ENCODE lzo
	,co_prznr VARCHAR(65535)   ENCODE lzo
	,imkey VARCHAR(65535)   ENCODE lzo
	,dabrz VARCHAR(65535)   ENCODE lzo
	,valut VARCHAR(65535)   ENCODE lzo
	,rscope VARCHAR(65535)   ENCODE lzo
	,awref_rev VARCHAR(65535)   ENCODE lzo
	,aworg_rev VARCHAR(65535)   ENCODE lzo
	,bwart VARCHAR(65535)   ENCODE lzo
	,blart VARCHAR(65535)   ENCODE lzo
	,timestamp_ NUMERIC(38,10)   ENCODE az64
	,valuetype NUMERIC(38,10)   ENCODE az64
	,curtype VARCHAR(65535)   ENCODE lzo
	,fiscvar VARCHAR(65535)   ENCODE lzo
	,chartaccts VARCHAR(65535)   ENCODE lzo
	,upmod VARCHAR(65535)   ENCODE lzo
	,valutyp NUMERIC(38,10)   ENCODE az64
	,debit NUMERIC(38,10)   ENCODE az64
	,credit NUMERIC(38,10)   ENCODE az64
	,sbdp10_zzctry VARCHAR(65535)   ENCODE lzo
	,sbdp10_zzsoldto VARCHAR(65535)   ENCODE lzo
	,ps_posid VARCHAR(65535)   ENCODE lzo
	,odq_changemode VARCHAR(65535)   ENCODE lzo
	,odq_entitycntr VARCHAR(65535)   ENCODE lzo
	,ins_dtm VARCHAR(65535)   ENCODE lzo
	,upd_dtm VARCHAR(65535)   ENCODE lzo
	,runid BIGINT   ENCODE az64
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.p10_0ec_pca_3_trans_current owner to base_admin;


-- bods.p10_0ec_pca_3_trans_current_temp definition

-- Drop table

-- DROP TABLE bods.p10_0ec_pca_3_trans_current_temp;

--DROP TABLE bods.p10_0ec_pca_3_trans_current_temp;
CREATE TABLE IF NOT EXISTS bods.p10_0ec_pca_3_trans_current_temp
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id BIGINT   ENCODE az64
	,fiscper BIGINT   ENCODE az64
	,fyr_id BIGINT   ENCODE az64
	,fmth_nbr BIGINT   ENCODE az64
	,docct VARCHAR(65535)   ENCODE lzo
	,docnr VARCHAR(65535)   ENCODE lzo
	,docln VARCHAR(65535)   ENCODE lzo
	,co_cd VARCHAR(65535)   ENCODE lzo
	,bus_area VARCHAR(65535)   ENCODE lzo
	,func_area VARCHAR(65535)   ENCODE lzo
	,dept VARCHAR(65535)   ENCODE lzo
	,func_area_lkp VARCHAR(65535)   ENCODE lzo
	,ctrl_area VARCHAR(65535)   ENCODE lzo
	,cost_cntr VARCHAR(65535)   ENCODE lzo
	,acct VARCHAR(65535)   ENCODE lzo
	,profit_cntr VARCHAR(65535)   ENCODE lzo
	,crncy_cd VARCHAR(65535)   ENCODE lzo
	,prod_hier_cd VARCHAR(65535)   ENCODE lzo
	,lvl1_prod_hier_cd VARCHAR(65535)   ENCODE lzo
	,prod_cd VARCHAR(65535)   ENCODE lzo
	,brand_cd VARCHAR(65535)   ENCODE lzo
	,shipto_cust_nbr VARCHAR(65535)   ENCODE lzo
	,shipto_ind_cd VARCHAR(65535)   ENCODE lzo
	,sgtxt VARCHAR(65535)   ENCODE lzo
	,amt NUMERIC(38,10)   ENCODE az64
	,int_functype VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,int_acctgrp VARCHAR(65535)   ENCODE lzo
	,quanunit VARCHAR(65535)   ENCODE lzo
	,quantity NUMERIC(38,10)   ENCODE az64
	,lvl2_prod_hier_cd VARCHAR(65535)   ENCODE lzo
	,lvl3_prod_hier_cd VARCHAR(65535)   ENCODE lzo
	,lvl4_prod_hier_cd VARCHAR(65535)   ENCODE lzo
	,lvl5_prod_hier_cd VARCHAR(65535)   ENCODE lzo
	,int_entity_type VARCHAR(65535)   ENCODE lzo
	,cust_act_grp VARCHAR(65535)   ENCODE lzo
	,cust_no VARCHAR(65535)   ENCODE lzo
	,cust_grp3 VARCHAR(65535)   ENCODE lzo
	,higher_lvl_cust VARCHAR(65535)   ENCODE lzo
	,rvers VARCHAR(65535)   ENCODE lzo
	,rtcur VARCHAR(65535)   ENCODE lzo
	,poper NUMERIC(38,10)   ENCODE az64
	,rhoart NUMERIC(38,10)   ENCODE az64
	,hrkft VARCHAR(65535)   ENCODE lzo
	,rassc VARCHAR(65535)   ENCODE lzo
	,eprctr VARCHAR(65535)   ENCODE lzo
	,activ VARCHAR(65535)   ENCODE lzo
	,afabe NUMERIC(38,10)   ENCODE az64
	,oclnt NUMERIC(38,10)   ENCODE az64
	,sbukrs VARCHAR(65535)   ENCODE lzo
	,sprctr VARCHAR(65535)   ENCODE lzo
	,shoart NUMERIC(38,10)   ENCODE az64
	,sfarea VARCHAR(65535)   ENCODE lzo
	,cpudt VARCHAR(65535)   ENCODE lzo
	,cputm VARCHAR(65535)   ENCODE lzo
	,usnam VARCHAR(65535)   ENCODE lzo
	,autom VARCHAR(65535)   ENCODE lzo
	,docty VARCHAR(65535)   ENCODE lzo
	,bldat VARCHAR(65535)   ENCODE lzo
	,budat VARCHAR(65535)   ENCODE lzo
	,wsdat VARCHAR(65535)   ENCODE lzo
	,refdocnr VARCHAR(65535)   ENCODE lzo
	,refryear NUMERIC(38,10)   ENCODE az64
	,refdocln NUMERIC(38,10)   ENCODE az64
	,refdocct VARCHAR(65535)   ENCODE lzo
	,refactiv VARCHAR(65535)   ENCODE lzo
	,awtyp VARCHAR(65535)   ENCODE lzo
	,aworg VARCHAR(65535)   ENCODE lzo
	,werks VARCHAR(65535)   ENCODE lzo
	,lstar VARCHAR(65535)   ENCODE lzo
	,aufnr VARCHAR(65535)   ENCODE lzo
	,aufpl NUMERIC(38,10)   ENCODE az64
	,anln1 VARCHAR(65535)   ENCODE lzo
	,anln2 VARCHAR(65535)   ENCODE lzo
	,bwkey VARCHAR(65535)   ENCODE lzo
	,bwtar VARCHAR(65535)   ENCODE lzo
	,anbwa VARCHAR(65535)   ENCODE lzo
	,kunnr VARCHAR(65535)   ENCODE lzo
	,lifnr VARCHAR(65535)   ENCODE lzo
	,rmvct VARCHAR(65535)   ENCODE lzo
	,ebeln VARCHAR(65535)   ENCODE lzo
	,ebelp NUMERIC(38,10)   ENCODE az64
	,kstrg VARCHAR(65535)   ENCODE lzo
	,erkrs VARCHAR(65535)   ENCODE lzo
	,paobjnr NUMERIC(38,10)   ENCODE az64
	,pasubnr NUMERIC(38,10)   ENCODE az64
	,ps_psp_pnr NUMERIC(38,10)   ENCODE az64
	,kdauf VARCHAR(65535)   ENCODE lzo
	,kdpos NUMERIC(38,10)   ENCODE az64
	,fkart VARCHAR(65535)   ENCODE lzo
	,vkorg VARCHAR(65535)   ENCODE lzo
	,vtweg VARCHAR(65535)   ENCODE lzo
	,aubel VARCHAR(65535)   ENCODE lzo
	,aupos NUMERIC(38,10)   ENCODE az64
	,spart VARCHAR(65535)   ENCODE lzo
	,vbeln VARCHAR(65535)   ENCODE lzo
	,posnr NUMERIC(38,10)   ENCODE az64
	,vkgrp VARCHAR(65535)   ENCODE lzo
	,vkbur VARCHAR(65535)   ENCODE lzo
	,vbund VARCHAR(65535)   ENCODE lzo
	,logsys VARCHAR(65535)   ENCODE lzo
	,alebn VARCHAR(65535)   ENCODE lzo
	,awsys VARCHAR(65535)   ENCODE lzo
	,versa VARCHAR(65535)   ENCODE lzo
	,stflg VARCHAR(65535)   ENCODE lzo
	,stokz VARCHAR(65535)   ENCODE lzo
	,rep_matnr VARCHAR(65535)   ENCODE lzo
	,co_prznr VARCHAR(65535)   ENCODE lzo
	,imkey VARCHAR(65535)   ENCODE lzo
	,dabrz VARCHAR(65535)   ENCODE lzo
	,valut VARCHAR(65535)   ENCODE lzo
	,rscope VARCHAR(65535)   ENCODE lzo
	,awref_rev VARCHAR(65535)   ENCODE lzo
	,aworg_rev VARCHAR(65535)   ENCODE lzo
	,bwart VARCHAR(65535)   ENCODE lzo
	,blart VARCHAR(65535)   ENCODE lzo
	,timestamp_ NUMERIC(38,10)   ENCODE az64
	,valuetype NUMERIC(38,10)   ENCODE az64
	,curtype VARCHAR(65535)   ENCODE lzo
	,fiscvar VARCHAR(65535)   ENCODE lzo
	,chartaccts VARCHAR(65535)   ENCODE lzo
	,upmod VARCHAR(65535)   ENCODE lzo
	,valutyp NUMERIC(38,10)   ENCODE az64
	,debit NUMERIC(38,10)   ENCODE az64
	,credit NUMERIC(38,10)   ENCODE az64
	,sbdp10_zzctry VARCHAR(65535)   ENCODE lzo
	,sbdp10_zzsoldto VARCHAR(65535)   ENCODE lzo
	,ps_posid VARCHAR(65535)   ENCODE lzo
	,odq_changemode VARCHAR(65535)   ENCODE lzo
	,odq_entitycntr VARCHAR(65535)   ENCODE lzo
	,ins_dtm VARCHAR(65535)   ENCODE lzo
	,upd_dtm VARCHAR(65535)   ENCODE lzo
	,runid BIGINT   ENCODE az64
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE EVEN
;
ALTER TABLE bods.p10_0ec_pca_3_trans_current_temp owner to base_admin;


-- bods.qad_argentina_pl_trans_current definition

-- Drop table

-- DROP TABLE bods.qad_argentina_pl_trans_current;

--DROP TABLE bods.qad_argentina_pl_trans_current;
CREATE TABLE IF NOT EXISTS bods.qad_argentina_pl_trans_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id BIGINT   ENCODE az64
	,fiscper VARCHAR(65535)   ENCODE lzo
	,"year" VARCHAR(65535)   ENCODE lzo
	,period VARCHAR(65535)   ENCODE lzo
	,qad_entity VARCHAR(65535)   ENCODE lzo
	,acct VARCHAR(65535)   ENCODE lzo
	,profit_center VARCHAR(65535)   ENCODE lzo
	,costctr VARCHAR(65535)   ENCODE lzo
	,functional_area VARCHAR(65535)   ENCODE lzo
	,site VARCHAR(65535)   ENCODE lzo
	,sold_to_customer VARCHAR(65535)   ENCODE lzo
	,bill_to_customer VARCHAR(65535)   ENCODE lzo
	,product VARCHAR(65535)   ENCODE lzo
	,ship_to VARCHAR(65535)   ENCODE lzo
	,ship_to_customer_country VARCHAR(65535)   ENCODE lzo
	,sold_to_customer_chanel VARCHAR(65535)   ENCODE lzo
	,currency_code VARCHAR(65535)   ENCODE lzo
	,transaction_date VARCHAR(65535)   ENCODE lzo
	,posting_date VARCHAR(65535)   ENCODE lzo
	,document_type VARCHAR(65535)   ENCODE lzo
	,document_id VARCHAR(65535)   ENCODE lzo
	,amount NUMERIC(38,10)   ENCODE az64
	,usd_amount NUMERIC(38,10)   ENCODE az64
	,product_line VARCHAR(65535)   ENCODE lzo
	,product_group VARCHAR(65535)   ENCODE lzo
	,product_class VARCHAR(65535)   ENCODE lzo
	,product_sub_class VARCHAR(65535)   ENCODE lzo
	,product_category VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,int_entitytype VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,brand VARCHAR(65535)   ENCODE lzo
	,brand_description VARCHAR(65535)   ENCODE lzo
	,quantity NUMERIC(38,10)   ENCODE az64
	,customer_name VARCHAR(65535)   ENCODE lzo
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.qad_argentina_pl_trans_current owner to base_admin;


-- bods.qad_brazil_pl_trans_current definition

-- Drop table

-- DROP TABLE bods.qad_brazil_pl_trans_current;

--DROP TABLE bods.qad_brazil_pl_trans_current;
CREATE TABLE IF NOT EXISTS bods.qad_brazil_pl_trans_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id BIGINT   ENCODE az64
	,fiscper VARCHAR(65535)   ENCODE lzo
	,"year" VARCHAR(65535)   ENCODE lzo
	,period VARCHAR(65535)   ENCODE lzo
	,qad_entity VARCHAR(65535)   ENCODE lzo
	,qad_domain VARCHAR(65535)   ENCODE lzo
	,site VARCHAR(65535)   ENCODE lzo
	,acct VARCHAR(65535)   ENCODE lzo
	,profit_center VARCHAR(65535)   ENCODE lzo
	,costctr VARCHAR(65535)   ENCODE lzo
	,functional_area VARCHAR(65535)   ENCODE lzo
	,sold_to_customer VARCHAR(65535)   ENCODE lzo
	,bill_to_customer VARCHAR(65535)   ENCODE lzo
	,product VARCHAR(65535)   ENCODE lzo
	,ship_to_customer VARCHAR(65535)   ENCODE lzo
	,ship_to_customer_country VARCHAR(65535)   ENCODE lzo
	,sold_to_customer_channel VARCHAR(65535)   ENCODE lzo
	,currency_code VARCHAR(65535)   ENCODE lzo
	,transaction_date VARCHAR(65535)   ENCODE lzo
	,posting_date VARCHAR(65535)   ENCODE lzo
	,document_type VARCHAR(65535)   ENCODE lzo
	,document_id VARCHAR(65535)   ENCODE lzo
	,amount NUMERIC(38,10)   ENCODE az64
	,usd_amount NUMERIC(38,10)   ENCODE az64
	,product_group VARCHAR(65535)   ENCODE lzo
	,product_class VARCHAR(65535)   ENCODE lzo
	,product_sub_category VARCHAR(65535)   ENCODE lzo
	,product_line VARCHAR(65535)   ENCODE lzo
	,product_sub_class VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,brand VARCHAR(65535)   ENCODE lzo
	,brand_description VARCHAR(65535)   ENCODE lzo
	,quantity NUMERIC(38,10)   ENCODE az64
	,customer_name VARCHAR(65535)   ENCODE lzo
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.qad_brazil_pl_trans_current owner to base_admin;


-- bods.qad_cca_pl_trans_current definition

-- Drop table

-- DROP TABLE bods.qad_cca_pl_trans_current;

--DROP TABLE bods.qad_cca_pl_trans_current;
CREATE TABLE IF NOT EXISTS bods.qad_cca_pl_trans_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id BIGINT   ENCODE az64
	,fiscper VARCHAR(65535)   ENCODE lzo
	,"year" VARCHAR(65535)   ENCODE lzo
	,period VARCHAR(65535)   ENCODE lzo
	,qad_entity VARCHAR(65535)   ENCODE lzo
	,acct VARCHAR(65535)   ENCODE lzo
	,profit_center VARCHAR(65535)   ENCODE lzo
	,costctr VARCHAR(65535)   ENCODE lzo
	,functional_area VARCHAR(65535)   ENCODE lzo
	,site VARCHAR(65535)   ENCODE lzo
	,sold_to_customer VARCHAR(65535)   ENCODE lzo
	,bill_to_customer VARCHAR(65535)   ENCODE lzo
	,product VARCHAR(65535)   ENCODE lzo
	,ship_to VARCHAR(65535)   ENCODE lzo
	,ship_to_customer_country VARCHAR(65535)   ENCODE lzo
	,sold_to_customer_chanel VARCHAR(65535)   ENCODE lzo
	,currency_code VARCHAR(65535)   ENCODE lzo
	,transaction_date VARCHAR(65535)   ENCODE lzo
	,posting_date VARCHAR(65535)   ENCODE lzo
	,document_type VARCHAR(65535)   ENCODE lzo
	,document_id VARCHAR(65535)   ENCODE lzo
	,amount NUMERIC(38,10)   ENCODE az64
	,usd_amount NUMERIC(38,10)   ENCODE az64
	,product_line VARCHAR(65535)   ENCODE lzo
	,product_group VARCHAR(65535)   ENCODE lzo
	,product_class VARCHAR(65535)   ENCODE lzo
	,product_sub_class VARCHAR(65535)   ENCODE lzo
	,product_category VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,int_entitytype VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.qad_cca_pl_trans_current owner to base_admin;


-- bods.qad_chile_pl_trans_current definition

-- Drop table

-- DROP TABLE bods.qad_chile_pl_trans_current;

--DROP TABLE bods.qad_chile_pl_trans_current;
CREATE TABLE IF NOT EXISTS bods.qad_chile_pl_trans_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id BIGINT   ENCODE az64
	,fiscper VARCHAR(65535)   ENCODE lzo
	,"year" VARCHAR(65535)   ENCODE lzo
	,period VARCHAR(65535)   ENCODE lzo
	,qad_entity VARCHAR(65535)   ENCODE lzo
	,acct VARCHAR(65535)   ENCODE lzo
	,profit_center VARCHAR(65535)   ENCODE lzo
	,costctr VARCHAR(65535)   ENCODE lzo
	,functional_area VARCHAR(65535)   ENCODE lzo
	,site VARCHAR(65535)   ENCODE lzo
	,sold_to_customer VARCHAR(65535)   ENCODE lzo
	,bill_to_customer VARCHAR(65535)   ENCODE lzo
	,product VARCHAR(65535)   ENCODE lzo
	,ship_to VARCHAR(65535)   ENCODE lzo
	,ship_to_customer_country VARCHAR(65535)   ENCODE lzo
	,sold_to_customer_chanel VARCHAR(65535)   ENCODE lzo
	,currency_code VARCHAR(65535)   ENCODE lzo
	,transaction_date VARCHAR(65535)   ENCODE lzo
	,posting_date VARCHAR(65535)   ENCODE lzo
	,document_type VARCHAR(65535)   ENCODE lzo
	,document_id VARCHAR(65535)   ENCODE lzo
	,amount NUMERIC(38,10)   ENCODE az64
	,usd_amount NUMERIC(38,10)   ENCODE az64
	,product_line VARCHAR(65535)   ENCODE lzo
	,product_group VARCHAR(65535)   ENCODE lzo
	,product_class VARCHAR(65535)   ENCODE lzo
	,product_sub_class VARCHAR(65535)   ENCODE lzo
	,product_category VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,int_entitytype VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,brand VARCHAR(65535)   ENCODE lzo
	,brand_description VARCHAR(65535)   ENCODE lzo
	,quantity NUMERIC(38,10)   ENCODE az64
	,customer_name VARCHAR(65535)   ENCODE lzo
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.qad_chile_pl_trans_current owner to base_admin;


-- bods.qad_dech_pl_trans_current definition

-- Drop table

-- DROP TABLE bods.qad_dech_pl_trans_current;

--DROP TABLE bods.qad_dech_pl_trans_current;
CREATE TABLE IF NOT EXISTS bods.qad_dech_pl_trans_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id VARCHAR(65535)   ENCODE lzo
	,"year" VARCHAR(65535)   ENCODE lzo
	,period VARCHAR(65535)   ENCODE lzo
	,"account" VARCHAR(65535)   ENCODE lzo
	,cost_center VARCHAR(65535)   ENCODE lzo
	,site VARCHAR(65535)   ENCODE lzo
	,sold_to_customer VARCHAR(65535)   ENCODE lzo
	,bill_to_customer VARCHAR(65535)   ENCODE lzo
	,product VARCHAR(65535)   ENCODE lzo
	,ship_to VARCHAR(65535)   ENCODE lzo
	,ship_to_customer_country VARCHAR(65535)   ENCODE lzo
	,sold_to_customer_channel VARCHAR(65535)   ENCODE lzo
	,currency_code VARCHAR(65535)   ENCODE lzo
	,transaction_date VARCHAR(65535)   ENCODE lzo
	,posting_date VARCHAR(65535)   ENCODE lzo
	,document_type VARCHAR(65535)   ENCODE lzo
	,document_id VARCHAR(65535)   ENCODE lzo
	,amount NUMERIC(38,10)   ENCODE az64
	,usd_amount VARCHAR(65535)   ENCODE lzo
	,quantity NUMERIC(38,10)   ENCODE az64
	,customer_name VARCHAR(65535)   ENCODE lzo
	,fiscper VARCHAR(65535)   ENCODE lzo
	,src_id VARCHAR(65535)   ENCODE lzo
	,prod_prod_line VARCHAR(65535)   ENCODE lzo
	,prod_group VARCHAR(65535)   ENCODE lzo
	,prod_item_type VARCHAR(65535)   ENCODE lzo
	,cust_class VARCHAR(65535)   ENCODE lzo
	,cust_type VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.qad_dech_pl_trans_current owner to base_admin;


-- bods.qad_peru_pl_trans_current definition

-- Drop table

-- DROP TABLE bods.qad_peru_pl_trans_current;

--DROP TABLE bods.qad_peru_pl_trans_current;
CREATE TABLE IF NOT EXISTS bods.qad_peru_pl_trans_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id BIGINT   ENCODE az64
	,fiscper VARCHAR(65535)   ENCODE lzo
	,"year" VARCHAR(65535)   ENCODE lzo
	,period VARCHAR(65535)   ENCODE lzo
	,qad_entity VARCHAR(65535)   ENCODE lzo
	,acct VARCHAR(65535)   ENCODE lzo
	,profit_center VARCHAR(65535)   ENCODE lzo
	,costctr VARCHAR(65535)   ENCODE lzo
	,functional_area VARCHAR(65535)   ENCODE lzo
	,site VARCHAR(65535)   ENCODE lzo
	,sold_to_customer VARCHAR(65535)   ENCODE lzo
	,bill_to_customer VARCHAR(65535)   ENCODE lzo
	,product VARCHAR(65535)   ENCODE lzo
	,ship_to VARCHAR(65535)   ENCODE lzo
	,ship_to_customer_country VARCHAR(65535)   ENCODE lzo
	,sold_to_customer_chanel VARCHAR(65535)   ENCODE lzo
	,currency_code VARCHAR(65535)   ENCODE lzo
	,transaction_date VARCHAR(65535)   ENCODE lzo
	,posting_date VARCHAR(65535)   ENCODE lzo
	,document_type VARCHAR(65535)   ENCODE lzo
	,document_id VARCHAR(65535)   ENCODE lzo
	,amount NUMERIC(38,10)   ENCODE az64
	,usd_amount NUMERIC(38,10)   ENCODE az64
	,product_line VARCHAR(65535)   ENCODE lzo
	,product_group VARCHAR(65535)   ENCODE lzo
	,product_class VARCHAR(65535)   ENCODE lzo
	,product_sub_class VARCHAR(65535)   ENCODE lzo
	,product_category VARCHAR(65535)   ENCODE lzo
	,int_entitytype VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,brand VARCHAR(65535)   ENCODE lzo
	,brand_description VARCHAR(65535)   ENCODE lzo
	,quantity NUMERIC(38,10)   ENCODE az64
	,customer_name VARCHAR(65535)   ENCODE lzo
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.qad_peru_pl_trans_current owner to base_admin;


-- bods.sc1_test_1_current definition

-- Drop table

-- DROP TABLE bods.sc1_test_1_current;

--DROP TABLE bods.sc1_test_1_current;
CREATE TABLE IF NOT EXISTS bods.sc1_test_1_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,header__change_seq VARCHAR(65535)   ENCODE lzo
	,header__change_oper VARCHAR(65535)   ENCODE lzo
	,header__change_mask VARCHAR(65535)   ENCODE lzo
	,header__stream_position VARCHAR(65535)   ENCODE lzo
	,header__operation VARCHAR(65535)   ENCODE lzo
	,header__transaction_id VARCHAR(65535)   ENCODE lzo
	,header__timestamp VARCHAR(65535)   ENCODE lzo
	,rpkco VARCHAR(65535)   ENCODE lzo
	,rpdoc VARCHAR(65535)   ENCODE lzo
	,rpdct VARCHAR(65535)   ENCODE lzo
	,rpsfx VARCHAR(65535)   ENCODE lzo
	,rpsfxe VARCHAR(65535)   ENCODE lzo
	,rpdcta VARCHAR(65535)   ENCODE lzo
	,rpan8 VARCHAR(65535)   ENCODE lzo
	,rppye VARCHAR(65535)   ENCODE lzo
	,rpsnto VARCHAR(65535)   ENCODE lzo
	,rpdivj VARCHAR(65535)   ENCODE lzo
	,rpdsvj VARCHAR(65535)   ENCODE lzo
	,rpddj VARCHAR(65535)   ENCODE lzo
	,rpddnj VARCHAR(65535)   ENCODE lzo
	,rpdgj VARCHAR(65535)   ENCODE lzo
	,rpfy VARCHAR(65535)   ENCODE lzo
	,rpctry VARCHAR(65535)   ENCODE lzo
	,rppn VARCHAR(65535)   ENCODE lzo
	,rpco VARCHAR(65535)   ENCODE lzo
	,rpicu VARCHAR(65535)   ENCODE lzo
	,rpicut VARCHAR(65535)   ENCODE lzo
	,rpdicj VARCHAR(65535)   ENCODE lzo
	,rpbalj VARCHAR(65535)   ENCODE lzo
	,rppst VARCHAR(65535)   ENCODE lzo
	,rpag VARCHAR(65535)   ENCODE lzo
	,rpaap VARCHAR(65535)   ENCODE lzo
	,rpadsc VARCHAR(65535)   ENCODE lzo
	,rpadsa VARCHAR(65535)   ENCODE lzo
	,rpatxa VARCHAR(65535)   ENCODE lzo
	,rpatxn VARCHAR(65535)   ENCODE lzo
	,rpstam VARCHAR(65535)   ENCODE lzo
	,rptxa1 VARCHAR(65535)   ENCODE lzo
	,rpexr1 VARCHAR(65535)   ENCODE lzo
	,rpcrrm VARCHAR(65535)   ENCODE lzo
	,rpcrcd VARCHAR(65535)   ENCODE lzo
	,rpcrr VARCHAR(65535)   ENCODE lzo
	,rpacr VARCHAR(65535)   ENCODE lzo
	,rpfap VARCHAR(65535)   ENCODE lzo
	,rpcds VARCHAR(65535)   ENCODE lzo
	,rpcdsa VARCHAR(65535)   ENCODE lzo
	,rpctxa VARCHAR(65535)   ENCODE lzo
	,rpctxn VARCHAR(65535)   ENCODE lzo
	,rpctam VARCHAR(65535)   ENCODE lzo
	,rpglc VARCHAR(65535)   ENCODE lzo
	,rpglba VARCHAR(65535)   ENCODE lzo
	,rppost VARCHAR(65535)   ENCODE lzo
	,rpam VARCHAR(65535)   ENCODE lzo
	,rpaid2 VARCHAR(65535)   ENCODE lzo
	,rpmcu VARCHAR(65535)   ENCODE lzo
	,rpobj VARCHAR(65535)   ENCODE lzo
	,rpsub VARCHAR(65535)   ENCODE lzo
	,rpsblt VARCHAR(65535)   ENCODE lzo
	,rpsbl VARCHAR(65535)   ENCODE lzo
	,rpbaid VARCHAR(65535)   ENCODE lzo
	,rpptc VARCHAR(65535)   ENCODE lzo
	,rpvod VARCHAR(65535)   ENCODE lzo
	,rpokco VARCHAR(65535)   ENCODE lzo
	,rpodct VARCHAR(65535)   ENCODE lzo
	,rpodoc VARCHAR(65535)   ENCODE lzo
	,rposfx VARCHAR(65535)   ENCODE lzo
	,rpcrc VARCHAR(65535)   ENCODE lzo
	,rpvinv VARCHAR(65535)   ENCODE lzo
	,rppkco VARCHAR(65535)   ENCODE lzo
	,rppo VARCHAR(65535)   ENCODE lzo
	,rppdct VARCHAR(65535)   ENCODE lzo
	,rplnid VARCHAR(65535)   ENCODE lzo
	,rpsfxo VARCHAR(65535)   ENCODE lzo
	,rpopsq VARCHAR(65535)   ENCODE lzo
	,rpvr01 VARCHAR(65535)   ENCODE lzo
	,rpunit VARCHAR(65535)   ENCODE lzo
	,rpmcu2 VARCHAR(65535)   ENCODE lzo
	,rprmk VARCHAR(65535)   ENCODE lzo
	,rprf VARCHAR(65535)   ENCODE lzo
	,rpdrf VARCHAR(65535)   ENCODE lzo
	,rpctl VARCHAR(65535)   ENCODE lzo
	,rpfnlp VARCHAR(65535)   ENCODE lzo
	,rpu VARCHAR(65535)   ENCODE lzo
	,rpum VARCHAR(65535)   ENCODE lzo
	,rppyin VARCHAR(65535)   ENCODE lzo
	,rptxa3 VARCHAR(65535)   ENCODE lzo
	,rpexr3 VARCHAR(65535)   ENCODE lzo
	,rprp1 VARCHAR(65535)   ENCODE lzo
	,rprp2 VARCHAR(65535)   ENCODE lzo
	,rprp3 VARCHAR(65535)   ENCODE lzo
	,rpac07 VARCHAR(65535)   ENCODE lzo
	,rptnn VARCHAR(65535)   ENCODE lzo
	,rpdmcd VARCHAR(65535)   ENCODE lzo
	,rpitm VARCHAR(65535)   ENCODE lzo
	,rphcrr VARCHAR(65535)   ENCODE lzo
	,rphdgj VARCHAR(65535)   ENCODE lzo
	,rpurc1 VARCHAR(65535)   ENCODE lzo
	,rpurdt VARCHAR(65535)   ENCODE lzo
	,rpurat VARCHAR(65535)   ENCODE lzo
	,rpurab VARCHAR(65535)   ENCODE lzo
	,rpurrf VARCHAR(65535)   ENCODE lzo
	,rptorg VARCHAR(65535)   ENCODE lzo
	,rpuser VARCHAR(65535)   ENCODE lzo
	,rppid VARCHAR(65535)   ENCODE lzo
	,rpupmj VARCHAR(65535)   ENCODE lzo
	,rpupmt VARCHAR(65535)   ENCODE lzo
	,rpjobn VARCHAR(65535)   ENCODE lzo
	,rptnst VARCHAR(65535)   ENCODE lzo
	,rpyc01 VARCHAR(65535)   ENCODE lzo
	,rpyc02 VARCHAR(65535)   ENCODE lzo
	,rpyc03 VARCHAR(65535)   ENCODE lzo
	,rpyc04 VARCHAR(65535)   ENCODE lzo
	,rpyc05 VARCHAR(65535)   ENCODE lzo
	,rpyc06 VARCHAR(65535)   ENCODE lzo
	,rpyc07 VARCHAR(65535)   ENCODE lzo
	,rpyc08 VARCHAR(65535)   ENCODE lzo
	,rpyc09 VARCHAR(65535)   ENCODE lzo
	,rpyc10 VARCHAR(65535)   ENCODE lzo
	,rpdtxs VARCHAR(65535)   ENCODE lzo
	,rpbcrc VARCHAR(65535)   ENCODE lzo
	,rpatad VARCHAR(65535)   ENCODE lzo
	,rpctad VARCHAR(65535)   ENCODE lzo
	,rpnrta VARCHAR(65535)   ENCODE lzo
	,rpfnrt VARCHAR(65535)   ENCODE lzo
	,rptaxp VARCHAR(65535)   ENCODE lzo
	,rpprgf VARCHAR(65535)   ENCODE lzo
	,rpgfl5 VARCHAR(65535)   ENCODE lzo
	,rpgfl6 VARCHAR(65535)   ENCODE lzo
	,rpgam1 VARCHAR(65535)   ENCODE lzo
	,rpgam2 VARCHAR(65535)   ENCODE lzo
	,rpgen4 VARCHAR(65535)   ENCODE lzo
	,rpgen5 VARCHAR(65535)   ENCODE lzo
	,rpwtad VARCHAR(65535)   ENCODE lzo
	,rpwtaf VARCHAR(65535)   ENCODE lzo
	,rpsmmf VARCHAR(65535)   ENCODE lzo
	,rppywp VARCHAR(65535)   ENCODE lzo
	,rppwpg VARCHAR(65535)   ENCODE lzo
	,rpnettcid VARCHAR(65535)   ENCODE lzo
	,rpnetdoc VARCHAR(65535)   ENCODE lzo
	,rpnetrc5 VARCHAR(65535)   ENCODE lzo
	,rpnetst VARCHAR(65535)   ENCODE lzo
	,rpcntrtid VARCHAR(65535)   ENCODE lzo
	,rpcntrtcd VARCHAR(65535)   ENCODE lzo
	,rpwvid VARCHAR(65535)   ENCODE lzo
	,rpblscd2 VARCHAR(65535)   ENCODE lzo
	,rpharper VARCHAR(65535)   ENCODE lzo
	,rpharsfx VARCHAR(65535)   ENCODE lzo
	,rpddrl VARCHAR(65535)   ENCODE lzo
	,rpseqn VARCHAR(65535)   ENCODE lzo
	,rpclass01 VARCHAR(65535)   ENCODE lzo
	,rpclass02 VARCHAR(65535)   ENCODE lzo
	,rpclass03 VARCHAR(65535)   ENCODE lzo
	,rpclass04 VARCHAR(65535)   ENCODE lzo
	,rpclass05 VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.sc1_test_1_current owner to base_admin;


-- bods.sc1_test_current definition

-- Drop table

-- DROP TABLE bods.sc1_test_current;

--DROP TABLE bods.sc1_test_current;
CREATE TABLE IF NOT EXISTS bods.sc1_test_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,mandt VARCHAR(5)   ENCODE lzo
	,header__change_seq VARCHAR(65535)   ENCODE lzo
	,ebeln VARCHAR(10)   ENCODE lzo
	,header__change_oper VARCHAR(65535)   ENCODE lzo
	,ebelp NUMERIC(38,10)   ENCODE az64
	,header__change_mask VARCHAR(65535)   ENCODE lzo
	,loekz VARCHAR(5)   ENCODE lzo
	,header__stream_position VARCHAR(65535)   ENCODE lzo
	,statu VARCHAR(5)   ENCODE lzo
	,header__operation VARCHAR(65535)   ENCODE lzo
	,aedat VARCHAR(10)   ENCODE lzo
	,header__transaction_id VARCHAR(65535)   ENCODE lzo
	,txz01 VARCHAR(40)   ENCODE lzo
	,header__timestamp VARCHAR(65535)   ENCODE lzo
	,matnr VARCHAR(40)   ENCODE lzo
	,rpkco VARCHAR(65535)   ENCODE lzo
	,ematn VARCHAR(40)   ENCODE lzo
	,rpdoc VARCHAR(65535)   ENCODE lzo
	,bukrs VARCHAR(4)   ENCODE lzo
	,rpdct VARCHAR(65535)   ENCODE lzo
	,werks VARCHAR(4)   ENCODE lzo
	,rpsfx VARCHAR(65535)   ENCODE lzo
	,lgort VARCHAR(4)   ENCODE lzo
	,rpsfxe VARCHAR(65535)   ENCODE lzo
	,bednr VARCHAR(10)   ENCODE lzo
	,rpdcta VARCHAR(65535)   ENCODE lzo
	,matkl VARCHAR(9)   ENCODE lzo
	,rpan8 VARCHAR(65535)   ENCODE lzo
	,infnr VARCHAR(10)   ENCODE lzo
	,rppye VARCHAR(65535)   ENCODE lzo
	,idnlf VARCHAR(35)   ENCODE lzo
	,rpsnto VARCHAR(65535)   ENCODE lzo
	,ktmng NUMERIC(38,10)   ENCODE az64
	,rpdivj VARCHAR(65535)   ENCODE lzo
	,menge NUMERIC(38,10)   ENCODE az64
	,rpdsvj VARCHAR(65535)   ENCODE lzo
	,meins VARCHAR(5)   ENCODE lzo
	,rpddj VARCHAR(65535)   ENCODE lzo
	,bprme VARCHAR(5)   ENCODE lzo
	,rpddnj VARCHAR(65535)   ENCODE lzo
	,bpumz NUMERIC(38,10)   ENCODE az64
	,rpdgj VARCHAR(65535)   ENCODE lzo
	,bpumn NUMERIC(38,10)   ENCODE az64
	,rpfy VARCHAR(65535)   ENCODE lzo
	,umrez NUMERIC(38,10)   ENCODE az64
	,rpctry VARCHAR(65535)   ENCODE lzo
	,umren NUMERIC(38,10)   ENCODE az64
	,rppn VARCHAR(65535)   ENCODE lzo
	,netpr NUMERIC(38,10)   ENCODE az64
	,rpco VARCHAR(65535)   ENCODE lzo
	,peinh NUMERIC(38,10)   ENCODE az64
	,rpicu VARCHAR(65535)   ENCODE lzo
	,netwr NUMERIC(38,10)   ENCODE az64
	,rpicut VARCHAR(65535)   ENCODE lzo
	,brtwr NUMERIC(38,10)   ENCODE az64
	,rpdicj VARCHAR(65535)   ENCODE lzo
	,agdat VARCHAR(10)   ENCODE lzo
	,rpbalj VARCHAR(65535)   ENCODE lzo
	,webaz NUMERIC(38,10)   ENCODE az64
	,rppst VARCHAR(65535)   ENCODE lzo
	,mwskz VARCHAR(5)   ENCODE lzo
	,rpag VARCHAR(65535)   ENCODE lzo
	,bonus VARCHAR(5)   ENCODE lzo
	,insmk VARCHAR(5)   ENCODE lzo
	,spinf VARCHAR(5)   ENCODE lzo
	,prsdr VARCHAR(5)   ENCODE lzo
	,schpr VARCHAR(5)   ENCODE lzo
	,mahnz NUMERIC(38,10)   ENCODE az64
	,mahn1 NUMERIC(38,10)   ENCODE az64
	,mahn2 NUMERIC(38,10)   ENCODE az64
	,mahn3 NUMERIC(38,10)   ENCODE az64
	,uebto NUMERIC(38,10)   ENCODE az64
	,uebtk VARCHAR(5)   ENCODE lzo
	,untto NUMERIC(38,10)   ENCODE az64
	,bwtar VARCHAR(10)   ENCODE lzo
	,bwtty VARCHAR(5)   ENCODE lzo
	,abskz VARCHAR(5)   ENCODE lzo
	,agmem VARCHAR(5)   ENCODE lzo
	,elikz VARCHAR(5)   ENCODE lzo
	,erekz VARCHAR(5)   ENCODE lzo
	,pstyp VARCHAR(5)   ENCODE lzo
	,knttp VARCHAR(5)   ENCODE lzo
	,kzvbr VARCHAR(5)   ENCODE lzo
	,vrtkz VARCHAR(5)   ENCODE lzo
	,twrkz VARCHAR(5)   ENCODE lzo
	,wepos VARCHAR(5)   ENCODE lzo
	,weunb VARCHAR(5)   ENCODE lzo
	,repos VARCHAR(5)   ENCODE lzo
	,webre VARCHAR(5)   ENCODE lzo
	,kzabs VARCHAR(5)   ENCODE lzo
	,labnr VARCHAR(20)   ENCODE lzo
	,konnr VARCHAR(10)   ENCODE lzo
	,ktpnr NUMERIC(38,10)   ENCODE az64
	,abdat VARCHAR(10)   ENCODE lzo
	,abftz NUMERIC(38,10)   ENCODE az64
	,etfz1 NUMERIC(38,10)   ENCODE az64
	,etfz2 NUMERIC(38,10)   ENCODE az64
	,kzstu VARCHAR(5)   ENCODE lzo
	,notkz VARCHAR(5)   ENCODE lzo
	,lmein VARCHAR(5)   ENCODE lzo
	,evers VARCHAR(5)   ENCODE lzo
	,zwert NUMERIC(38,10)   ENCODE az64
	,navnw NUMERIC(38,10)   ENCODE az64
	,abmng NUMERIC(38,10)   ENCODE az64
	,prdat VARCHAR(10)   ENCODE lzo
	,bstyp VARCHAR(5)   ENCODE lzo
	,effwr NUMERIC(38,10)   ENCODE az64
	,xoblr VARCHAR(5)   ENCODE lzo
	,kunnr VARCHAR(10)   ENCODE lzo
	,adrnr VARCHAR(10)   ENCODE lzo
	,ekkol VARCHAR(4)   ENCODE lzo
	,sktof VARCHAR(5)   ENCODE lzo
	,stafo VARCHAR(6)   ENCODE lzo
	,plifz NUMERIC(38,10)   ENCODE az64
	,ntgew NUMERIC(38,10)   ENCODE az64
	,gewei VARCHAR(5)   ENCODE lzo
	,txjcd VARCHAR(15)   ENCODE lzo
	,etdrk VARCHAR(5)   ENCODE lzo
	,sobkz VARCHAR(5)   ENCODE lzo
	,arsnr NUMERIC(38,10)   ENCODE az64
	,arsps NUMERIC(38,10)   ENCODE az64
	,insnc VARCHAR(5)   ENCODE lzo
	,ssqss VARCHAR(8)   ENCODE lzo
	,zgtyp VARCHAR(4)   ENCODE lzo
	,ean11 VARCHAR(18)   ENCODE lzo
	,bstae VARCHAR(4)   ENCODE lzo
	,revlv VARCHAR(5)   ENCODE lzo
	,geber VARCHAR(10)   ENCODE lzo
	,fistl VARCHAR(16)   ENCODE lzo
	,fipos VARCHAR(14)   ENCODE lzo
	,ko_gsber VARCHAR(4)   ENCODE lzo
	,ko_pargb VARCHAR(4)   ENCODE lzo
	,ko_prctr VARCHAR(10)   ENCODE lzo
	,ko_pprctr VARCHAR(10)   ENCODE lzo
	,meprf VARCHAR(5)   ENCODE lzo
	,brgew NUMERIC(38,10)   ENCODE az64
	,volum NUMERIC(38,10)   ENCODE az64
	,voleh VARCHAR(5)   ENCODE lzo
	,inco1 VARCHAR(5)   ENCODE lzo
	,inco2 VARCHAR(28)   ENCODE lzo
	,vorab VARCHAR(5)   ENCODE lzo
	,kolif VARCHAR(10)   ENCODE lzo
	,ltsnr VARCHAR(6)   ENCODE lzo
	,packno NUMERIC(38,10)   ENCODE az64
	,fplnr VARCHAR(10)   ENCODE lzo
	,gnetwr NUMERIC(38,10)   ENCODE az64
	,stapo VARCHAR(5)   ENCODE lzo
	,uebpo NUMERIC(38,10)   ENCODE az64
	,lewed VARCHAR(10)   ENCODE lzo
	,emlif VARCHAR(10)   ENCODE lzo
	,lblkz VARCHAR(5)   ENCODE lzo
	,satnr VARCHAR(40)   ENCODE lzo
	,attyp VARCHAR(5)   ENCODE lzo
	,vsart VARCHAR(5)   ENCODE lzo
	,handoverloc VARCHAR(10)   ENCODE lzo
	,kanba VARCHAR(5)   ENCODE lzo
	,adrn2 VARCHAR(10)   ENCODE lzo
	,cuobj NUMERIC(38,10)   ENCODE az64
	,xersy VARCHAR(5)   ENCODE lzo
	,eildt VARCHAR(10)   ENCODE lzo
	,drdat VARCHAR(10)   ENCODE lzo
	,druhr VARCHAR(24)   ENCODE lzo
	,drunr NUMERIC(38,10)   ENCODE az64
	,aktnr VARCHAR(10)   ENCODE lzo
	,abeln VARCHAR(10)   ENCODE lzo
	,abelp NUMERIC(38,10)   ENCODE az64
	,anzpu NUMERIC(38,10)   ENCODE az64
	,punei VARCHAR(5)   ENCODE lzo
	,saiso VARCHAR(5)   ENCODE lzo
	,saisj VARCHAR(5)   ENCODE lzo
	,ebon2 VARCHAR(5)   ENCODE lzo
	,ebon3 VARCHAR(5)   ENCODE lzo
	,ebonf VARCHAR(5)   ENCODE lzo
	,mlmaa VARCHAR(5)   ENCODE lzo
	,mhdrz NUMERIC(38,10)   ENCODE az64
	,anfnr VARCHAR(10)   ENCODE lzo
	,anfps NUMERIC(38,10)   ENCODE az64
	,kzkfg VARCHAR(5)   ENCODE lzo
	,usequ VARCHAR(5)   ENCODE lzo
	,umsok VARCHAR(5)   ENCODE lzo
	,banfn VARCHAR(10)   ENCODE lzo
	,bnfpo NUMERIC(38,10)   ENCODE az64
	,mtart VARCHAR(4)   ENCODE lzo
	,uptyp VARCHAR(5)   ENCODE lzo
	,upvor VARCHAR(5)   ENCODE lzo
	,kzwi1 NUMERIC(38,10)   ENCODE az64
	,kzwi2 NUMERIC(38,10)   ENCODE az64
	,kzwi3 NUMERIC(38,10)   ENCODE az64
	,kzwi4 NUMERIC(38,10)   ENCODE az64
	,kzwi5 NUMERIC(38,10)   ENCODE az64
	,kzwi6 NUMERIC(38,10)   ENCODE az64
	,sikgr VARCHAR(5)   ENCODE lzo
	,mfzhi NUMERIC(38,10)   ENCODE az64
	,ffzhi NUMERIC(38,10)   ENCODE az64
	,retpo VARCHAR(5)   ENCODE lzo
	,aurel VARCHAR(5)   ENCODE lzo
	,bsgru VARCHAR(5)   ENCODE lzo
	,lfret VARCHAR(4)   ENCODE lzo
	,mfrgr VARCHAR(8)   ENCODE lzo
	,nrfhg VARCHAR(5)   ENCODE lzo
	,j_1bnbm VARCHAR(16)   ENCODE lzo
	,j_1bmatuse VARCHAR(5)   ENCODE lzo
	,j_1bmatorg VARCHAR(5)   ENCODE lzo
	,j_1bownpro VARCHAR(5)   ENCODE lzo
	,j_1bindust VARCHAR(5)   ENCODE lzo
	,abueb VARCHAR(4)   ENCODE lzo
	,nlabd VARCHAR(10)   ENCODE lzo
	,nfabd VARCHAR(10)   ENCODE lzo
	,kzbws VARCHAR(5)   ENCODE lzo
	,bonba NUMERIC(38,10)   ENCODE az64
	,fabkz VARCHAR(5)   ENCODE lzo
	,j_1aindxp VARCHAR(5)   ENCODE lzo
	,j_1aidatep VARCHAR(10)   ENCODE lzo
	,mprof VARCHAR(4)   ENCODE lzo
	,eglkz VARCHAR(5)   ENCODE lzo
	,kztlf VARCHAR(5)   ENCODE lzo
	,kzfme VARCHAR(5)   ENCODE lzo
	,rdprf VARCHAR(4)   ENCODE lzo
	,techs VARCHAR(12)   ENCODE lzo
	,chg_srv VARCHAR(5)   ENCODE lzo
	,chg_fplnr VARCHAR(5)   ENCODE lzo
	,mfrpn VARCHAR(40)   ENCODE lzo
	,mfrnr VARCHAR(10)   ENCODE lzo
	,emnfr VARCHAR(10)   ENCODE lzo
	,novet VARCHAR(5)   ENCODE lzo
	,afnam VARCHAR(12)   ENCODE lzo
	,tzonrc VARCHAR(6)   ENCODE lzo
	,iprkz VARCHAR(5)   ENCODE lzo
	,lebre VARCHAR(5)   ENCODE lzo
	,berid VARCHAR(10)   ENCODE lzo
	,xconditions VARCHAR(5)   ENCODE lzo
	,apoms VARCHAR(5)   ENCODE lzo
	,ccomp VARCHAR(5)   ENCODE lzo
	,grant_nbr VARCHAR(20)   ENCODE lzo
	,fkber VARCHAR(16)   ENCODE lzo
	,status VARCHAR(1)   ENCODE lzo
	,reslo VARCHAR(4)   ENCODE lzo
	,kblnr VARCHAR(10)   ENCODE lzo
	,kblpos NUMERIC(38,10)   ENCODE az64
	,weora VARCHAR(5)   ENCODE lzo
	,srv_bas_com VARCHAR(5)   ENCODE lzo
	,prio_urg NUMERIC(38,10)   ENCODE az64
	,prio_req NUMERIC(38,10)   ENCODE az64
	,empst VARCHAR(25)   ENCODE lzo
	,diff_invoice VARCHAR(5)   ENCODE lzo
	,trmrisk_relevant VARCHAR(5)   ENCODE lzo
	,spe_abgru VARCHAR(5)   ENCODE lzo
	,spe_crm_so VARCHAR(10)   ENCODE lzo
	,spe_crm_so_item NUMERIC(38,10)   ENCODE az64
	,spe_crm_ref_so VARCHAR(35)   ENCODE lzo
	,spe_crm_ref_item VARCHAR(6)   ENCODE lzo
	,spe_crm_fkrel VARCHAR(5)   ENCODE lzo
	,spe_chng_sys VARCHAR(5)   ENCODE lzo
	,spe_insmk_src VARCHAR(5)   ENCODE lzo
	,spe_cq_ctrltype VARCHAR(5)   ENCODE lzo
	,spe_cq_nocq VARCHAR(5)   ENCODE lzo
	,reason_code VARCHAR(4)   ENCODE lzo
	,cqu_sar NUMERIC(38,10)   ENCODE az64
	,anzsn INTEGER   ENCODE az64
	,spe_ewm_dtc VARCHAR(5)   ENCODE lzo
	,exlin VARCHAR(40)   ENCODE lzo
	,exsnr NUMERIC(38,10)   ENCODE az64
	,ehtyp VARCHAR(4)   ENCODE lzo
	,retpc NUMERIC(38,10)   ENCODE az64
	,dptyp VARCHAR(4)   ENCODE lzo
	,dppct NUMERIC(38,10)   ENCODE az64
	,dpamt NUMERIC(38,10)   ENCODE az64
	,dpdat VARCHAR(10)   ENCODE lzo
	,fls_rsto VARCHAR(5)   ENCODE lzo
	,ext_rfx_number VARCHAR(35)   ENCODE lzo
	,ext_rfx_item VARCHAR(10)   ENCODE lzo
	,ext_rfx_system VARCHAR(10)   ENCODE lzo
	,srm_contract_id VARCHAR(10)   ENCODE lzo
	,srm_contract_itm NUMERIC(38,10)   ENCODE az64
	,blk_reason_id VARCHAR(4)   ENCODE lzo
	,blk_reason_txt VARCHAR(40)   ENCODE lzo
	,itcons VARCHAR(5)   ENCODE lzo
	,fixmg VARCHAR(5)   ENCODE lzo
	,wabwe VARCHAR(5)   ENCODE lzo
	,cmpl_dlv_itm VARCHAR(5)   ENCODE lzo
	,inco2_l VARCHAR(70)   ENCODE lzo
	,inco3_l VARCHAR(70)   ENCODE lzo
	,stawn VARCHAR(30)   ENCODE lzo
	,isvco VARCHAR(30)   ENCODE lzo
	,grwrt NUMERIC(38,10)   ENCODE az64
	,serviceperformer VARCHAR(10)   ENCODE lzo
	,producttype VARCHAR(5)   ENCODE lzo
	,requestforquotation VARCHAR(10)   ENCODE lzo
	,requestforquotationitem NUMERIC(38,10)   ENCODE az64
	,tc_aut_det VARCHAR(5)   ENCODE lzo
	,manual_tc_reason VARCHAR(5)   ENCODE lzo
	,fiscal_incentive VARCHAR(4)   ENCODE lzo
	,tax_subject_st VARCHAR(5)   ENCODE lzo
	,fiscal_incentive_id VARCHAR(4)   ENCODE lzo
	,sf_txjcd VARCHAR(15)   ENCODE lzo
	,dummy_ekpo_incl_eew_ps VARCHAR(5)   ENCODE lzo
	,expected_value NUMERIC(38,10)   ENCODE az64
	,limit_amount NUMERIC(38,10)   ENCODE az64
	,enh_date1 VARCHAR(10)   ENCODE lzo
	,enh_date2 VARCHAR(10)   ENCODE lzo
	,enh_percent NUMERIC(38,10)   ENCODE az64
	,enh_numc1 NUMERIC(38,10)   ENCODE az64
	,dataaging VARCHAR(10)   ENCODE lzo
	,bev1_negen_item VARCHAR(5)   ENCODE lzo
	,bev1_nedepfree VARCHAR(5)   ENCODE lzo
	,bev1_nestruccat VARCHAR(5)   ENCODE lzo
	,advcode VARCHAR(5)   ENCODE lzo
	,budget_pd VARCHAR(10)   ENCODE lzo
	,excpe NUMERIC(38,10)   ENCODE az64
	,fmfgus_key VARCHAR(22)   ENCODE lzo
	,iuid_relevant VARCHAR(5)   ENCODE lzo
	,mrpind VARCHAR(5)   ENCODE lzo
	,sgt_scat VARCHAR(40)   ENCODE lzo
	,sgt_rcat VARCHAR(40)   ENCODE lzo
	,tms_ref_uuid VARCHAR(22)   ENCODE lzo
	,wrf_charstc1 VARCHAR(18)   ENCODE lzo
	,wrf_charstc2 VARCHAR(18)   ENCODE lzo
	,wrf_charstc3 VARCHAR(18)   ENCODE lzo
	,refsite VARCHAR(4)   ENCODE lzo
	,zapcgk NUMERIC(38,10)   ENCODE az64
	,apcgk_extend NUMERIC(38,10)   ENCODE az64
	,zbas_date VARCHAR(10)   ENCODE lzo
	,zadattyp VARCHAR(5)   ENCODE lzo
	,zstart_dat VARCHAR(10)   ENCODE lzo
	,z_dev NUMERIC(38,10)   ENCODE az64
	,zindanx VARCHAR(5)   ENCODE lzo
	,zlimit_dat VARCHAR(10)   ENCODE lzo
	,numerator VARCHAR(20)   ENCODE lzo
	,hashcal_bdat VARCHAR(10)   ENCODE lzo
	,hashcal VARCHAR(5)   ENCODE lzo
	,negative VARCHAR(5)   ENCODE lzo
	,hashcal_exists VARCHAR(4)   ENCODE lzo
	,known_index VARCHAR(5)   ENCODE lzo
	,sapmp_gpose NUMERIC(38,10)   ENCODE az64
	,angpn NUMERIC(38,10)   ENCODE az64
	,admoi VARCHAR(4)   ENCODE lzo
	,adpri VARCHAR(5)   ENCODE lzo
	,lprio NUMERIC(38,10)   ENCODE az64
	,adacn VARCHAR(10)   ENCODE lzo
	,afpnr NUMERIC(38,10)   ENCODE az64
	,bsark VARCHAR(5)   ENCODE lzo
	,audat VARCHAR(10)   ENCODE lzo
	,angnr VARCHAR(20)   ENCODE lzo
	,pnstat VARCHAR(5)   ENCODE lzo
	,addns VARCHAR(5)   ENCODE lzo
	,serru VARCHAR(5)   ENCODE lzo
	,sernp VARCHAR(4)   ENCODE lzo
	,disub_sobkz VARCHAR(5)   ENCODE lzo
	,disub_pspnr NUMERIC(38,10)   ENCODE az64
	,disub_kunnr VARCHAR(10)   ENCODE lzo
	,disub_vbeln VARCHAR(10)   ENCODE lzo
	,disub_posnr NUMERIC(38,10)   ENCODE az64
	,disub_owner VARCHAR(10)   ENCODE lzo
	,fsh_season_year VARCHAR(4)   ENCODE lzo
	,fsh_season VARCHAR(4)   ENCODE lzo
	,fsh_collection VARCHAR(5)   ENCODE lzo
	,fsh_theme VARCHAR(4)   ENCODE lzo
	,fsh_atp_date VARCHAR(10)   ENCODE lzo
	,fsh_vas_rel VARCHAR(5)   ENCODE lzo
	,fsh_vas_prnt_id NUMERIC(38,10)   ENCODE az64
	,fsh_transaction VARCHAR(10)   ENCODE lzo
	,fsh_item_group NUMERIC(38,10)   ENCODE az64
	,fsh_item NUMERIC(38,10)   ENCODE az64
	,fsh_ss VARCHAR(5)   ENCODE lzo
	,fsh_grid_cond_rec VARCHAR(32)   ENCODE lzo
	,fsh_psm_pfm_split VARCHAR(15)   ENCODE lzo
	,cnfm_qty NUMERIC(38,10)   ENCODE az64
	,stpac VARCHAR(5)   ENCODE lzo
	,lgbzo VARCHAR(10)   ENCODE lzo
	,lgbzo_b VARCHAR(10)   ENCODE lzo
	,addrnum VARCHAR(10)   ENCODE lzo
	,consnum NUMERIC(38,10)   ENCODE az64
	,borgr_miss VARCHAR(5)   ENCODE lzo
	,dep_id VARCHAR(12)   ENCODE lzo
	,belnr VARCHAR(10)   ENCODE lzo
	,kblpos_cab NUMERIC(38,10)   ENCODE az64
	,kblnr_comp VARCHAR(10)   ENCODE lzo
	,kblpos_comp NUMERIC(38,10)   ENCODE az64
	,wbs_element NUMERIC(38,10)   ENCODE az64
	,ref_item NUMERIC(38,10)   ENCODE az64
	,source_id VARCHAR(3)   ENCODE lzo
	,source_key VARCHAR(32)   ENCODE lzo
	,put_back VARCHAR(5)   ENCODE lzo
	,pol_id VARCHAR(10)   ENCODE lzo
	,cons_order VARCHAR(5)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.sc1_test_current owner to base_admin;


-- bods.shp_tb_litm_current definition

-- Drop table

-- DROP TABLE bods.shp_tb_litm_current;

--DROP TABLE bods.shp_tb_litm_current;
CREATE TABLE IF NOT EXISTS bods.shp_tb_litm_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id BIGINT   ENCODE az64
	,ledger VARCHAR(65535)   ENCODE lzo
	,companycode VARCHAR(65535)   ENCODE lzo
	,fiscalyear NUMERIC(38,10)   ENCODE az64
	,fiscper VARCHAR(65535)   ENCODE lzo
	,docnum VARCHAR(65535)   ENCODE lzo
	,ledgerpostingitem VARCHAR(65535)   ENCODE lzo
	,country VARCHAR(65535)   ENCODE lzo
	,doctype VARCHAR(65535)   ENCODE lzo
	,glaccount VARCHAR(65535)   ENCODE lzo
	,costcenterhier VARCHAR(65535)   ENCODE lzo
	,costcentername2 VARCHAR(65535)   ENCODE lzo
	,costcentername3 VARCHAR(65535)   ENCODE lzo
	,costcentername4 VARCHAR(65535)   ENCODE lzo
	,tradingpartner VARCHAR(65535)   ENCODE lzo
	,endcustomer VARCHAR(65535)   ENCODE lzo
	,custentattr VARCHAR(65535)   ENCODE lzo
	,customerchannel VARCHAR(65535)   ENCODE lzo
	,materialbrand VARCHAR(65535)   ENCODE lzo
	,"location" VARCHAR(65535)   ENCODE lzo
	,siteid VARCHAR(65535)   ENCODE lzo
	,hfm_entity VARCHAR(65535)   ENCODE lzo
	,hfm_custom1 VARCHAR(65535)   ENCODE lzo
	,controllingarea VARCHAR(65535)   ENCODE lzo
	,profitcenter VARCHAR(65535)   ENCODE lzo
	,transtype VARCHAR(65535)   ENCODE lzo
	,costcenter VARCHAR(65535)   ENCODE lzo
	,functionalarea VARCHAR(65535)   ENCODE lzo
	,itemcategory VARCHAR(65535)   ENCODE lzo
	,plant VARCHAR(65535)   ENCODE lzo
	,postingdate VARCHAR(65535)   ENCODE lzo
	,"assignment" VARCHAR(65535)   ENCODE lzo
	,postingkey VARCHAR(65535)   ENCODE lzo
	,refdoc VARCHAR(65535)   ENCODE lzo
	,accountenteredon NUMERIC(38,10)   ENCODE az64
	,referencetransaction VARCHAR(65535)   ENCODE lzo
	,lineitmnum NUMERIC(38,10)   ENCODE az64
	,materialnum VARCHAR(65535)   ENCODE lzo
	,ordernum VARCHAR(65535)   ENCODE lzo
	,customernum VARCHAR(65535)   ENCODE lzo
	,salesorg VARCHAR(65535)   ENCODE lzo
	,soldtoparty VARCHAR(65535)   ENCODE lzo
	,shiptoparty VARCHAR(65535)   ENCODE lzo
	,payer VARCHAR(65535)   ENCODE lzo
	,distchannel VARCHAR(65535)   ENCODE lzo
	,division VARCHAR(65535)   ENCODE lzo
	,fiscalyearperiod NUMERIC(38,10)   ENCODE az64
	,currency VARCHAR(65535)   ENCODE lzo
	,amount NUMERIC(38,10)   ENCODE az64
	,lastchangedat NUMERIC(38,10)   ENCODE az64
	,int_acct VARCHAR(65535)   ENCODE lzo
	,int_geo VARCHAR(65535)   ENCODE lzo
	,int_chnl VARCHAR(65535)   ENCODE lzo
	,int_chnl_knb1 VARCHAR(65535)   ENCODE lzo
	,act1_description VARCHAR(65535)   ENCODE lzo
	,act2_description VARCHAR(65535)   ENCODE lzo
	,ent1_description VARCHAR(65535)   ENCODE lzo
	,ent2_description VARCHAR(65535)   ENCODE lzo
	,ent3_description VARCHAR(65535)   ENCODE lzo
	,hfm_account VARCHAR(65535)   ENCODE lzo
	,material_division VARCHAR(65535)   ENCODE lzo
	,wrkst VARCHAR(65535)   ENCODE lzo
	,entity_grp VARCHAR(65535)   ENCODE lzo
	,prod_hierarchy VARCHAR(65535)   ENCODE lzo
	,c1_description VARCHAR(65535)   ENCODE lzo
	,costcenter_hier VARCHAR(65535)   ENCODE lzo
	,bw_entity VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.shp_tb_litm_current owner to base_admin;


-- bods.ufida_pl_trans_current definition

-- Drop table

-- DROP TABLE bods.ufida_pl_trans_current;

--DROP TABLE bods.ufida_pl_trans_current;
CREATE TABLE IF NOT EXISTS bods.ufida_pl_trans_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,eventdts VARCHAR(65535)   ENCODE lzo
	,rec_src VARCHAR(65535)   ENCODE lzo
	,row_sqn BIGINT   ENCODE az64
	,hash_full_record VARCHAR(65535)   ENCODE lzo
	,id BIGINT   ENCODE az64
	,fiscper VARCHAR(65535)   ENCODE lzo
	,"year" VARCHAR(65535)   ENCODE lzo
	,period VARCHAR(65535)   ENCODE lzo
	,entity VARCHAR(65535)   ENCODE lzo
	,glaccount_code VARCHAR(65535)   ENCODE lzo
	,glaccount_name VARCHAR(65535)   ENCODE lzo
	,customer_code VARCHAR(65535)   ENCODE lzo
	,department_code VARCHAR(65535)   ENCODE lzo
	,posting_date VARCHAR(65535)   ENCODE lzo
	,person_id VARCHAR(65535)   ENCODE lzo
	,currency_code VARCHAR(65535)   ENCODE lzo
	,vouchno VARCHAR(65535)   ENCODE lzo
	,product_code VARCHAR(65535)   ENCODE lzo
	,cdccode VARCHAR(65535)   ENCODE lzo
	,gemcusdchod VARCHAR(65535)   ENCODE lzo
	,gppport VARCHAR(65535)   ENCODE lzo
	,amount NUMERIC(38,10)   ENCODE az64
	,amount_rmb NUMERIC(38,10)   ENCODE az64
	,bar_entity VARCHAR(65535)   ENCODE lzo
	,bar_acct VARCHAR(65535)   ENCODE lzo
	,bar_function VARCHAR(65535)   ENCODE lzo
	,bar_custno VARCHAR(65535)   ENCODE lzo
	,bar_shipto VARCHAR(65535)   ENCODE lzo
	,bar_product VARCHAR(65535)   ENCODE lzo
	,bar_brand VARCHAR(65535)   ENCODE lzo
	,bar_scenario VARCHAR(65535)   ENCODE lzo
	,bar_year VARCHAR(65535)   ENCODE lzo
	,bar_period VARCHAR(65535)   ENCODE lzo
	,bar_currtype VARCHAR(65535)   ENCODE lzo
	,bar_bu VARCHAR(65535)   ENCODE lzo
	,bar_amt NUMERIC(38,10)   ENCODE az64
	,product_desc VARCHAR(65535)   ENCODE lzo
	,product_qty NUMERIC(38,10)   ENCODE az64
	,runid VARCHAR(65535)   ENCODE lzo
	,loaddatetime VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE bods.ufida_pl_trans_current owner to base_admin;