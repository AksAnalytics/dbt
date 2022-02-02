CREATE OR REPLACE PROCEDURE public.sp_sbd_dm_customer_master_insert()
	LANGUAGE plpgsql
AS $$
	
  BEGIN
  
	TRUNCATE TABLE global_pl.customer_master;
	
	INSERT INTO global_pl.customer_master
		(ERP_CUSTOMER_NUMBER ,ERP_CUSTOMER_ADDRESS_CODE ,ERP_CUSTOMER_INDUSTRY_CODE_1 ,ERP_CUSTOMER_INDUSTRY_CODE_2 ,ERP_CUSTOMER_INDUSTRY_CODE_3 ,ERP_CUSTOMER_INDUSTRY_CODE_4 ,ERP_CUSTOMER_INDUSTRY_CODE_5 ,ERP_CUSTOMER_INDUSTRY_KEY ,ERP_CUSTOMER_CITY_CODE ,ERP_CUSTOMER_COUNTY_CODE ,ERP_CUSTOMER_COUNTRY ,ERP_CUSTOMER_NAME ,ERP_CUSTOMER_CITY ,ERP_CUSTOMER_DISTRICT ,ERP_CUSTOMER_PO_BOX ,ERP_CUSTOMER_PO_BOX_POSTAL_CODE ,ERP_CUSTOMER_POSTAL_CODE ,ERP_CUSTOMER_REGION ,ERP_CUSTOMER_REGIONAL_MARKET ,ERP_SOURCE ,ERP_CUSTOMER_ADDRESS ,ETL_CRTE_USER ,ETL_CRTE_TS)
		(
		Select KUNNR ,ADRNR ,BRAN1 ,BRAN2 ,BRAN3 ,BRAN4 ,BRAN5 ,BRSCH ,CITYC ,COUNC ,LAND1 ,NAME1 ,ORT01 ,ORT02 ,PFACH ,PSTL2 ,PSTLZ ,REGIO ,RPMKR ,'C11',STRAS,'etl_user',to_date(GETDATE(),'yyyyMMdd') FROM  bods.c11_0customer_attr_current 
		UNION ALL
		Select KUNNR ,ADRNR ,BRAN1 ,BRAN2 ,BRAN3 ,BRAN4 ,BRAN5 ,BRSCH ,CITYC ,COUNC ,LAND1 ,NAME1 ,ORT01 ,ORT02 ,PFACH ,PSTL2 ,PSTLZ ,REGIO ,RPMKR ,'E03' ,STRAS,'etl_user',to_date(GETDATE(),'yyyyMMdd') from  bods.e03_0customer_attr_current
		UNION ALL
		Select KUNNR ,ADRNR ,BRAN1 ,BRAN2 ,BRAN3 ,BRAN4 ,BRAN5 ,BRSCH ,CITYC ,COUNC ,LAND1 ,NAME1 ,ORT01 ,ORT02 ,PFACH ,PSTL2 ,PSTLZ ,REGIO ,RPMKR ,'P10' ,STRAS,'etl_user',to_date(GETDATE(),'yyyyMMdd') FROM bods.extr_p10_0customer_attr_current 
		UNION ALL
		Select KUNNR ,ADRNR ,BRAN1 ,BRAN2 ,BRAN3 ,BRAN4 ,BRAN5 ,BRSCH ,CITYC ,COUNC ,LAND1 ,NAME1 ,ORT01 ,ORT02 ,PFACH ,PSTL2 ,PSTLZ ,REGIO ,RPMKR ,'SHP' ,STRAS,'etl_user',to_date(GETDATE(),'yyyyMMdd') FROM bods.extr_shp_customer_attr_current
		);

  RAISE INFO 'Ran SP Successfull';
  
END;

$$
;

CREATE OR REPLACE PROCEDURE public.sp_sbd_dm_material_master_insert()
	LANGUAGE plpgsql
AS $$
	
  BEGIN
	INSERT INTO global_pl.material_master_temp
		(ERP_MATERIAL_NUMBER ,ERP_MATERIAL_DESCRIPTION ,ERP_MATERIAL_CATEGORY ,ERP_CONTAINER_REQUIREMENTS ,ERP_GENERIC_MATERIAL_WITH_LOGISTICAL_VARIANTS ,ERP_OLD_MATERIAL_NUMBER ,ERP_BRAND ,ERP_WIDTH ,ERP_GROSS_WEIGHT ,ERP_PURCHASE_ORDER_UoM ,ERP_SOURCE_OF_SUPPLY ,ERP_PROCUREMENT_RULE ,ERP_CAD_INDICATOR ,ERP_QUALITY_CONVERSION_METHOD ,ERP_MATERIAL_COMPLETION_LEVEL ,ERP_INTERNAL_OBJECT_NUMBER ,ERP_VALID_FROM_DATE ,ERP_EAN_UPC ,ERP_PURHCASING_VALUE_KEY ,ERP_UNIT_OF_WEIGHT_PACKAGING ,ERP_ALLOWED_PACKAGING_WEIGHT ,ERP_VOLUME_UNIT ,ERP_ALLOWED_PACKAGING_VOLUME ,ERP_WEIGHT_UNIT ,ERP_SIZE_DIMENSIONS ,ERP_HEIGHT ,ERP_MATERIAL_GROUP ,ERP_INDUSTRY_SECTOR ,ERP_MATERIAL_TYPE ,ERP_NET_WEIGHT ,ERP_PRODUCT_HIERARCHY ,ERP_DIVISION ,ERP_HAZARDOUS_MATERIAL_NUMBER ,ERP_TRANSPORTATION_GROUP ,ERP_PACKAGING_MATERIAL_TYPE ,ERP_GLOBAL_PRODUCT_HIERARCHY ,ERP_SOURCE ,ETL_CRTE_USER ,ETL_CRTE_TS)
			(
				Select MATNR ,NULL ,ATTYP ,BEHVO ,BFLME ,BISMT ,NULL ,BREIT ,BRGEW ,BSTME ,BWSCL ,BWVOR ,CADKZ ,CMETH ,COMPL ,CUOBF ,DATAB::date ,EAN11 ,EKWSL ,ERGEI ,ERGEW ,ERVOE ,ERVOL ,GEWEI ,GROES ,HOEHE ,MATKL ,MBRSH ,MTART ,NTGEW ,PRDHA ,SPART ,STOFF ,TRAGR ,VHART ,WRKST ,'C11','etl_user',to_date(GETDATE(),'yyyyMMdd') FROM bods.c11_0material_attr_current
				UNION ALL
				Select MATNR ,NULL ,ATTYP ,BEHVO ,BFLME ,BISMT ,NULL ,BREIT ,BRGEW ,BSTME ,BWSCL ,BWVOR ,CADKZ ,CMETH ,COMPL ,CUOBF ,DATAB::date ,EAN11 ,EKWSL ,ERGEI ,ERGEW ,ERVOE ,ERVOL ,GEWEI ,GROES ,HOEHE ,MATKL ,MBRSH ,MTART ,NTGEW ,PRDHA ,SPART ,STOFF ,TRAGR ,VHART ,WRKST ,'E03','etl_user',to_date(GETDATE(),'yyyyMMdd') FROM  bods.e03_0material_attr_current
				UNION ALL
				Select MATNR ,NULL ,ATTYP ,BEHVO ,BFLME ,BISMT ,NULL ,BREIT ,BRGEW ,BSTME ,BWSCL ,BWVOR ,CADKZ ,CMETH ,COMPL ,CUOBF ,DATAB::date ,EAN11 ,EKWSL ,ERGEI ,ERGEW ,ERVOE ,ERVOL ,GEWEI ,GROES ,HOEHE ,MATKL ,MBRSH ,MTART ,NTGEW ,PRDHA ,SPART ,STOFF ,TRAGR ,VHART ,WRKST ,'SHP','etl_user',to_date(GETDATE(),'yyyyMMdd') FROM   bods.extr_shp_material_attr_current
				UNION ALL
				Select MATNR ,NULL ,ATTYP ,BEHVO ,BFLME ,BISMT ,NULL ,BREIT ,BRGEW ,BSTME ,BWSCL ,BWVOR ,CADKZ ,CMETH ,COMPL::numeric(38,10) ,CUOBF ,DATAB::date ,EAN11 ,EKWSL ,ERGEI ,ERGEW ,ERVOE ,ERVOL ,GEWEI ,GROES ,HOEHE ,MATKL ,MBRSH ,MTART ,NTGEW ,PRDHA ,SPART ,STOFF ,TRAGR ,VHART ,WRKST ,'P10','etl_user',to_date(GETDATE(),'yyyyMMdd') FROM  bods.extr_p10_0material_attr_current
				limit 1000
			);

  RAISE INFO 'Ran SP Successfull';
  
END;

$$
;

CREATE OR REPLACE PROCEDURE public.sp_sbd_dm_trans_daily_insert()
	LANGUAGE plpgsql
AS $$
	
  BEGIN
	INSERT INTO  global_pl.pl_trans_fact_temp
		(bar_account  ,bar_amt_lc ,bar_brand ,bar_bu ,bar_currtype ,bar_customer ,bar_entity ,bar_function ,bar_period ,bar_product ,bar_scenario ,bar_shipto ,bar_year ,bar_fiscal_period ,erp_account ,erp_brand_code ,erp_business_area ,erp_company_code ,erp_cost_center ,erp_doc_type ,erp_doc_line_num ,erp_doc_num ,erp_document_text ,erp_vendor ,erp_material ,erp_customer_parent ,erp_posting_date ,erp_quantity ,erp_quantity_uom ,erp_ref_doc_type ,erp_ref_doc_line_num ,erp_ref_doc_num ,erp_profit_center ,erp_sales_group ,erp_sales_office ,erp_customer_ship_to ,erp_customer_sold_to ,erp_plant ,erp_chartaccts, bar_bods_loaddatetime ,bar_bods_record_id ,erp_source ,bar_s_entity_currency ,bar_s_curr_rate_actual ,bar_amt_usd ,etl_crte_ts,etl_crte_user)
		(  
			select t.BAR_ACCT,t.BAR_AMT,t.BAR_BRAND,t.BAR_BU,t.BAR_CURRTYPE,t.BAR_CUSTNO,t.BAR_ENTITY,t.BAR_FUNCTION,t.BAR_PERIOD,t.BAR_PRODUCT,t.BAR_SCENARIO,t.BAR_SHIPTO,t.BAR_YEAR, cast(t.PERIOD as text) as PERIOD,t.ACCT,t.INT_BRANDGRP,NULL,t.COCODE,t.COSTCTR,t.DOCTYPE,t.DOCLINE,t.DOCNO,t.SGTXT,NULL,t.PRODUCT,t.PAYER,t.CPUDT,t.QUANTITY,t.QUANUNIT,t.REFDOCCAT,t.REFITM,t.REFDOC,t.PROFCTR,t.SALESDIV,t.SALESOFF,t.SHIPTOCUST,t.SOLDTOCUST,t.PLANT,t.CHARTACCTS,t.LOADDATETIME,t.ID,'E03',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd,to_date(GETDATE(),'yyyyMMdd'), 'etl_user' from bods.e03_3fi_sl_h1_si_current t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )  where t.id is not null
		);
		
	INSERT INTO  global_pl.pl_trans_fact_temp
		(bar_account  ,bar_amt_lc ,bar_brand ,bar_bu ,bar_currtype ,bar_customer ,bar_entity ,bar_function ,bar_period ,bar_product ,bar_scenario ,bar_shipto ,bar_year ,bar_fiscal_period ,erp_account ,erp_brand_code ,erp_business_area ,erp_company_code ,erp_cost_center ,erp_doc_type ,erp_doc_line_num ,erp_doc_num ,erp_document_text ,erp_vendor ,erp_material ,erp_customer_parent ,erp_posting_date ,erp_quantity ,erp_quantity_uom ,erp_ref_doc_type ,erp_ref_doc_line_num ,erp_ref_doc_num ,erp_profit_center ,erp_sales_group ,erp_sales_office ,erp_customer_ship_to ,erp_customer_sold_to ,erp_plant ,erp_chartaccts, bar_bods_loaddatetime ,bar_bods_record_id ,erp_source ,bar_s_entity_currency ,bar_s_curr_rate_actual ,bar_amt_usd ,etl_crte_ts,etl_crte_user)
		(  
			select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.GLACCOUNT ,t.MATERIALBRAND ,NULL ,t.COMPANYCODE ,t.COSTCENTER ,t.DOCTYPE ,cast(t.LINEITMNUM as text) as LINEITMNUM ,t.DOCNUM ,NULL ,NULL ,t.MATERIALNUM ,t.PAYER ,t.POSTINGDATE ,NULL ,NULL ,NULL ,NULL ,t.REFDOC ,t.PROFITCENTER ,t.SALESORG ,NULL ,t.SHIPTOPARTY ,t.SOLDTOPARTY ,t.PLANT ,NULL ,t.LOADDATETIME ,t.ID,'SHP',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd,to_date(GETDATE(),'yyyyMMdd'), 'etl_user' FROM bods.shp_tb_litm_current t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);
		
	INSERT INTO  global_pl.pl_trans_fact_temp
		(bar_account  ,bar_amt_lc ,bar_brand ,bar_bu ,bar_currtype ,bar_customer ,bar_entity ,bar_function ,bar_period ,bar_product ,bar_scenario ,bar_shipto ,bar_year ,bar_fiscal_period ,erp_account ,erp_brand_code ,erp_business_area ,erp_company_code ,erp_cost_center ,erp_doc_type ,erp_doc_line_num ,erp_doc_num ,erp_document_text ,erp_vendor ,erp_material ,erp_customer_parent ,erp_posting_date ,erp_quantity ,erp_quantity_uom ,erp_ref_doc_type ,erp_ref_doc_line_num ,erp_ref_doc_num ,erp_profit_center ,erp_sales_group ,erp_sales_office ,erp_customer_ship_to ,erp_customer_sold_to ,erp_plant ,erp_chartaccts, bar_bods_loaddatetime ,bar_bods_record_id ,erp_source ,bar_s_entity_currency ,bar_s_curr_rate_actual ,bar_amt_usd ,etl_crte_ts,etl_crte_user)
		( 
			select t.BAR_ACCT, t.BAR_AMT, t.BAR_BRAND, t.BAR_BU, t.BAR_CURRTYPE, t.BAR_CUSTNO, t.BAR_ENTITY, t.BAR_FUNCTION,t.BAR_PERIOD,t.BAR_PRODUCT,t.BAR_SCENARIO,t.BAR_SHIPTO,t.BAR_YEAR,t.FISCPER::text,t.ACCT,t.BRAND_CD,t.BUS_AREA,t.CO_CD,t.COST_CNTR,t.DOCCT,t.DOCLN,t.DOCNR,t.SGTXT,t.LIFNR,t.PROD_CD,t.HIGHER_LVL_CUST,t.CPUDT,t.QUANTITY,t.QUANUNIT,t.REFDOCCT,t.REFDOCLN,t.REFDOCNR,t.PROFIT_CNTR,t.CUST_ACT_GRP,NULL,t.SHIPTO_CUST_NBR,t.CUST_NO,t.WERKS,t.CHARTACCTS,t.LOADDATETIME,t.ID,'P10',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'   from bods.p10_0ec_pca_3_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);

	INSERT INTO  global_pl.pl_trans_fact_temp
		(bar_account  ,bar_amt_lc ,bar_brand ,bar_bu ,bar_currtype ,bar_customer ,bar_entity ,bar_function ,bar_period ,bar_product ,bar_scenario ,bar_shipto ,bar_year ,bar_fiscal_period ,erp_account ,erp_brand_code ,erp_business_area ,erp_company_code ,erp_cost_center ,erp_doc_type ,erp_doc_line_num ,erp_doc_num ,erp_document_text ,erp_vendor ,erp_material ,erp_customer_parent ,erp_posting_date ,erp_quantity ,erp_quantity_uom ,erp_ref_doc_type ,erp_ref_doc_line_num ,erp_ref_doc_num ,erp_profit_center ,erp_sales_group ,erp_sales_office ,erp_customer_ship_to ,erp_customer_sold_to ,erp_plant ,erp_chartaccts, bar_bods_loaddatetime ,bar_bods_record_id ,erp_source ,bar_s_entity_currency ,bar_s_curr_rate_actual ,bar_amt_usd ,etl_crte_ts,etl_crte_user)
		(  
			select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.ACCT ,t.BRAND_CD ,t.ACCT_UNIT ,t.CO_CD ,t.SUB_ACCT ,t.SYS_CD ,t.POST_DOC_REF_LN_NBR ,t.POST_DOC_REF_NBR ,t.SYS_NAME ,NULL ,t.PROD_CD ,NULL ,t.POST_DTE ,t.QUANTITY::numeric(38,10) ,NULL ,t.SRC_DOC_TYP ,NULL ,t.SRC_DOC_NBR ,NULL ,NULL ,NULL ,NULL ,CUST_NBR ,NULL ,NULL ,t.LOADDATETIME ,t.ID::bigint, 'LAWSON',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'  FROM bods.lawson_mac_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);
						


  RAISE INFO 'Ran SP Successfull';
  
END;

$$
;

CREATE OR REPLACE PROCEDURE public.sp_sbd_dm_trans_monthly_insert()
	LANGUAGE plpgsql
AS $$
	
  BEGIN
	INSERT INTO  global_pl.pl_trans_fact_temp
		(bar_account  ,bar_amt_lc ,bar_brand ,bar_bu ,bar_currtype ,bar_customer ,bar_entity ,bar_function ,bar_period ,bar_product ,bar_scenario ,bar_shipto ,bar_year ,bar_fiscal_period ,erp_account ,erp_brand_code ,erp_business_area ,erp_company_code ,erp_cost_center ,erp_doc_type ,erp_doc_line_num ,erp_doc_num ,erp_document_text ,erp_vendor ,erp_material ,erp_customer_parent ,erp_posting_date ,erp_quantity ,erp_quantity_uom ,erp_ref_doc_type ,erp_ref_doc_line_num ,erp_ref_doc_num ,erp_profit_center ,erp_sales_group ,erp_sales_office ,erp_customer_ship_to ,erp_customer_sold_to ,erp_plant ,erp_chartaccts, bar_bods_loaddatetime ,bar_bods_record_id ,erp_source ,bar_s_entity_currency ,bar_s_curr_rate_actual ,bar_amt_usd ,etl_crte_ts,etl_crte_user)
		(  
			Select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.GL_ACCOUNT ,t.INT_BRANDGRP ,t.SEGMENT ,t.COMPANY_ID ,t.COST_CENTER ,NULL ,t.JOURNAL_ENTRY_ITEM ,t.JOURNAL_ENTRY ,NULL ,NULL ,t.PRODUCT_ID ,t.PAYER ,t.POSTING_DATE ,t.INVOICED_QUANTITY ,NULL ,t.DOCUMENT_TYPE ,NULL ,t.SOURCE_DOCUMENT_ID ,t.PROFIT_CENTER ,t.CUSTOMER_CHANNEL_CODE ,NULL ,t.SHIP_TO_CUSTOMER ,t.BILL_TO_CUSTOMER ,NULL ,NULL ,t.LOADDATETIME ,t.ID ,'BYD',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'     from bods.byd_pl_trans_archive_current t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )  where t.id is not null
		);
		
INSERT INTO  global_pl.pl_trans_fact_temp
	(bar_account  ,bar_amt_lc ,bar_brand ,bar_bu ,bar_currtype ,bar_customer ,bar_entity ,bar_function ,bar_period ,bar_product ,bar_scenario ,bar_shipto ,bar_year ,bar_fiscal_period ,erp_account ,erp_brand_code ,erp_business_area ,erp_company_code ,erp_cost_center ,erp_doc_type ,erp_doc_line_num ,erp_doc_num ,erp_document_text ,erp_vendor ,erp_material ,erp_customer_parent ,erp_posting_date ,erp_quantity ,erp_quantity_uom ,erp_ref_doc_type ,erp_ref_doc_line_num ,erp_ref_doc_num ,erp_profit_center ,erp_sales_group ,erp_sales_office ,erp_customer_ship_to ,erp_customer_sold_to ,erp_plant ,erp_chartaccts, bar_bods_loaddatetime ,bar_bods_record_id ,erp_source ,bar_s_entity_currency ,bar_s_curr_rate_actual ,bar_amt_usd ,etl_crte_ts,etl_crte_user)
	(  
		Select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,NULL ,t.ACCT ,NULL ,t.CUSTOM1 ,t.ENTITY ,t.CUSTOM2 ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.LOADDATETIME ,t.ID,'HFM',b.bar_entity_currency ,t.bar_amt as currency_rate_actual,t.bar_amt ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'  from bods.hfm_vw_hfm_actual_trans_current t  LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity 
	);
		
	INSERT INTO  global_pl.pl_trans_fact_temp
		(bar_account  ,bar_amt_lc ,bar_brand ,bar_bu ,bar_currtype ,bar_customer ,bar_entity ,bar_function ,bar_period ,bar_product ,bar_scenario ,bar_shipto ,bar_year ,bar_fiscal_period ,erp_account ,erp_brand_code ,erp_business_area ,erp_company_code ,erp_cost_center ,erp_doc_type ,erp_doc_line_num ,erp_doc_num ,erp_document_text ,erp_vendor ,erp_material ,erp_customer_parent ,erp_posting_date ,erp_quantity ,erp_quantity_uom ,erp_ref_doc_type ,erp_ref_doc_line_num ,erp_ref_doc_num ,erp_profit_center ,erp_sales_group ,erp_sales_office ,erp_customer_ship_to ,erp_customer_sold_to ,erp_plant ,erp_chartaccts, bar_bods_loaddatetime ,bar_bods_record_id ,erp_source ,bar_s_entity_currency ,bar_s_curr_rate_actual ,bar_amt_usd ,etl_crte_ts,etl_crte_user)
		( 
			select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.ACCT ,NULL ,t.DEPARTMENT ,t.COCODE ,NULL ,t.DOCUMENT_TYPE ,NULL ,t.DOCUMENT_NO ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.TRANSACTION_NO ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.LOADDATETIME ,t.ID,'NAVSTORAGE',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'  FROM bods.nav_storage_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);

	INSERT INTO  global_pl.pl_trans_fact_temp
		(bar_account  ,bar_amt_lc ,bar_brand ,bar_bu ,bar_currtype ,bar_customer ,bar_entity ,bar_function ,bar_period ,bar_product ,bar_scenario ,bar_shipto ,bar_year ,bar_fiscal_period ,erp_account ,erp_brand_code ,erp_business_area ,erp_company_code ,erp_cost_center ,erp_doc_type ,erp_doc_line_num ,erp_doc_num ,erp_document_text ,erp_vendor ,erp_material ,erp_customer_parent ,erp_posting_date ,erp_quantity ,erp_quantity_uom ,erp_ref_doc_type ,erp_ref_doc_line_num ,erp_ref_doc_num ,erp_profit_center ,erp_sales_group ,erp_sales_office ,erp_customer_ship_to ,erp_customer_sold_to ,erp_plant ,erp_chartaccts, bar_bods_loaddatetime ,bar_bods_record_id ,erp_source ,bar_s_entity_currency ,bar_s_curr_rate_actual ,bar_amt_usd ,etl_crte_ts,etl_crte_user)
		(  
			select t.BAR_ACCT,t. BAR_AMT,t. BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.ACCT ,NULL ,NULL ,t.CO_CD ,NULL ,NULL ,NULL ,t.TXN_ID ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.BAR_CUSTNO ,NULL ,NULL ,t.LOADDATETIME ,t.ID, 'NAVEUR',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'  FROM bods.nav_eur_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);
			

	INSERT INTO  global_pl.pl_trans_fact_temp
		(bar_account  ,bar_amt_lc ,bar_brand ,bar_bu ,bar_currtype ,bar_customer ,bar_entity ,bar_function ,bar_period ,bar_product ,bar_scenario ,bar_shipto ,bar_year ,bar_fiscal_period ,erp_account ,erp_brand_code ,erp_business_area ,erp_company_code ,erp_cost_center ,erp_doc_type ,erp_doc_line_num ,erp_doc_num ,erp_document_text ,erp_vendor ,erp_material ,erp_customer_parent ,erp_posting_date ,erp_quantity ,erp_quantity_uom ,erp_ref_doc_type ,erp_ref_doc_line_num ,erp_ref_doc_num ,erp_profit_center ,erp_sales_group ,erp_sales_office ,erp_customer_ship_to ,erp_customer_sold_to ,erp_plant ,erp_chartaccts, bar_bods_loaddatetime ,bar_bods_record_id ,erp_source ,bar_s_entity_currency ,bar_s_curr_rate_actual ,bar_amt_usd ,etl_crte_ts,etl_crte_user)
		(  
			Select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.GLACCOUNT_CODE ,NULL ,t.DEPARTMENT_CODE ,t.CDCCODE ,NULL ,NULL ,NULL ,t.VOUCHNO ,NULL ,NULL ,t.PRODUCT_CODE ,NULL ,t.POSTING_DATE ,t.PRODUCT_QTY ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.CUSTOMER_CODE ,NULL ,NULL ,t.LOADDATETIME ,t.ID,'UFIDA',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user' FROM bods.ufida_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);

	INSERT INTO  global_pl.pl_trans_fact_temp
		(bar_account  ,bar_amt_lc ,bar_brand ,bar_bu ,bar_currtype ,bar_customer ,bar_entity ,bar_function ,bar_period ,bar_product ,bar_scenario ,bar_shipto ,bar_year ,bar_fiscal_period ,erp_account ,erp_brand_code ,erp_business_area ,erp_company_code ,erp_cost_center ,erp_doc_type ,erp_doc_line_num ,erp_doc_num ,erp_document_text ,erp_vendor ,erp_material ,erp_customer_parent ,erp_posting_date ,erp_quantity ,erp_quantity_uom ,erp_ref_doc_type ,erp_ref_doc_line_num ,erp_ref_doc_num ,erp_profit_center ,erp_sales_group ,erp_sales_office ,erp_customer_ship_to ,erp_customer_sold_to ,erp_plant ,erp_chartaccts, bar_bods_loaddatetime ,bar_bods_record_id ,erp_source ,bar_s_entity_currency ,bar_s_curr_rate_actual ,bar_amt_usd ,etl_crte_ts,etl_crte_user)
		(  
			Select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.ACCT ,NULL ,t.ICP_CD ,t.CO_CD ,NULL ,NULL ,NULL ,t.TXN_ID ,NULL ,NULL ,NULL ,NULL ,t.POST_DTE ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.LOADDATETIME ,t.ID,'ORCHBGI',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'   FROM bods.orch_bgi_pl_trans_current    t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);

	INSERT INTO  global_pl.pl_trans_fact_temp
		(bar_account  ,bar_amt_lc ,bar_brand ,bar_bu ,bar_currtype ,bar_customer ,bar_entity ,bar_function ,bar_period ,bar_product ,bar_scenario ,bar_shipto ,bar_year ,bar_fiscal_period ,erp_account ,erp_brand_code ,erp_business_area ,erp_company_code ,erp_cost_center ,erp_doc_type ,erp_doc_line_num ,erp_doc_num ,erp_document_text ,erp_vendor ,erp_material ,erp_customer_parent ,erp_posting_date ,erp_quantity ,erp_quantity_uom ,erp_ref_doc_type ,erp_ref_doc_line_num ,erp_ref_doc_num ,erp_profit_center ,erp_sales_group ,erp_sales_office ,erp_customer_ship_to ,erp_customer_sold_to ,erp_plant ,erp_chartaccts, bar_bods_loaddatetime ,bar_bods_record_id ,erp_source ,bar_s_entity_currency ,bar_s_curr_rate_actual ,bar_amt_usd ,etl_crte_ts,etl_crte_user)
		(  
			Select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.ACCT ,NULL ,NULL ,t.CO_CD ,NULL ,NULL ,NULL ,t.TXN_ID ,NULL ,NULL ,NULL ,NULL ,t.POST_DTE ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.LOADDATETIME ,t.ID,'CONT',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'  from bods.cont_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);


	INSERT INTO  global_pl.pl_trans_fact_temp
		(bar_account  ,bar_amt_lc ,bar_brand ,bar_bu ,bar_currtype ,bar_customer ,bar_entity ,bar_function ,bar_period ,bar_product ,bar_scenario ,bar_shipto ,bar_year ,bar_fiscal_period ,erp_account ,erp_brand_code ,erp_business_area ,erp_company_code ,erp_cost_center ,erp_doc_type ,erp_doc_line_num ,erp_doc_num ,erp_document_text ,erp_vendor ,erp_material ,erp_customer_parent ,erp_posting_date ,erp_quantity ,erp_quantity_uom ,erp_ref_doc_type ,erp_ref_doc_line_num ,erp_ref_doc_num ,erp_profit_center ,erp_sales_group ,erp_sales_office ,erp_customer_ship_to ,erp_customer_sold_to ,erp_plant ,erp_chartaccts, bar_bods_loaddatetime ,bar_bods_record_id ,erp_source ,bar_s_entity_currency ,bar_s_curr_rate_actual ,bar_amt_usd ,etl_crte_ts,etl_crte_user)
		(  
			Select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.ACCT ,NULL ,t.DEPT ,t.CO_CD ,NULL ,NULL ,NULL ,t.TXN_ID ,NULL ,NULL ,NULL ,NULL ,t.TXN_DTE ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.CUST_NBR ,NULL ,NULL ,t.LOADDATETIME ,t.ID,'MOVEX',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'  from bods.movex_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);		



	INSERT INTO  global_pl.pl_trans_fact_temp
		(bar_account  ,bar_amt_lc ,bar_brand ,bar_bu ,bar_currtype ,bar_customer ,bar_entity ,bar_function ,bar_period ,bar_product ,bar_scenario ,bar_shipto ,bar_year ,bar_fiscal_period ,erp_account ,erp_brand_code ,erp_business_area ,erp_company_code ,erp_cost_center ,erp_doc_type ,erp_doc_line_num ,erp_doc_num ,erp_document_text ,erp_vendor ,erp_material ,erp_customer_parent ,erp_posting_date ,erp_quantity ,erp_quantity_uom ,erp_ref_doc_type ,erp_ref_doc_line_num ,erp_ref_doc_num ,erp_profit_center ,erp_sales_group ,erp_sales_office ,erp_customer_ship_to ,erp_customer_sold_to ,erp_plant ,erp_chartaccts, bar_bods_loaddatetime ,bar_bods_record_id ,erp_source ,bar_s_entity_currency ,bar_s_curr_rate_actual ,bar_amt_usd ,etl_crte_ts,etl_crte_user)
		(  
			select t.ACCOUNT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.ACCOUNT ,NULL ,t.CODE_B ,t.COMPANY ,t.ACCOUNT_GROUP ,t.VOUCHER_TYPE ,NULL ,t.VOUCHER_NO ,REGEXP_REPLACE(TEXT,'[^a-zA-Z0-9\u00E0-\u00FC ]+','') as TEXT ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.LOADDATETIME ,t.ID ,'IFS',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'), 'etl_user'  FROM   bods.ifs_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);		


	INSERT INTO  global_pl.pl_trans_fact_temp
		(bar_account  ,bar_amt_lc ,bar_brand ,bar_bu ,bar_currtype ,bar_customer ,bar_entity ,bar_function ,bar_period ,bar_product ,bar_scenario ,bar_shipto ,bar_year ,bar_fiscal_period ,erp_account ,erp_brand_code ,erp_business_area ,erp_company_code ,erp_cost_center ,erp_doc_type ,erp_doc_line_num ,erp_doc_num ,erp_document_text ,erp_vendor ,erp_material ,erp_customer_parent ,erp_posting_date ,erp_quantity ,erp_quantity_uom ,erp_ref_doc_type ,erp_ref_doc_line_num ,erp_ref_doc_num ,erp_profit_center ,erp_sales_group ,erp_sales_office ,erp_customer_ship_to ,erp_customer_sold_to ,erp_plant ,erp_chartaccts, bar_bods_loaddatetime ,bar_bods_record_id ,erp_source ,bar_s_entity_currency ,bar_s_curr_rate_actual ,bar_amt_usd ,etl_crte_ts,etl_crte_user)
		(  
			select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.GL_ACCOUNT ,NULL ,NULL ,t.COMPANY ,NULL ,t.JOURNAL_CODE ,t.JOURNAL_LINE ,t.JOURNAL_NUM ,REGEXP_REPLACE(DESCRIPTION,'[^a-zA-Z0-9\u00E0-\u00FC ]+','') as DESCRIPTION ,NULL ,NULL ,NULL ,t.POSTED_DATE ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.GROUPID ,NULL ,NULL ,NULL ,NULL ,NULL ,t.LOADDATETIME ,t.ID ,'NELSON',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'), 'etl_user'  FROM   bods.nelson_asmp_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);		


	INSERT INTO  global_pl.pl_trans_fact_temp
		(bar_account  ,bar_amt_lc ,bar_brand ,bar_bu ,bar_currtype ,bar_customer ,bar_entity ,bar_function ,bar_period ,bar_product ,bar_scenario ,bar_shipto ,bar_year ,bar_fiscal_period ,erp_account ,erp_brand_code ,erp_business_area ,erp_company_code ,erp_cost_center ,erp_doc_type ,erp_doc_line_num ,erp_doc_num ,erp_document_text ,erp_vendor ,erp_material ,erp_customer_parent ,erp_posting_date ,erp_quantity ,erp_quantity_uom ,erp_ref_doc_type ,erp_ref_doc_line_num ,erp_ref_doc_num ,erp_profit_center ,erp_sales_group ,erp_sales_office ,erp_customer_ship_to ,erp_customer_sold_to ,erp_plant ,erp_chartaccts, bar_bods_loaddatetime ,bar_bods_record_id ,erp_source ,bar_s_entity_currency ,bar_s_curr_rate_actual ,bar_amt_usd ,etl_crte_ts,etl_crte_user)
		(  
			select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,NULL ,t.BAR_YEAR ,t.FISCPER ,t.ACCOUNT ,NULL ,t.BUSINESS_AREA ,NULL ,t.COST_CENTER ,NULL ,NULL ,NULL ,NULL ,NULL ,t.PRODUCT_CODE ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.PROFIT_CENTER ,NULL ,NULL ,NULL ,t.CUST_NUM ,NULL ,NULL ,t.LOADDATETIME ,t.ID ,'AGRESSO',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'  FROM   bods.agresso_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);		


	INSERT INTO  global_pl.pl_trans_fact_temp
		(bar_account  ,bar_amt_lc ,bar_brand ,bar_bu ,bar_currtype ,bar_customer ,bar_entity ,bar_function ,bar_period ,bar_product ,bar_scenario ,bar_shipto ,bar_year ,bar_fiscal_period ,erp_account ,erp_brand_code ,erp_business_area ,erp_company_code ,erp_cost_center ,erp_doc_type ,erp_doc_line_num ,erp_doc_num ,erp_document_text ,erp_vendor ,erp_material ,erp_customer_parent ,erp_posting_date ,erp_quantity ,erp_quantity_uom ,erp_ref_doc_type ,erp_ref_doc_line_num ,erp_ref_doc_num ,erp_profit_center ,erp_sales_group ,erp_sales_office ,erp_customer_ship_to ,erp_customer_sold_to ,erp_plant ,erp_chartaccts, bar_bods_loaddatetime ,bar_bods_record_id ,erp_source ,bar_s_entity_currency ,bar_s_curr_rate_actual ,bar_amt_usd ,etl_crte_ts,etl_crte_user)
		(  
			select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.ACCOUNT ,NULL ,t.DEPARTMENT_EXP ,NULL ,t.EXPENSES_TYPE ,t.DOCUMENT_TYPE ,NULL ,t.DOCUMENT_ID ,NULL ,NULL ,t.PRODUCT ,t.BILL_TO_CUSTOMER ,t.TRANSACTION_DATE ,t.QTY ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.SHIP_TO_CUSTOMER ,t.SOLD_TO_CUSTOMER ,NULL ,NULL ,t.LOADDATETIME ,t.ID ,'NAV',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'  FROM   bods.nav_assm_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);		


	INSERT INTO  global_pl.pl_trans_fact_temp
		(bar_account  ,bar_amt_lc ,bar_brand ,bar_bu ,bar_currtype ,bar_customer ,bar_entity ,bar_function ,bar_period ,bar_product ,bar_scenario ,bar_shipto ,bar_year ,bar_fiscal_period ,erp_account ,erp_brand_code ,erp_business_area ,erp_company_code ,erp_cost_center ,erp_doc_type ,erp_doc_line_num ,erp_doc_num ,erp_document_text ,erp_vendor ,erp_material ,erp_customer_parent ,erp_posting_date ,erp_quantity ,erp_quantity_uom ,erp_ref_doc_type ,erp_ref_doc_line_num ,erp_ref_doc_num ,erp_profit_center ,erp_sales_group ,erp_sales_office ,erp_customer_ship_to ,erp_customer_sold_to ,erp_plant ,erp_chartaccts, bar_bods_loaddatetime ,bar_bods_record_id ,erp_source ,bar_s_entity_currency ,bar_s_curr_rate_actual ,bar_amt_usd ,etl_crte_ts,etl_crte_user)
		(  
			select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.ACCT ,NULL ,NULL ,t.QAD_ENTITY ,t.COSTCTR ,t.DOCUMENT_TYPE ,NULL ,t.DOCUMENT_ID ,NULL ,NULL ,NULL ,t.BILL_TO_CUSTOMER ,t.TRANSACTION_DATE ,t.QUANTITY ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.SOLD_TO_CUSTOMER ,t.SITE ,NULL ,t.LOADDATETIME ,t.ID ,'BRAZIL',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'  FROM   bods.qad_brazil_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);		


	INSERT INTO  global_pl.pl_trans_fact_temp
		(bar_account  ,bar_amt_lc ,bar_brand ,bar_bu ,bar_currtype ,bar_customer ,bar_entity ,bar_function ,bar_period ,bar_product ,bar_scenario ,bar_shipto ,bar_year ,bar_fiscal_period ,erp_account ,erp_brand_code ,erp_business_area ,erp_company_code ,erp_cost_center ,erp_doc_type ,erp_doc_line_num ,erp_doc_num ,erp_document_text ,erp_vendor ,erp_material ,erp_customer_parent ,erp_posting_date ,erp_quantity ,erp_quantity_uom ,erp_ref_doc_type ,erp_ref_doc_line_num ,erp_ref_doc_num ,erp_profit_center ,erp_sales_group ,erp_sales_office ,erp_customer_ship_to ,erp_customer_sold_to ,erp_plant ,erp_chartaccts, bar_bods_loaddatetime ,bar_bods_record_id ,erp_source ,bar_s_entity_currency ,bar_s_curr_rate_actual ,bar_amt_usd ,etl_crte_ts,etl_crte_user)
		(  
			select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,NULL ,NULL ,NULL ,NULL ,NULL ,t.DOCUMENT_TYPE ,NULL ,t.DOCUMENT_ID ,NULL ,NULL ,t.PRODUCT ,t.BILL_TO_CUSTOMER ,t.POSTING_DATE ,t.QUANTITY ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.SHIP_TO ,t.SOLD_TO_CUSTOMER ,NULL ,NULL ,t.LOADDATETIME ,t.ID ,'DECH',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'  FROM   bods.qad_dech_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);		


	INSERT INTO  global_pl.pl_trans_fact_temp
		(bar_account  ,bar_amt_lc ,bar_brand ,bar_bu ,bar_currtype ,bar_customer ,bar_entity ,bar_function ,bar_period ,bar_product ,bar_scenario ,bar_shipto ,bar_year ,bar_fiscal_period ,erp_account ,erp_brand_code ,erp_business_area ,erp_company_code ,erp_cost_center ,erp_doc_type ,erp_doc_line_num ,erp_doc_num ,erp_document_text ,erp_vendor ,erp_material ,erp_customer_parent ,erp_posting_date ,erp_quantity ,erp_quantity_uom ,erp_ref_doc_type ,erp_ref_doc_line_num ,erp_ref_doc_num ,erp_profit_center ,erp_sales_group ,erp_sales_office ,erp_customer_ship_to ,erp_customer_sold_to ,erp_plant ,erp_chartaccts, bar_bods_loaddatetime ,bar_bods_record_id ,erp_source ,bar_s_entity_currency ,bar_s_curr_rate_actual ,bar_amt_usd ,etl_crte_ts,etl_crte_user)
		(  
			select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.ACCT ,NULL ,NULL ,t.QAD_ENTITY ,t.COSTCTR ,t.DOCUMENT_TYPE ,NULL ,t.DOCUMENT_ID ,NULL ,NULL ,NULL ,t.BILL_TO_CUSTOMER ,t.TRANSACTION_DATE ,t.QUANTITY ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.SOLD_TO_CUSTOMER ,t.SITE ,NULL ,t.LOADDATETIME ,t.ID ,'QAD',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'  FROM   bods.qad_chile_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);		


	INSERT INTO  global_pl.pl_trans_fact_temp
		(bar_account  ,bar_amt_lc ,bar_brand ,bar_bu ,bar_currtype ,bar_customer ,bar_entity ,bar_function ,bar_period ,bar_product ,bar_scenario ,bar_shipto ,bar_year ,bar_fiscal_period ,erp_account ,erp_brand_code ,erp_business_area ,erp_company_code ,erp_cost_center ,erp_doc_type ,erp_doc_line_num ,erp_doc_num ,erp_document_text ,erp_vendor ,erp_material ,erp_customer_parent ,erp_posting_date ,erp_quantity ,erp_quantity_uom ,erp_ref_doc_type ,erp_ref_doc_line_num ,erp_ref_doc_num ,erp_profit_center ,erp_sales_group ,erp_sales_office ,erp_customer_ship_to ,erp_customer_sold_to ,erp_plant ,erp_chartaccts, bar_bods_loaddatetime ,bar_bods_record_id ,erp_source ,bar_s_entity_currency ,bar_s_curr_rate_actual ,bar_amt_usd ,etl_crte_ts,etl_crte_user)
		(  
			select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.ACCT ,NULL ,NULL ,t.QAD_ENTITY ,t.COSTCTR ,t.DOCUMENT_TYPE ,NULL ,t.DOCUMENT_ID ,NULL ,NULL ,NULL ,t.BILL_TO_CUSTOMER ,t.TRANSACTION_DATE ,t.QUANTITY ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.SOLD_TO_CUSTOMER ,t.SITE ,NULL ,t.LOADDATETIME ,t.ID ,'QAD',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'  FROM   bods.qad_argentina_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);		


	INSERT INTO  global_pl.pl_trans_fact_temp
		(bar_account  ,bar_amt_lc ,bar_brand ,bar_bu ,bar_currtype ,bar_customer ,bar_entity ,bar_function ,bar_period ,bar_product ,bar_scenario ,bar_shipto ,bar_year ,bar_fiscal_period ,erp_account ,erp_brand_code ,erp_business_area ,erp_company_code ,erp_cost_center ,erp_doc_type ,erp_doc_line_num ,erp_doc_num ,erp_document_text ,erp_vendor ,erp_material ,erp_customer_parent ,erp_posting_date ,erp_quantity ,erp_quantity_uom ,erp_ref_doc_type ,erp_ref_doc_line_num ,erp_ref_doc_num ,erp_profit_center ,erp_sales_group ,erp_sales_office ,erp_customer_ship_to ,erp_customer_sold_to ,erp_plant ,erp_chartaccts, bar_bods_loaddatetime ,bar_bods_record_id ,erp_source ,bar_s_entity_currency ,bar_s_curr_rate_actual ,bar_amt_usd ,etl_crte_ts,etl_crte_user)
		(  
			select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.ACCT ,NULL ,NULL ,t.QAD_ENTITY ,t.COSTCTR ,t.DOCUMENT_TYPE ,NULL ,t.DOCUMENT_ID ,NULL ,NULL ,NULL ,t.BILL_TO_CUSTOMER ,t.TRANSACTION_DATE ,t.QUANTITY ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.SOLD_TO_CUSTOMER ,t.SITE ,NULL ,t.LOADDATETIME ,t.ID ,'QAD',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'  FROM   bods.qad_peru_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);		


	INSERT INTO  global_pl.pl_trans_fact_temp
		(bar_account  ,bar_amt_lc ,bar_brand ,bar_bu ,bar_currtype ,bar_customer ,bar_entity ,bar_function ,bar_period ,bar_product ,bar_scenario ,bar_shipto ,bar_year ,bar_fiscal_period ,erp_account ,erp_brand_code ,erp_business_area ,erp_company_code ,erp_cost_center ,erp_doc_type ,erp_doc_line_num ,erp_doc_num ,erp_document_text ,erp_vendor ,erp_material ,erp_customer_parent ,erp_posting_date ,erp_quantity ,erp_quantity_uom ,erp_ref_doc_type ,erp_ref_doc_line_num ,erp_ref_doc_num ,erp_profit_center ,erp_sales_group ,erp_sales_office ,erp_customer_ship_to ,erp_customer_sold_to ,erp_plant ,erp_chartaccts, bar_bods_loaddatetime ,bar_bods_record_id ,erp_source ,bar_s_entity_currency ,bar_s_curr_rate_actual ,bar_amt_usd ,etl_crte_ts,etl_crte_user)
		(  
			select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,NULL ,t.BAR_YEAR ,t.FISCPER ,t.ACCT ,NULL ,t.BUS_AREA ,t.CO_CD ,t.COST_CNTR ,t.DOCCT ,t.DOCLN ,t.DOCNR ,REGEXP_REPLACE(SGTXT ,'[^a-zA-Z0-9\u00E0-\u00FC ]+','') as SGTXT ,t.VENDOR_ID ,t.MATERIAL ,NULL ,t.CPUDT ,t.QUANTITY ,t.QUANUNIT ,t.REFDOCCT ,t.REFDOCLN ,t.REFDOCNR ,t.PROFIT_CNTR ,NULL ,NULL ,t.SHIPTO_CUST_NBR ,NULL ,t.PLANT ,t.CHARTACCTS ,t.LOADDATETIME ,t.ID ,'P02',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'  FROM   bods.p02_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);		


	INSERT INTO  global_pl.pl_trans_fact_temp
		(bar_account  ,bar_amt_lc ,bar_brand ,bar_bu ,bar_currtype ,bar_customer ,bar_entity ,bar_function ,bar_period ,bar_product ,bar_scenario ,bar_shipto ,bar_year ,bar_fiscal_period ,erp_account ,erp_brand_code ,erp_business_area ,erp_company_code ,erp_cost_center ,erp_doc_type ,erp_doc_line_num ,erp_doc_num ,erp_document_text ,erp_vendor ,erp_material ,erp_customer_parent ,erp_posting_date ,erp_quantity ,erp_quantity_uom ,erp_ref_doc_type ,erp_ref_doc_line_num ,erp_ref_doc_num ,erp_profit_center ,erp_sales_group ,erp_sales_office ,erp_customer_ship_to ,erp_customer_sold_to ,erp_plant ,erp_chartaccts, bar_bods_loaddatetime ,bar_bods_record_id ,erp_source ,bar_s_entity_currency ,bar_s_curr_rate_actual ,bar_amt_usd ,etl_crte_ts,etl_crte_user)
		(  
			Select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.ACCT ,NULL ,NULL ,t.CO_CD ,t.COST_CNTR ,NULL ,t.DOC_LN_NBR ,t.DOC_NBR ,NULL ,NULL ,NULL ,NULL ,t.POST_DTE ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.LOADDATETIME ,t.ID,'BAANBESCOTW',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user' from bods.baan_besco_tw_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);		



	INSERT INTO  global_pl.pl_trans_fact_temp
		(bar_account  ,bar_amt_lc ,bar_brand ,bar_bu ,bar_currtype ,bar_customer ,bar_entity ,bar_function ,bar_period ,bar_product ,bar_scenario ,bar_shipto ,bar_year ,bar_fiscal_period ,erp_account ,erp_brand_code ,erp_business_area ,erp_company_code ,erp_cost_center ,erp_doc_type ,erp_doc_line_num ,erp_doc_num ,erp_document_text ,erp_vendor ,erp_material ,erp_customer_parent ,erp_posting_date ,erp_quantity ,erp_quantity_uom ,erp_ref_doc_type ,erp_ref_doc_line_num ,erp_ref_doc_num ,erp_profit_center ,erp_sales_group ,erp_sales_office ,erp_customer_ship_to ,erp_customer_sold_to ,erp_plant ,erp_chartaccts, bar_bods_loaddatetime ,bar_bods_record_id ,erp_source ,bar_s_entity_currency ,bar_s_curr_rate_actual ,bar_amt_usd ,etl_crte_ts,etl_crte_user)
		(  
			select t.BAR_ACCT,t.BAR_AMT,t.BAR_BRAND,t.BAR_BU,t.BAR_CURRTYPE,t.BAR_CUSTNO,t.BAR_ENTITY,t.BAR_FUNCTION,t.BAR_PERIOD,t.BAR_PRODUCT,t.BAR_SCENARIO,t.BAR_SHIPTO,t.BAR_YEAR,NULL,t.ACCT,NULL,t.FUNC,t.ENTITY,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,t.LOADDATETIME,t.ID ,'NAVISION',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'), 'etl_user'  FROM   bods.navision_actuals_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);		



	INSERT INTO  global_pl.pl_trans_fact_temp
		(bar_account  ,bar_amt_lc ,bar_brand ,bar_bu ,bar_currtype ,bar_customer ,bar_entity ,bar_function ,bar_period ,bar_product ,bar_scenario ,bar_shipto ,bar_year ,bar_fiscal_period ,erp_account ,erp_brand_code ,erp_business_area ,erp_company_code ,erp_cost_center ,erp_doc_type ,erp_doc_line_num ,erp_doc_num ,erp_document_text ,erp_vendor ,erp_material ,erp_customer_parent ,erp_posting_date ,erp_quantity ,erp_quantity_uom ,erp_ref_doc_type ,erp_ref_doc_line_num ,erp_ref_doc_num ,erp_profit_center ,erp_sales_group ,erp_sales_office ,erp_customer_ship_to ,erp_customer_sold_to ,erp_plant ,erp_chartaccts, bar_bods_loaddatetime ,bar_bods_record_id ,erp_source ,bar_s_entity_currency ,bar_s_curr_rate_actual ,bar_amt_usd ,etl_crte_ts,etl_crte_user)
		(  
			select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.ACCOUNT ,t.BRAND ,NULL ,t.ENTITY ,NULL ,t.DOCUMENT_TYPE ,NULL ,t.DOCUMENT_ID ,NULL ,NULL ,t.PRODUCT ,t.CUSTOMER ,t.POSTING_DATE ,t.QUANTITY ,NULL ,NULL ,NULL ,NULL ,t.COMPANY ,NULL ,NULL ,t.END_CUSTOMER ,t.SOLD_TO_CUSTOMER ,t.PLANT ,NULL ,t.LOADDATETIME ,t.ID ,'JDE',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'  FROM   bods.jde_na_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);		



	INSERT INTO  global_pl.pl_trans_fact_temp
		(bar_account  ,bar_amt_lc ,bar_brand ,bar_bu ,bar_currtype ,bar_customer ,bar_entity ,bar_function ,bar_period ,bar_product ,bar_scenario ,bar_shipto ,bar_year ,bar_fiscal_period ,erp_account ,erp_brand_code ,erp_business_area ,erp_company_code ,erp_cost_center ,erp_doc_type ,erp_doc_line_num ,erp_doc_num ,erp_document_text ,erp_vendor ,erp_material ,erp_customer_parent ,erp_posting_date ,erp_quantity ,erp_quantity_uom ,erp_ref_doc_type ,erp_ref_doc_line_num ,erp_ref_doc_num ,erp_profit_center ,erp_sales_group ,erp_sales_office ,erp_customer_ship_to ,erp_customer_sold_to ,erp_plant ,erp_chartaccts, bar_bods_loaddatetime ,bar_bods_record_id ,erp_source ,bar_s_entity_currency ,bar_s_curr_rate_actual ,bar_amt_usd ,etl_crte_ts,etl_crte_user)
		(  
			Select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.ACCT ,NULL ,t.ICP_CD ,t.CO_CD ,NULL ,NULL ,NULL ,t.TXN_ID ,NULL ,NULL ,NULL ,NULL ,t.POST_DTE ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.LOADDATETIME ,t.ID,'ORCHPPE',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'   FROM bods.orch_ppe_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);		





			


  RAISE INFO 'Ran SP Successfull';
  
END;

$$
;

