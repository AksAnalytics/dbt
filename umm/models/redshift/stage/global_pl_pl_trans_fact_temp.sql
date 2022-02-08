
		(  
			Select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.GL_ACCOUNT ,t.INT_BRANDGRP ,t.SEGMENT ,t.COMPANY_ID ,t.COST_CENTER ,NULL ,t.JOURNAL_ENTRY_ITEM ,t.JOURNAL_ENTRY ,NULL ,NULL ,t.PRODUCT_ID ,t.PAYER ,t.POSTING_DATE ,t.INVOICED_QUANTITY ,NULL ,t.DOCUMENT_TYPE ,NULL ,t.SOURCE_DOCUMENT_ID ,t.PROFIT_CENTER ,t.CUSTOMER_CHANNEL_CODE ,NULL ,t.SHIP_TO_CUSTOMER ,t.BILL_TO_CUSTOMER ,NULL ,NULL ,t.LOADDATETIME ,t.ID ,'BYD',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'     from bods.byd_pl_trans_archive_current t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )  where t.id is not null
		);
		

	(  
		Select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,NULL ,t.ACCT ,NULL ,t.CUSTOM1 ,t.ENTITY ,t.CUSTOM2 ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.LOADDATETIME ,t.ID,'HFM',b.bar_entity_currency ,t.bar_amt as currency_rate_actual,t.bar_amt ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'  from bods.hfm_vw_hfm_actual_trans_current t  LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity 
	);
		

		( 
			select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.ACCT ,NULL ,t.DEPARTMENT ,t.COCODE ,NULL ,t.DOCUMENT_TYPE ,NULL ,t.DOCUMENT_NO ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.TRANSACTION_NO ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.LOADDATETIME ,t.ID,'NAVSTORAGE',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'  FROM bods.nav_storage_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);


		(  
			select t.BAR_ACCT,t. BAR_AMT,t. BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.ACCT ,NULL ,NULL ,t.CO_CD ,NULL ,NULL ,NULL ,t.TXN_ID ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.BAR_CUSTNO ,NULL ,NULL ,t.LOADDATETIME ,t.ID, 'NAVEUR',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'  FROM bods.nav_eur_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);

		(  
			Select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.GLACCOUNT_CODE ,NULL ,t.DEPARTMENT_CODE ,t.CDCCODE ,NULL ,NULL ,NULL ,t.VOUCHNO ,NULL ,NULL ,t.PRODUCT_CODE ,NULL ,t.POSTING_DATE ,t.PRODUCT_QTY ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.CUSTOMER_CODE ,NULL ,NULL ,t.LOADDATETIME ,t.ID,'UFIDA',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user' FROM bods.ufida_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);


		(  
			Select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.ACCT ,NULL ,t.ICP_CD ,t.CO_CD ,NULL ,NULL ,NULL ,t.TXN_ID ,NULL ,NULL ,NULL ,NULL ,t.POST_DTE ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.LOADDATETIME ,t.ID,'ORCHBGI',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'   FROM bods.orch_bgi_pl_trans_current    t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);


		(  
			Select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.ACCT ,NULL ,NULL ,t.CO_CD ,NULL ,NULL ,NULL ,t.TXN_ID ,NULL ,NULL ,NULL ,NULL ,t.POST_DTE ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.LOADDATETIME ,t.ID,'CONT',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'  from bods.cont_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);


		(  
			Select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.ACCT ,NULL ,t.DEPT ,t.CO_CD ,NULL ,NULL ,NULL ,t.TXN_ID ,NULL ,NULL ,NULL ,NULL ,t.TXN_DTE ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.CUST_NBR ,NULL ,NULL ,t.LOADDATETIME ,t.ID,'MOVEX',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'  from bods.movex_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);		



		(  
			select t.ACCOUNT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.ACCOUNT ,NULL ,t.CODE_B ,t.COMPANY ,t.ACCOUNT_GROUP ,t.VOUCHER_TYPE ,NULL ,t.VOUCHER_NO ,REGEXP_REPLACE(TEXT,'[^a-zA-Z0-9\u00E0-\u00FC ]+','') as TEXT ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.LOADDATETIME ,t.ID ,'IFS',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'), 'etl_user'  FROM   bods.ifs_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);		

		(  
			select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.GL_ACCOUNT ,NULL ,NULL ,t.COMPANY ,NULL ,t.JOURNAL_CODE ,t.JOURNAL_LINE ,t.JOURNAL_NUM ,REGEXP_REPLACE(DESCRIPTION,'[^a-zA-Z0-9\u00E0-\u00FC ]+','') as DESCRIPTION ,NULL ,NULL ,NULL ,t.POSTED_DATE ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.GROUPID ,NULL ,NULL ,NULL ,NULL ,NULL ,t.LOADDATETIME ,t.ID ,'NELSON',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'), 'etl_user'  FROM   bods.nelson_asmp_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);		



		(  
			select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,NULL ,t.BAR_YEAR ,t.FISCPER ,t.ACCOUNT ,NULL ,t.BUSINESS_AREA ,NULL ,t.COST_CENTER ,NULL ,NULL ,NULL ,NULL ,NULL ,t.PRODUCT_CODE ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.PROFIT_CENTER ,NULL ,NULL ,NULL ,t.CUST_NUM ,NULL ,NULL ,t.LOADDATETIME ,t.ID ,'AGRESSO',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'  FROM   bods.agresso_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);		



		(  
			select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.ACCOUNT ,NULL ,t.DEPARTMENT_EXP ,NULL ,t.EXPENSES_TYPE ,t.DOCUMENT_TYPE ,NULL ,t.DOCUMENT_ID ,NULL ,NULL ,t.PRODUCT ,t.BILL_TO_CUSTOMER ,t.TRANSACTION_DATE ,t.QTY ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.SHIP_TO_CUSTOMER ,t.SOLD_TO_CUSTOMER ,NULL ,NULL ,t.LOADDATETIME ,t.ID ,'NAV',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'  FROM   bods.nav_assm_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);		



		(  
			select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.ACCT ,NULL ,NULL ,t.QAD_ENTITY ,t.COSTCTR ,t.DOCUMENT_TYPE ,NULL ,t.DOCUMENT_ID ,NULL ,NULL ,NULL ,t.BILL_TO_CUSTOMER ,t.TRANSACTION_DATE ,t.QUANTITY ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.SOLD_TO_CUSTOMER ,t.SITE ,NULL ,t.LOADDATETIME ,t.ID ,'BRAZIL',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'  FROM   bods.qad_brazil_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);		



		(  
			select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,NULL ,NULL ,NULL ,NULL ,NULL ,t.DOCUMENT_TYPE ,NULL ,t.DOCUMENT_ID ,NULL ,NULL ,t.PRODUCT ,t.BILL_TO_CUSTOMER ,t.POSTING_DATE ,t.QUANTITY ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.SHIP_TO ,t.SOLD_TO_CUSTOMER ,NULL ,NULL ,t.LOADDATETIME ,t.ID ,'DECH',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'  FROM   bods.qad_dech_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);		



		(  
			select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.ACCT ,NULL ,NULL ,t.QAD_ENTITY ,t.COSTCTR ,t.DOCUMENT_TYPE ,NULL ,t.DOCUMENT_ID ,NULL ,NULL ,NULL ,t.BILL_TO_CUSTOMER ,t.TRANSACTION_DATE ,t.QUANTITY ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.SOLD_TO_CUSTOMER ,t.SITE ,NULL ,t.LOADDATETIME ,t.ID ,'QAD',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'  FROM   bods.qad_chile_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);		

		(  
			select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.ACCT ,NULL ,NULL ,t.QAD_ENTITY ,t.COSTCTR ,t.DOCUMENT_TYPE ,NULL ,t.DOCUMENT_ID ,NULL ,NULL ,NULL ,t.BILL_TO_CUSTOMER ,t.TRANSACTION_DATE ,t.QUANTITY ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.SOLD_TO_CUSTOMER ,t.SITE ,NULL ,t.LOADDATETIME ,t.ID ,'QAD',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'  FROM   bods.qad_argentina_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);		



		(  
			select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.ACCT ,NULL ,NULL ,t.QAD_ENTITY ,t.COSTCTR ,t.DOCUMENT_TYPE ,NULL ,t.DOCUMENT_ID ,NULL ,NULL ,NULL ,t.BILL_TO_CUSTOMER ,t.TRANSACTION_DATE ,t.QUANTITY ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.SOLD_TO_CUSTOMER ,t.SITE ,NULL ,t.LOADDATETIME ,t.ID ,'QAD',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'  FROM   bods.qad_peru_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);		

		(  
			select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,NULL ,t.BAR_YEAR ,t.FISCPER ,t.ACCT ,NULL ,t.BUS_AREA ,t.CO_CD ,t.COST_CNTR ,t.DOCCT ,t.DOCLN ,t.DOCNR ,REGEXP_REPLACE(SGTXT ,'[^a-zA-Z0-9\u00E0-\u00FC ]+','') as SGTXT ,t.VENDOR_ID ,t.MATERIAL ,NULL ,t.CPUDT ,t.QUANTITY ,t.QUANUNIT ,t.REFDOCCT ,t.REFDOCLN ,t.REFDOCNR ,t.PROFIT_CNTR ,NULL ,NULL ,t.SHIPTO_CUST_NBR ,NULL ,t.PLANT ,t.CHARTACCTS ,t.LOADDATETIME ,t.ID ,'P02',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'  FROM   bods.p02_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);		

		(  
			Select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.ACCT ,NULL ,NULL ,t.CO_CD ,t.COST_CNTR ,NULL ,t.DOC_LN_NBR ,t.DOC_NBR ,NULL ,NULL ,NULL ,NULL ,t.POST_DTE ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.LOADDATETIME ,t.ID,'BAANBESCOTW',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user' from bods.baan_besco_tw_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);		


		(  
			select t.BAR_ACCT,t.BAR_AMT,t.BAR_BRAND,t.BAR_BU,t.BAR_CURRTYPE,t.BAR_CUSTNO,t.BAR_ENTITY,t.BAR_FUNCTION,t.BAR_PERIOD,t.BAR_PRODUCT,t.BAR_SCENARIO,t.BAR_SHIPTO,t.BAR_YEAR,NULL,t.ACCT,NULL,t.FUNC,t.ENTITY,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,t.LOADDATETIME,t.ID ,'NAVISION',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'), 'etl_user'  FROM   bods.navision_actuals_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);		

		(  
			select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.ACCOUNT ,t.BRAND ,NULL ,t.ENTITY ,NULL ,t.DOCUMENT_TYPE ,NULL ,t.DOCUMENT_ID ,NULL ,NULL ,t.PRODUCT ,t.CUSTOMER ,t.POSTING_DATE ,t.QUANTITY ,NULL ,NULL ,NULL ,NULL ,t.COMPANY ,NULL ,NULL ,t.END_CUSTOMER ,t.SOLD_TO_CUSTOMER ,t.PLANT ,NULL ,t.LOADDATETIME ,t.ID ,'JDE',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'  FROM   bods.jde_na_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);		


		(  
			Select t.BAR_ACCT ,t.BAR_AMT ,t.BAR_BRAND ,t.BAR_BU ,t.BAR_CURRTYPE ,t.BAR_CUSTNO ,t.BAR_ENTITY ,t.BAR_FUNCTION ,t.BAR_PERIOD ,t.BAR_PRODUCT ,t.BAR_SCENARIO ,t.BAR_SHIPTO ,t.BAR_YEAR ,t.FISCPER ,t.ACCT ,NULL ,t.ICP_CD ,t.CO_CD ,NULL ,NULL ,NULL ,t.TXN_ID ,NULL ,NULL ,NULL ,NULL ,t.POST_DTE ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,t.LOADDATETIME ,t.ID,'ORCHPPE',b.bar_entity_currency ,hfm.bar_amt as currency_rate_actual,(t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) as bar_amt_usd ,to_date(GETDATE(),'yyyyMMdd'),'etl_user'   FROM bods.orch_ppe_pl_trans_current  t LEFT OUTER JOIN global_pl.bar_acct_attr a on t.BAR_ACCT = a.bar_account LEFT OUTER JOIN global_pl.bar_entity_attr b on t.BAR_ENTITY = b.bar_entity LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND hfm.bar_function = b.bar_entity_currency )
		);		