CREATE OR REPLACE FUNCTION global_pl.f_holiday(dt date)
	RETURNS bool
	LANGUAGE plpythonu
	STABLE
AS $$
	
    import pandas as pd
    from pandas.tseries.holiday import USFederalHolidayCalendar as calendar
    holidays = calendar().holidays(start='1900-01-01', end='2049-12-31')
    return dt in holidays

$$
;

CREATE OR REPLACE PROCEDURE global_pl.sp_pl_master($1 varchar,$2 varchar,$3 int4)
	LANGUAGE plpgsql
AS $$
	

  DECLARE
	REC RECORD;
	QUERY varchar(max);
  
	CURRENT_PERIOD varchar(50);
	PREVIOUS_PERIOD varchar(50);
	CURRENT_YEAR varchar(50);
	PREVIOUS_YEAR varchar(50);
	CURRENT_PERIOD_DELETED bigint;
	CURRENT_PERIOD_INSERTED bigint;
	PREVIOUS_PERIOD_DELETED bigint;
	PREVIOUS_PERIOD_INSERTED bigint;
	
	CURRENT_JOB_ID bigint;
	CURRENT_RUN_SEQ bigint;
	CURRENT_JOB_STATE varchar(50);
	CURRENT_JOB_NAME varchar(50);
	
	TBL_TYPE varchar(50);
	RUN_FREQUENCY varchar(50);
	FLAG integer;
	

	
  BEGIN
	
	TBL_TYPE := $1;
	RUN_FREQUENCY := $2;
	FLAG := $3;
	
		
	
	
	/* Get details of all jobs for running*/
	QUERY := 'Select job_nm, src_tbl_nm, tgt_tbl_nm,src_col_lst, erp_source,tgt_col_lst FROM global_pl.status_master where frequency = ''' || RUN_FREQUENCY || '''and tbl_type=''' || TBL_TYPE || ''' and job_state = ''ACTIVE'' and manual_run =' || FLAG ;
	
	FOR REC IN EXECUTE QUERY
	LOOP
		RAISE INFO 'Job_Name: %  , src table_Name : %', REC.job_nm,REC.tgt_tbl_nm;
	
	
	
	CURRENT_JOB_NAME := REC.job_nm;
	--SELECT job_nm INTO CURRENT_JOB_NAME FROM global_pl.status_master where erp_source = 'C11' AND TABLE_TYPE = 'TRANS';
	RAISE INFO 'Running SP for Job : %',CURRENT_JOB_NAME;
	
	SELECT COALESCE(max(run_seq),0)+1 into CURRENT_RUN_SEQ from global_pl.status_history where job_nm = CURRENT_JOB_NAME AND DATE(GETDATE()) = DATE(run_date);
	RAISE INFO 'Current Run Sequence : %',CURRENT_RUN_SEQ;
	
	INSERT INTO global_pl.status_history (job_nm,run_date,run_seq,start_time,job_status) values (CURRENT_JOB_NAME,DATE(GETDATE()),CURRENT_RUN_SEQ,GETDATE(),'RUNNING');
  
  
  
	
	
	/* Delete Target Table Data */
	
		IF TBL_TYPE = 'CUSTOMER_MASTER' OR TBL_TYPE = 'MATERIAL_MASTER' THEN
			QUERY := 'delete from ' || REC.tgt_tbl_nm || ' where erp_source = ''' || REC.erp_source || '''';
		ELSE
			QUERY := 'delete from ' || REC.tgt_tbl_nm;
		END IF;
	
	--QUERY := 'delete from ' || REC.tgt_tbl_nm || ' where erp_source = ''' || REC.erp_source || '''';
	EXECUTE QUERY;	
	GET DIAGNOSTICS CURRENT_PERIOD_DELETED = ROW_COUNT;
	RAISE INFO 'Current Period ROws Deleted :- % ',CURRENT_PERIOD_DELETED;
	
	UPDATE global_pl.status_history SET curr_mon_rows_del = CURRENT_PERIOD_DELETED 	where run_seq = CURRENT_RUN_SEQ and job_nm = CURRENT_JOB_NAME AND DATE(GETDATE()) = DATE(run_date);
	

	/* Insert Data */
	QUERY := 'INSERT INTO  ' || REC.tgt_tbl_nm || '  ' || REC.tgt_col_lst || '(Select ' || REC.src_col_lst || '  FROM ' || REC.src_tbl_nm || ')' ;
	EXECUTE QUERY;
	GET DIAGNOSTICS CURRENT_PERIOD_INSERTED = ROW_COUNT;
	RAISE NOTICE 'Current Period ROws Inserted :- % ',QUERY;

	UPDATE global_pl.status_history SET	curr_mon_rows_ins = CURRENT_PERIOD_INSERTED	where run_seq = CURRENT_RUN_SEQ and job_nm = CURRENT_JOB_NAME AND DATE(GETDATE()) = DATE(run_date);
	
	
	UPDATE global_pl.status_history SET
	end_time = GETDATE(),
	job_status = 'COMPLETED'
	where run_seq = CURRENT_RUN_SEQ and job_nm = CURRENT_JOB_NAME AND DATE(GETDATE()) = DATE(run_date);
	
	
	
	
	
  RAISE INFO 'Ran SP(pl_trans_fact_c11_sp) Successfull';
	END LOOP;
END;

$$
;

CREATE OR REPLACE PROCEDURE global_pl.sp_pl_master1($1 varchar,$2 varchar,$3 int4)
	LANGUAGE plpgsql
AS $$
	

  DECLARE
	REC RECORD;
	QUERY varchar(max);
  
	CURRENT_PERIOD varchar(50);
	PREVIOUS_PERIOD varchar(50);
	CURRENT_YEAR varchar(50);
	PREVIOUS_YEAR varchar(50);
	CURRENT_PERIOD_DELETED bigint;
	CURRENT_PERIOD_INSERTED bigint;
	PREVIOUS_PERIOD_DELETED bigint;
	PREVIOUS_PERIOD_INSERTED bigint;
	
	CURRENT_JOB_ID bigint;
	CURRENT_RUN_SEQ bigint;
	CURRENT_JOB_STATE varchar(50);
	CURRENT_JOB_NAME varchar(50);
	
	TBL_TYPE varchar(50);
	RUN_FREQUENCY varchar(50);
	FLAG integer;
	

	
  BEGIN
	
	TBL_TYPE := $1;
	RUN_FREQUENCY := $2;
	FLAG := $3;
	
		
	
	
	/* Get details of all jobs for running*/
	QUERY := 'Select job_nm, src_tbl_nm, tgt_tbl_nm,src_col_lst, erp_source,tgt_col_lst FROM global_pl.status_master_1 where frequency = ''' || RUN_FREQUENCY || '''and tbl_type=''' || TBL_TYPE || ''' and job_state = ''ACTIVE'' and manual_run =' || FLAG ;
	
	FOR REC IN EXECUTE QUERY
	LOOP
		--RAISE INFO 'Job_Name: %  , src table_Name : %', REC.job_nm,REC.tgt_tbl_nm;
	
	
	
	
	BEGIN
	
	
		CURRENT_JOB_NAME := REC.job_nm;
		--SELECT job_nm INTO CURRENT_JOB_NAME FROM global_pl.status_master_1 where erp_source = 'C11' AND TABLE_TYPE = 'TRANS';
		--RAISE INFO 'Running SP for Job : %',CURRENT_JOB_NAME;
		
		SELECT COALESCE(max(run_seq),0)+1 into CURRENT_RUN_SEQ from global_pl.status_history_1 where job_nm = CURRENT_JOB_NAME AND DATE(GETDATE()) = DATE(run_date);
		--RAISE INFO 'Current Run Sequence : %',CURRENT_RUN_SEQ;
		
		INSERT INTO global_pl.status_history_1 (job_nm,run_date,run_seq,start_time,job_status) values (CURRENT_JOB_NAME,DATE(GETDATE()),CURRENT_RUN_SEQ,GETDATE(),'RUNNING');
	  
	  
	  
		
		
		/* Delete Target Table Data */
		
			IF TBL_TYPE = 'CUSTOMER_MASTER' OR TBL_TYPE = 'MATERIAL_MASTER' THEN
				QUERY := 'delete from ' || REC.tgt_tbl_nm || ' where erp_source = ''' || REC.erp_source || '''';
			ELSE
				QUERY := 'delete from ' || REC.tgt_tbl_nm;
			END IF;
		
		--QUERY := 'delete from ' || REC.tgt_tbl_nm || ' where erp_source = ''' || REC.erp_source || '''';
		EXECUTE QUERY;	
		GET DIAGNOSTICS CURRENT_PERIOD_DELETED = ROW_COUNT;
		--RAISE INFO 'Current Period ROws Deleted :- % ',CURRENT_PERIOD_DELETED;
		
		UPDATE global_pl.status_history_1 SET curr_mon_rows_del = CURRENT_PERIOD_DELETED 	where run_seq = CURRENT_RUN_SEQ and job_nm = CURRENT_JOB_NAME AND DATE(GETDATE()) = DATE(run_date);
		

		/* Insert Data */
		QUERY := 'INSERT INTO  ' || REC.tgt_tbl_nm || '  ' || REC.tgt_col_lst || '(Select ' || REC.src_col_lst || '  FROM ' || REC.src_tbl_nm || ')' ;
		EXECUTE QUERY;
		GET DIAGNOSTICS CURRENT_PERIOD_INSERTED = ROW_COUNT;
		--RAISE NOTICE 'Current Period ROws Inserted :- % ',QUERY;

		UPDATE global_pl.status_history_1 SET	curr_mon_rows_ins = CURRENT_PERIOD_INSERTED	where run_seq = CURRENT_RUN_SEQ and job_nm = CURRENT_JOB_NAME AND DATE(GETDATE()) = DATE(run_date);
		
			


		UPDATE global_pl.status_history SET
		end_time = GETDATE(),
		job_status = 'COMPLETED'
		where run_seq = CURRENT_RUN_SEQ and job_nm = CURRENT_JOB_NAME AND DATE(GETDATE()) = DATE(run_date);			
			
			
		EXCEPTION
		  WHEN OTHERS THEN NULL;
		
			
		END;
	
	
	
  --RAISE INFO 'Ran SP(pl_trans_fact_c11_sp) Successfull';
	END LOOP;
END;

$$
;

CREATE OR REPLACE PROCEDURE global_pl.sp_pl_trans_fact($1 varchar,$2 int4)
	LANGUAGE plpgsql
AS $$
	

  DECLARE
	REC RECORD;
	QUERY varchar(max);
  
	CURRENT_PERIOD varchar(10);
	PREVIOUS_PERIOD varchar(10);
	CURRENT_YEAR varchar(10);
	PREVIOUS_YEAR varchar(10);
	CURRENT_PERIOD_DELETED bigint;
	CURRENT_PERIOD_INSERTED bigint;
	PREVIOUS_PERIOD_DELETED bigint;
	PREVIOUS_PERIOD_INSERTED bigint;
	
	CURRENT_JOB_ID bigint;
	CURRENT_RUN_SEQ bigint;
	CURRENT_JOB_STATE VARCHAR(100);
	CURRENT_JOB_NAME VARCHAR(100);
	
	RUN_FREQUENCY VARCHAR(100);
	FLAG integer;
	

	
  BEGIN
	
	RUN_FREQUENCY := $1;
	FLAG := $2;
	
		
	
	
	/* Get details of all jobs for running*/
	QUERY := 'Select job_nm, src_tbl_nm, tgt_tbl_nm,src_col_lst, erp_source,tgt_col_lst FROM global_pl.status_master where tbl_type = ''TRANS'' and frequency = ''' || RUN_FREQUENCY ||''' and job_state = ''ACTIVE'' and manual_run =' || FLAG ;
	
	FOR REC IN EXECUTE QUERY
	LOOP
		RAISE INFO 'Job_Name: %  , src table_Name : %', REC.job_nm,REC.tgt_tbl_nm;
	
	
	
	CURRENT_JOB_NAME := REC.job_nm;
	--SELECT job_nm INTO CURRENT_JOB_NAME FROM global_pl.status_master where erp_source = 'C11' AND TABLE_TYPE = 'TRANS';
	RAISE INFO 'Running SP for Job : %',CURRENT_JOB_NAME;
	
	SELECT COALESCE(max(run_seq),0)+1 into CURRENT_RUN_SEQ from global_pl.status_history where job_nm = CURRENT_JOB_NAME AND DATE(GETDATE()) = DATE(run_date);
	RAISE INFO 'Current Run Sequence : %',CURRENT_RUN_SEQ;
	
	INSERT INTO global_pl.status_history (job_nm,run_date,run_seq,start_time,job_status) values (CURRENT_JOB_NAME,DATE(GETDATE()),CURRENT_RUN_SEQ,GETDATE(),'RUNNING');
  
  
  
  
	
	/* Get Current/Previous  Year/Period from System Date */
	
	Select TO_CHAR(CURRENT_DATE - '0 month'::interval, 'Mon') AS "Mon" into CURRENT_PERIOD;
	Select TO_CHAR(CURRENT_DATE - '1 month'::interval, 'Mon') AS "Mon" into PREVIOUS_PERIOD;
	SELECT EXTRACT(year FROM CURRENT_DATE) into CURRENT_YEAR;
	
		IF CURRENT_PERIOD = 'Jan' THEN
			SELECT EXTRACT(year FROM CURRENT_DATE - '1 year'::interval) into PREVIOUS_YEAR;
		ELSE
			SELECT EXTRACT(year FROM CURRENT_DATE) into PREVIOUS_YEAR;
		END IF;
	
	
	RAISE INFO 'Current Year :: % , Current Period:: %',CURRENT_YEAR,CURRENT_PERIOD;
	RAISE INFO 'Previous Year :: % , Previosu Period  : %',PREVIOUS_YEAR,PREVIOUS_PERIOD;
	
	/* Delete Current Period Data */
	--delete from REC.tgt_tbl_nm where erp_source = REC.erp_source and bar_year in (CURRENT_YEAR) and bar_period in (CURRENT_PERIOD);
	QUERY := 'delete from ' || REC.tgt_tbl_nm || ' where erp_source = ''' || REC.erp_source || ''' and bar_year in (' || CURRENT_YEAR || ') and bar_period in (''' || CURRENT_PERIOD || ''')';
	EXECUTE QUERY;	
	GET DIAGNOSTICS CURRENT_PERIOD_DELETED = ROW_COUNT;
	RAISE INFO 'Current Period ROws Deleted :- % ',CURRENT_PERIOD_DELETED;
	
	UPDATE global_pl.status_history SET curr_mon_rows_del = CURRENT_PERIOD_DELETED 	where run_seq = CURRENT_RUN_SEQ and job_nm = CURRENT_JOB_NAME AND DATE(GETDATE()) = DATE(run_date);
	

	/* Delete Previous Period Data */
	--delete from REC.tgt_tbl_nm where erp_source = REC.erp_source and bar_year in (PREVIOUS_YEAR) and bar_period in (PREVIOUS_PERIOD);
	QUERY := 'delete from ' || REC.tgt_tbl_nm || ' where erp_source = ''' || REC.erp_source || ''' and bar_year in (' || PREVIOUS_YEAR || ') and bar_period in (''' || PREVIOUS_PERIOD || ''')';
	EXECUTE QUERY;	
	GET DIAGNOSTICS PREVIOUS_PERIOD_DELETED = ROW_COUNT;
	RAISE INFO 'Previous Period ROws Deleted :- % ',PREVIOUS_PERIOD_DELETED;
	
	UPDATE global_pl.status_history SET	prev_mon_rows_del = PREVIOUS_PERIOD_DELETED where run_seq = CURRENT_RUN_SEQ and job_nm = CURRENT_JOB_NAME AND DATE(GETDATE()) = DATE(run_date);
		
	/* Insert Current Period Data */
	/*INSERT INTO  REC.tgt_tbl_nm
	(bar_account  ,bar_amt_lc ,bar_brand ,bar_bu ,bar_currtype ,bar_customer ,bar_entity ,bar_function ,bar_period ,bar_product ,bar_scenario ,bar_shipto ,bar_year ,bar_fiscal_period ,erp_account ,erp_brand_code ,erp_business_area ,erp_company_code ,erp_cost_center ,erp_doc_type ,erp_doc_line_num ,erp_doc_num ,erp_document_text ,erp_vendor ,erp_material ,erp_customer_parent ,erp_posting_date ,erp_quantity ,erp_quantity_uom ,erp_ref_doc_type ,erp_ref_doc_line_num ,erp_ref_doc_num ,erp_profit_center ,erp_sales_group ,erp_sales_office ,erp_customer_ship_to ,erp_customer_sold_to ,erp_plant ,erp_chartaccts, bar_bods_loaddatetime ,bar_bods_record_id ,erp_source ,bar_s_entity_currency ,bar_s_curr_rate_actual ,bar_amt_usd ,etl_crte_ts,etl_crte_user)
	(
	Select REC.src_col_lst  FROM REC.src_tbl_nm  t  LEFT OUTER JOIN global_pl.bar_acct_attr a on UPPER(t.BAR_ACCT) = UPPER(a.bar_account) LEFT OUTER JOIN global_pl.bar_entity_attr b on UPPER(t.BAR_ENTITY) = UPPER(b.bar_entity) LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND UPPER(hfm.bar_function) = UPPER(b.bar_entity_currency) AND UPPER(hfm.BAR_ACCT)='PLRATE')   where t.bar_year in (CURRENT_YEAR) and t.bar_period in (CURRENT_PERIOD)  
	);*/
	QUERY := 'INSERT INTO  ' || REC.tgt_tbl_nm || '  ' || REC.tgt_col_lst || '(Select ' || REC.src_col_lst || '  FROM ' || REC.src_tbl_nm || '  t  LEFT OUTER JOIN global_pl.bar_acct_attr a on UPPER(t.BAR_ACCT) = UPPER(a.bar_account) LEFT OUTER JOIN global_pl.bar_entity_attr b on UPPER(t.BAR_ENTITY) = UPPER(b.bar_entity) LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND UPPER(hfm.bar_function) = UPPER(b.bar_entity_currency) AND UPPER(hfm.BAR_ACCT)=''PLRATE'')   where t.bar_year in (' || CURRENT_YEAR || ') and t.bar_period in (''' || CURRENT_PERIOD || '''))' ;
	EXECUTE QUERY;
	GET DIAGNOSTICS CURRENT_PERIOD_INSERTED = ROW_COUNT;
	RAISE NOTICE 'Current Period ROws Inserted :- % ',QUERY;

	UPDATE global_pl.status_history SET	curr_mon_rows_ins = CURRENT_PERIOD_INSERTED	where run_seq = CURRENT_RUN_SEQ and job_nm = CURRENT_JOB_NAME AND DATE(GETDATE()) = DATE(run_date);

	/* Insert Previous Period Data */
	/*INSERT INTO  REC.tgt_tbl_nm
	(bar_account  ,bar_amt_lc ,bar_brand ,bar_bu ,bar_currtype ,bar_customer ,bar_entity ,bar_function ,bar_period ,bar_product ,bar_scenario ,bar_shipto ,bar_year ,bar_fiscal_period ,erp_account ,erp_brand_code ,erp_business_area ,erp_company_code ,erp_cost_center ,erp_doc_type ,erp_doc_line_num ,erp_doc_num ,erp_document_text ,erp_vendor ,erp_material ,erp_customer_parent ,erp_posting_date ,erp_quantity ,erp_quantity_uom ,erp_ref_doc_type ,erp_ref_doc_line_num ,erp_ref_doc_num ,erp_profit_center ,erp_sales_group ,erp_sales_office ,erp_customer_ship_to ,erp_customer_sold_to ,erp_plant ,erp_chartaccts, bar_bods_loaddatetime ,bar_bods_record_id ,erp_source ,bar_s_entity_currency ,bar_s_curr_rate_actual ,bar_amt_usd ,etl_crte_ts,etl_crte_user)
	(
	Select REC.src_col_lst  FROM REC.src_tbl_nm  t  LEFT OUTER JOIN global_pl.bar_acct_attr a on UPPER(t.BAR_ACCT) = UPPER(a.bar_account) LEFT OUTER JOIN global_pl.bar_entity_attr b on UPPER(t.BAR_ENTITY) = UPPER(b.bar_entity) LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND UPPER(hfm.bar_function) = UPPER(b.bar_entity_currency) AND UPPER(hfm.BAR_ACCT)='PLRATE')   where t.bar_year in (PREVIOUS_YEAR) and t.bar_period in (PREVIOUS_PERIOD)  
	);*/

	QUERY := 'INSERT INTO  ' || REC.tgt_tbl_nm || '  ' || REC.tgt_col_lst || '(Select ' || REC.src_col_lst || '  FROM ' || REC.src_tbl_nm || '  t  LEFT OUTER JOIN global_pl.bar_acct_attr a on UPPER(t.BAR_ACCT) = UPPER(a.bar_account) LEFT OUTER JOIN global_pl.bar_entity_attr b on UPPER(t.BAR_ENTITY) = UPPER(b.bar_entity) LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND UPPER(hfm.bar_function) = UPPER(b.bar_entity_currency) AND UPPER(hfm.BAR_ACCT)=''PLRATE'')   where t.bar_year in (' || PREVIOUS_YEAR || ') and t.bar_period in (''' || PREVIOUS_PERIOD || '''))' ;
	EXECUTE QUERY;
	GET DIAGNOSTICS PREVIOUS_PERIOD_INSERTED = ROW_COUNT;
	RAISE NOTICE 'Current Period ROws Inserted :- % ',QUERY;
	
	UPDATE global_pl.status_history SET	prev_mon_rows_ins = PREVIOUS_PERIOD_INSERTED	where run_seq = CURRENT_RUN_SEQ and job_nm = CURRENT_JOB_NAME AND DATE(GETDATE()) = DATE(run_date);
	
	
	UPDATE global_pl.status_history SET
	end_time = GETDATE(),
	job_status = 'COMPLETED'
	where run_seq = CURRENT_RUN_SEQ and job_nm = CURRENT_JOB_NAME AND DATE(GETDATE()) = DATE(run_date);
	
	
	
	
	
  RAISE INFO 'Ran SP(pl_trans_fact_c11_sp) Successfull';
	END LOOP;
END;

$$
;

CREATE OR REPLACE PROCEDURE global_pl.sp_pl_trans_fact_1($1 varchar,$2 int4)
	LANGUAGE plpgsql
AS $$
	

  DECLARE
	REC RECORD;
	QUERY varchar(max);
  
	CURRENT_PERIOD varchar(10);
	PREVIOUS_PERIOD varchar(10);
	CURRENT_YEAR varchar(10);
	PREVIOUS_YEAR varchar(10);
	CURRENT_PERIOD_DELETED bigint;
	CURRENT_PERIOD_INSERTED bigint;
	PREVIOUS_PERIOD_DELETED bigint;
	PREVIOUS_PERIOD_INSERTED bigint;
	
	CURRENT_JOB_ID bigint;
	CURRENT_RUN_SEQ bigint;
	CURRENT_JOB_STATE VARCHAR(100);
	CURRENT_JOB_NAME VARCHAR(100);
	
	RUN_FREQUENCY VARCHAR(100);
	FLAG integer;
	

	
  BEGIN
	
	RUN_FREQUENCY := $1;
	FLAG := $2;
	
		
	
	
	/* Get details of all jobs for running*/
	QUERY := 'Select job_nm, src_tbl_nm, tgt_tbl_nm,src_col_lst, erp_source,tgt_col_lst FROM global_pl.status_master_1 where tbl_type = ''TRANS'' and frequency = ''' || RUN_FREQUENCY ||''' and job_state = ''ACTIVE'' and manual_run =' || FLAG ;
	
	FOR REC IN EXECUTE QUERY
	LOOP
		RAISE INFO 'Job_Name: %  , src table_Name : %', REC.job_nm,REC.tgt_tbl_nm;
	
	
	
	CURRENT_JOB_NAME := REC.job_nm;
	--SELECT job_nm INTO CURRENT_JOB_NAME FROM global_pl.status_master_1 where erp_source = 'C11' AND TABLE_TYPE = 'TRANS';
	RAISE INFO 'Running SP for Job : %',CURRENT_JOB_NAME;
	
	SELECT COALESCE(max(run_seq),0)+1 into CURRENT_RUN_SEQ from global_pl.status_history_1 where job_nm = CURRENT_JOB_NAME AND DATE(GETDATE()) = DATE(run_date);
	RAISE INFO 'Current Run Sequence : %',CURRENT_RUN_SEQ;
	
	INSERT INTO global_pl.status_history_1 (job_nm,run_date,run_seq,start_time,job_status) values (CURRENT_JOB_NAME,DATE(GETDATE()),CURRENT_RUN_SEQ,GETDATE(),'RUNNING');
  
  
  
  
	
	/* Get Current/Previous  Year/Period from System Date */
	
	Select TO_CHAR(CURRENT_DATE - '0 month'::interval, 'Mon') AS "Mon" into CURRENT_PERIOD;
	Select TO_CHAR(CURRENT_DATE - '1 month'::interval, 'Mon') AS "Mon" into PREVIOUS_PERIOD;
	SELECT EXTRACT(year FROM CURRENT_DATE) into CURRENT_YEAR;
	
		IF CURRENT_PERIOD = 'Jan' THEN
			SELECT EXTRACT(year FROM CURRENT_DATE - '1 year'::interval) into PREVIOUS_YEAR;
		ELSE
			SELECT EXTRACT(year FROM CURRENT_DATE) into PREVIOUS_YEAR;
		END IF;
	
	
	RAISE INFO 'Current Year :: % , Current Period:: %',CURRENT_YEAR,CURRENT_PERIOD;
	RAISE INFO 'Previous Year :: % , Previosu Period  : %',PREVIOUS_YEAR,PREVIOUS_PERIOD;
	
	/* Delete Current Period Data */
	--delete from REC.tgt_tbl_nm where erp_source = REC.erp_source and bar_year in (CURRENT_YEAR) and bar_period in (CURRENT_PERIOD);
	QUERY := 'delete from ' || REC.tgt_tbl_nm || ' where erp_source = ''' || REC.erp_source || ''' and bar_year in (' || CURRENT_YEAR || ') and bar_period in (''' || CURRENT_PERIOD || ''')';
	EXECUTE QUERY;	
	GET DIAGNOSTICS CURRENT_PERIOD_DELETED = ROW_COUNT;
	RAISE INFO 'Current Period ROws Deleted :- % ',CURRENT_PERIOD_DELETED;
	
	UPDATE global_pl.status_history_1 SET curr_mon_rows_del = CURRENT_PERIOD_DELETED 	where run_seq = CURRENT_RUN_SEQ and job_nm = CURRENT_JOB_NAME AND DATE(GETDATE()) = DATE(run_date);
	

	/* Delete Previous Period Data */
	--delete from REC.tgt_tbl_nm where erp_source = REC.erp_source and bar_year in (PREVIOUS_YEAR) and bar_period in (PREVIOUS_PERIOD);
	QUERY := 'delete from ' || REC.tgt_tbl_nm || ' where erp_source = ''' || REC.erp_source || ''' and bar_year in (' || PREVIOUS_YEAR || ') and bar_period in (''' || PREVIOUS_PERIOD || ''')';
	EXECUTE QUERY;	
	GET DIAGNOSTICS PREVIOUS_PERIOD_DELETED = ROW_COUNT;
	RAISE INFO 'Previous Period ROws Deleted :- % ',PREVIOUS_PERIOD_DELETED;
	
	UPDATE global_pl.status_history_1 SET	prev_mon_rows_del = PREVIOUS_PERIOD_DELETED where run_seq = CURRENT_RUN_SEQ and job_nm = CURRENT_JOB_NAME AND DATE(GETDATE()) = DATE(run_date);
		
	/* Insert Current Period Data */
	/*INSERT INTO  REC.tgt_tbl_nm
	(bar_account  ,bar_amt_lc ,bar_brand ,bar_bu ,bar_currtype ,bar_customer ,bar_entity ,bar_function ,bar_period ,bar_product ,bar_scenario ,bar_shipto ,bar_year ,bar_fiscal_period ,erp_account ,erp_brand_code ,erp_business_area ,erp_company_code ,erp_cost_center ,erp_doc_type ,erp_doc_line_num ,erp_doc_num ,erp_document_text ,erp_vendor ,erp_material ,erp_customer_parent ,erp_posting_date ,erp_quantity ,erp_quantity_uom ,erp_ref_doc_type ,erp_ref_doc_line_num ,erp_ref_doc_num ,erp_profit_center ,erp_sales_group ,erp_sales_office ,erp_customer_ship_to ,erp_customer_sold_to ,erp_plant ,erp_chartaccts, bar_bods_loaddatetime ,bar_bods_record_id ,erp_source ,bar_s_entity_currency ,bar_s_curr_rate_actual ,bar_amt_usd ,etl_crte_ts,etl_crte_user)
	(
	Select REC.src_col_lst  FROM REC.src_tbl_nm  t  LEFT OUTER JOIN global_pl.bar_acct_attr a on UPPER(t.BAR_ACCT) = UPPER(a.bar_account) LEFT OUTER JOIN global_pl.bar_entity_attr b on UPPER(t.BAR_ENTITY) = UPPER(b.bar_entity) LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND UPPER(hfm.bar_function) = UPPER(b.bar_entity_currency) AND UPPER(hfm.BAR_ACCT)='PLRATE')   where t.bar_year in (CURRENT_YEAR) and t.bar_period in (CURRENT_PERIOD)  
	);*/
	QUERY := 'INSERT INTO  ' || REC.tgt_tbl_nm || '  ' || REC.tgt_col_lst || '(Select ' || REC.src_col_lst || '  FROM ' || REC.src_tbl_nm || '  t  LEFT OUTER JOIN global_pl.bar_acct_attr a on UPPER(t.BAR_ACCT) = UPPER(a.bar_account) LEFT OUTER JOIN global_pl.bar_entity_attr b on UPPER(t.BAR_ENTITY) = UPPER(b.bar_entity) LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND UPPER(hfm.bar_function) = UPPER(b.bar_entity_currency) AND UPPER(hfm.BAR_ACCT)=''PLRATE'')   where t.bar_year in (' || CURRENT_YEAR || ') and t.bar_period in (''' || CURRENT_PERIOD || '''))' ;
	EXECUTE QUERY;
	GET DIAGNOSTICS CURRENT_PERIOD_INSERTED = ROW_COUNT;
	RAISE NOTICE 'Current Period ROws Inserted :- % ',QUERY;

	UPDATE global_pl.status_history_1 SET	curr_mon_rows_ins = CURRENT_PERIOD_INSERTED	where run_seq = CURRENT_RUN_SEQ and job_nm = CURRENT_JOB_NAME AND DATE(GETDATE()) = DATE(run_date);

	/* Insert Previous Period Data */
	/*INSERT INTO  REC.tgt_tbl_nm
	(bar_account  ,bar_amt_lc ,bar_brand ,bar_bu ,bar_currtype ,bar_customer ,bar_entity ,bar_function ,bar_period ,bar_product ,bar_scenario ,bar_shipto ,bar_year ,bar_fiscal_period ,erp_account ,erp_brand_code ,erp_business_area ,erp_company_code ,erp_cost_center ,erp_doc_type ,erp_doc_line_num ,erp_doc_num ,erp_document_text ,erp_vendor ,erp_material ,erp_customer_parent ,erp_posting_date ,erp_quantity ,erp_quantity_uom ,erp_ref_doc_type ,erp_ref_doc_line_num ,erp_ref_doc_num ,erp_profit_center ,erp_sales_group ,erp_sales_office ,erp_customer_ship_to ,erp_customer_sold_to ,erp_plant ,erp_chartaccts, bar_bods_loaddatetime ,bar_bods_record_id ,erp_source ,bar_s_entity_currency ,bar_s_curr_rate_actual ,bar_amt_usd ,etl_crte_ts,etl_crte_user)
	(
	Select REC.src_col_lst  FROM REC.src_tbl_nm  t  LEFT OUTER JOIN global_pl.bar_acct_attr a on UPPER(t.BAR_ACCT) = UPPER(a.bar_account) LEFT OUTER JOIN global_pl.bar_entity_attr b on UPPER(t.BAR_ENTITY) = UPPER(b.bar_entity) LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND UPPER(hfm.bar_function) = UPPER(b.bar_entity_currency) AND UPPER(hfm.BAR_ACCT)='PLRATE')   where t.bar_year in (PREVIOUS_YEAR) and t.bar_period in (PREVIOUS_PERIOD)  
	);*/

	QUERY := 'INSERT INTO  ' || REC.tgt_tbl_nm || '  ' || REC.tgt_col_lst || '(Select ' || REC.src_col_lst || '  FROM ' || REC.src_tbl_nm || '  t  LEFT OUTER JOIN global_pl.bar_acct_attr a on UPPER(t.BAR_ACCT) = UPPER(a.bar_account) LEFT OUTER JOIN global_pl.bar_entity_attr b on UPPER(t.BAR_ENTITY) = UPPER(b.bar_entity) LEFT OUTER JOIN bods.hfm_vw_hfm_actual_trans_current hfm ON (t.bar_year = hfm.bar_year AND t.bar_period = hfm.bar_period AND UPPER(hfm.bar_function) = UPPER(b.bar_entity_currency) AND UPPER(hfm.BAR_ACCT)=''PLRATE'')   where t.bar_year in (' || PREVIOUS_YEAR || ') and t.bar_period in (''' || PREVIOUS_PERIOD || '''))' ;
	EXECUTE QUERY;
	GET DIAGNOSTICS PREVIOUS_PERIOD_INSERTED = ROW_COUNT;
	RAISE NOTICE 'Current Period ROws Inserted :- % ',QUERY;
	
	UPDATE global_pl.status_history_1 SET	prev_mon_rows_ins = PREVIOUS_PERIOD_INSERTED	where run_seq = CURRENT_RUN_SEQ and job_nm = CURRENT_JOB_NAME AND DATE(GETDATE()) = DATE(run_date);
	
	
	UPDATE global_pl.status_history_1 SET
	end_time = GETDATE(),
	job_status = 'COMPLETED'
	where run_seq = CURRENT_RUN_SEQ and job_nm = CURRENT_JOB_NAME AND DATE(GETDATE()) = DATE(run_date);
	
	
	
	
	
  RAISE INFO 'Ran SP(pl_trans_fact_c11_sp) Successfull';
	END LOOP;
END;

$$
;

CREATE OR REPLACE PROCEDURE global_pl.test_framework()
	LANGUAGE plpgsql
AS $$
	
	
  BEGIN
	insert into tttt (name) values('Xyz');
  RAISE INFO 'Ran SP Successfull';
  
END;


$$
;

CREATE OR REPLACE PROCEDURE global_pl.test_sp1()
	LANGUAGE plpgsql
AS $$
	
  BEGIN
	insert into global_pl.testsp (dtrun) values(GETDATE());
  RAISE INFO 'Ran SP Successfull';
  
END;

$$
;

CREATE OR REPLACE FUNCTION global_pl.udf_py_bignum(x float8,y float8)
	RETURNS float8
	LANGUAGE plpythonu
	STABLE
AS $$
	
 if x > y:
 	return x
 return y

$$
;

CREATE OR REPLACE FUNCTION global_pl.udf_sql_bignum($1 float8,$2 float8)
	RETURNS float8
	LANGUAGE sql
	STABLE
AS $$
	
 select case when $1 > $2 then $1
 else $2
 end

$$
;

