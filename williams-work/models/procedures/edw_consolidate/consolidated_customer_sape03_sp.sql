
CREATE OR REPLACE PROCEDURE edw_consolidated.consolidated_customer_sape03_sp()
	LANGUAGE plpgsql
AS $$
	
	
declare
	v_job_name varchar(100);
    v_table_name varchar(100);
    v_source_system varchar(50);
	v_job_id int;
	v_exit_code int;
    v_runid int;
	v_start_timestamp timestamp;
	v_endtimestamp timestamp;
    v_insert_count bigint;
    v_delete_count bigint;
    audit_rec RECORD;
    v_last_extract_timestamp timestamp;
    v_run_seq int;
begin
	v_job_id=11;
	SELECT INTO audit_rec * FROM EDW.EDW_STAGE.job_master WHERE job_id = v_job_id;
	v_start_timestamp:=(select current_timestamp);
	v_source_system=audit_rec.source_sys;
	v_job_name=audit_rec.job_name;
    v_table_name=audit_rec.table_name;
    v_runid:=(select pg_backend_pid());
    v_last_extract_timestamp=audit_rec.last_extract_timestamp;
	v_exit_code=-1;
    v_run_seq:=(select coalesce((max(run_seq)+1),1) from EDW_STAGE.job_history where job_id=v_job_id and run_id=v_runid);
    
    ---Entry Into Job History Table----
    insert into EDW.EDW_STAGE.job_history
	 select  
	 v_runid as run_id
	,v_job_id as job_id
	,v_job_name
	,v_table_name
	,to_date(GETDATE(), 'yyyy-MM-DD HH24:MI:SS') run_date
	,v_start_timestamp
	,null as end_timestamp
	,v_run_seq run_seq
	,'STARTED' as job_status
	,'ETL_USER' as etl_crte_user
	,current_timestamp etl_crte_ts
	,'ETL_USER' as etl_updt_user
	,null etl_updt_ts;
	
    COMMIT;
    RAISE NOTICE 'RECORD INSERTED INTO JOB_HISTORY';
    
    drop table if exists CONSOLIDATED_CUSTOMER_STG_SAPE03;
   
	CREATE TEMPORARY table CONSOLIDATED_CUSTOMER_STG_SAPE03 AS
	with txt_cust_class as (
		select distinct * from edw_stage.texts where field = 'cust_class' and source_sys='E03'
					   )
	select 
	'E03' as  SOURCE_SYS,
	KNA1.KUNNR as CUSTOMER_CONS,
	KNA1.BRSCH as INDUSTKEY_CONS,
	KNA1.KTOKD as ACCGROUP_CONS,
	KNA1.KUKLA as CUSTCLASS_CONS,
	KNA1.LAND1 as COUNTRY_CONS,
	KNA1.NAME1 as NAME_CONS,
	KNA1.ORT01 as CITY_CONS,
	KNA1.REGIO as REGION_CONS,
	txt_cust_class.CODE_TXT as CUSTCLASS_CONS_TXT,
	KNA1.LOADDTS as LOADDTS,
	v_job_name as  ETL_CRTE_USER,
	current_timestamp as  ETL_CRTE_TS ,	
	null as  ETL_UPDT_USER,	
	cast(null as timestamp) as  ETL_UPDT_TS	
	from sape03.kna1_current KNA1 
	LEFT JOIN txt_cust_class ON txt_cust_class.code= KNA1.KUKLA
    where KNA1.loaddts>v_last_extract_timestamp;
    RAISE NOTICE 'TEMPORARY TABLE CREATED CONSOLIDATED_CUSTOMER_STG';
    
    ----INSERTS & UPDATES----
	delete from EDW.EDW_CONSOLIDATED.CONSOLIDATED_CUSTOMER
	using CONSOLIDATED_CUSTOMER_STG_SAPE03 customer_stg
	where customer_stg.CUSTOMER_CONS=CONSOLIDATED_CUSTOMER.CUSTOMER_CONS
	and customer_stg.source_sys=CONSOLIDATED_CUSTOMER.source_sys;
	GET DIAGNOSTICS v_delete_count:= ROW_COUNT;
    RAISE NOTICE 'DELETED RECORDS FROM % % ', v_source_system, v_table_name;

    insert into EDW.EDW_CONSOLIDATED.CONSOLIDATED_CUSTOMER
	select * from CONSOLIDATED_CUSTOMER_STG_SAPE03;
	GET DIAGNOSTICS v_insert_count:= ROW_COUNT;
    v_endtimestamp:=(select current_timestamp);
    RAISE NOTICE 'INSERTED RECORDS INTO % %; RECORD COUNT:%', v_source_system, v_table_name, v_insert_count;
	v_exit_code=0;		

	update edw.edw_stage.job_master
	set job_state=case when v_exit_code=0 then 'SUCCESS' ELSE 'FAILED' end,
	last_extract_timestamp=(select max(loaddts) from EDW.EDW_CONSOLIDATED.CONSOLIDATED_CUSTOMER where source_sys=v_source_system),
	etl_updt_ts=current_timestamp
	where job_id=v_job_id;
	RAISE NOTICE 'RECORD UPDATED IN JOB_MASTER';

    update EDW.EDW_STAGE.job_history
	set job_status=case when v_exit_code=0 then 'SUCCESS' ELSE 'FAILED' end,
	end_timestamp=v_endtimestamp,
	etl_updt_ts=current_timestamp,
	insert_count=v_insert_count,
	delete_count=v_delete_count
	where run_id=v_runid and job_id=v_job_id and run_seq=v_run_seq;
    RAISE NOTICE 'RECORD UPDATED IN JOB_HISTORY';
    RAISE NOTICE 'DELTA LOAD FOR % % COMPLETED SUCCESSFULLY', v_source_system, v_table_name;
	
    GRANT SELECT  ON ALL TABLES IN SCHEMA OTIF TO GROUP "G-ADA-OTIFOpt-RO";
   
	-----EXCEPTION HANDLING------
	EXCEPTION
       WHEN others THEN
       	  RAISE EXCEPTION 'GOT EXCEPTION:SQLSTATE: % SQLERRM: % FOR JOB: % AND JOB ID:%', SQLSTATE, SQLERRM,v_job_name,v_job_id;          
end;


$$
;
