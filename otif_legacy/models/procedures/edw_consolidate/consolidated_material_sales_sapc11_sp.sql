CREATE OR REPLACE PROCEDURE edw_consolidated.consolidated_material_sales_sapc11_sp()
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

---- need to insert record into EDW.EDW_STAGE.job_master
	v_job_id=20;
	SELECT INTO audit_rec * FROM EDW.EDW_STAGE.job_master WHERE job_id = v_job_id;
	v_start_timestamp:=(select current_timestamp);
	v_source_system=audit_rec.source_sys;
	v_job_name=audit_rec.job_name;
    v_table_name=audit_rec.table_name;
    v_runid=(select pg_backend_pid());
    v_last_extract_timestamp=audit_rec.last_extract_timestamp;
	v_exit_code=-1;
----need to insert record in EDW_STAGE.job_history
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
   
    drop table if exists CONSOLIDATED_MATERIAL_SALES_STG_SAPC11;
   
	CREATE TEMPORARY TABLE CONSOLIDATED_MATERIAL_SALES_STG_SAPC11 AS
	
      Select 'C11' as SOURCE_SYS,
             mvke.MATNR as MATERIAL_CONS,
             mvke.VTWEG as MATERIAL_SALESDIV_CONS,
             mvke.VKORG as MATERIAL_SALESORG_CONS,
             mvke.VMSTA as MATERIAL_CONS_STATUS,
             mvke.LOADDTS as LOADDTS,
             v_job_name as ETL_CRTE_USER,
		 current_timestamp as ETL_CRTE_TS,
		 null as ETL_UPDT_USER,
             cast(null as timestamp) as  ETL_UPDT_TS
             from sapc11.mvke_current as mvke;


    RAISE NOTICE 'TEMPORARY TABLE CREATED CONSOLIDATED_MATERIAL_PLANT_STG';
    
    ----INSERTS & UPDATES----
	delete from EDW.EDW_CONSOLIDATED.CONSOLIDATED_MATERIAL_SALES
	using CONSOLIDATED_MATERIAL_SALES_STG_SAPC11 as material_sales_stg
	where material_sales_stg.MATERIAL_CONS= CONSOLIDATED_MATERIAL_SALES.MATERIAL_CONS
	and material_sales_stg. MATERIAL_SALESORG_CONS= CONSOLIDATED_MATERIAL_SALES. MATERIAL_SALESORG_CONS
	and material_sales_stg. MATERIAL_SALESDIV_CONS = CONSOLIDATED_MATERIAL_SALES. MATERIAL_SALESDIV_CONS
    and material_sales_stg. SOURCE_SYS= CONSOLIDATED_MATERIAL_SALES. SOURCE_SYS
;
	
GET DIAGNOSTICS v_delete_count:= ROW_COUNT;
    RAISE NOTICE 'DELETED RECORDS FROM % % ', v_source_system, v_table_name;

    insert into EDW.EDW_CONSOLIDATED.CONSOLIDATED_MATERIAL_SALES
	select * from CONSOLIDATED_MATERIAL_SALES_STG_SAPC11;
	GET DIAGNOSTICS v_insert_count:= ROW_COUNT;
    v_endtimestamp:=(select current_timestamp);
    RAISE NOTICE 'INSERTED RECORDS INTO % %; RECORD COUNT:%', v_source_system, v_table_name, v_insert_count;
	v_exit_code=0;		

	update edw.edw_stage.job_master
	set job_state=case when v_exit_code=0 then 'SUCCESS' ELSE 'FAILED' end,
	last_extract_timestamp=(select max(loaddts) from EDW.EDW_CONSOLIDATED.CONSOLIDATED_MATERIAL_PLANT where source_sys=v_source_system),
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