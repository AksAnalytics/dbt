CREATE OR REPLACE PROCEDURE edw_consolidated.consolidated_customer_hierarchy_sapc11_sp()
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
	v_job_id=16;
	SELECT INTO audit_rec * FROM EDW.EDW_STAGE.job_master WHERE job_id = v_job_id;
	v_start_timestamp:=(select current_timestamp);
	v_source_system=audit_rec.source_sys;
	v_job_name=audit_rec.job_name;
    v_table_name=audit_rec.table_name;
    v_runid:=(select pg_backend_pid());
    --v_last_extract_timestamp=audit_rec.last_extract_timestamp;
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
    
   
    drop table if exists edw.edw_stage.consolidated_customer_hierarchy_stg_c11;
	create table edw.edw_stage.consolidated_customer_hierarchy_stg_c11 as
	with  knvh as (
                  select distinct kunnr, hkunnr ,hityp, vkorg , vtweg, spart 
                  from edw.sapc11.knvh_current  knvh
                  where CURRENT_DATE BETWEEN   to_date(knvh.datab, 'YYYYMMDD') AND to_date(knvh.datbi, 'YYYYMMDD')                   
                  )
	, src as (
            select distinct  kna1.kunnr as customer ,knvh.kunnr, knvh.hityp, name1,ktokd, knvh.vkorg , knvh.hkunnr   
            ,  vtweg, spart
            from knvh
            join edw.sapc11.kna1_current  as kna1
            on (kna1.kunnr = knvh.kunnr )            
            )
	select 'C11' as source_sys, * from  src;

    RAISE NOTICE 'STAGE TABLE CREATED';
	
    create temporary table consolidated_customer_hierarchy_c11 as
    with recursive p(source_sys,customer ,kunnr, hityp, name1,ktokd, vkorg , hkunnr, level, vtweg,spart) as
	(
  	select source_sys,customer ,kunnr, hityp, name1,ktokd, vkorg , hkunnr, 1 as level, vtweg,spart
  	from edw.edw_stage.consolidated_customer_hierarchy_stg_c11
  	where ktokd in ('0001','0002') 
  	union all
  	select p.source_sys,p.customer ,p.hkunnr, c.hityp, c.name1,c.ktokd, c.vkorg , c.hkunnr, level+1, c.vtweg, c.spart
  	from edw.edw_stage.consolidated_customer_hierarchy_stg_c11 c,  p
  	where c.kunnr = p.hkunnr and c.hityp = p.hityp and c.vkorg = p.vkorg and c.vtweg = p.vtweg and c.spart = p.spart  
  		and c.source_sys = p.source_sys and level <=7
  	)
  	select source_sys,customer as CUSTOMER_CONS ,kunnr  as PARENT_CONS, hityp as HIER_TYP , name1 as PARENT_NAME
  	,ktokd as ACCT_GRP, vkorg as SALESORG_CONS
  	,((max(level+1) over(partition by source_sys,hityp,customer,vkorg))-level) as HIER_LEVEL
 	,level as demand_group_logic_level
 	, vtweg as SALESDIST_CONS, spart as SALESDIV_CONS,
 	'ETL_USER' as  ETL_CRTE_USER,
	current_timestamp as  ETL_CRTE_TS ,
	null as  ETL_UPDT_USER,
	null as  ETL_UPDT_TS
 	from p 
 	order by customer,hityp;	
 
    RAISE NOTICE 'TEMPORARY TABLE CREATED';
    
    delete from edw.edw_consolidated.consolidated_customer_hierarchy where source_sys='C11';
    GET DIAGNOSTICS v_delete_count:= ROW_COUNT;
    RAISE NOTICE 'DELETED RECORDS FROM % % ', v_source_system, v_table_name;
   
    insert into edw.edw_consolidated.consolidated_customer_hierarchy
    select * from consolidated_customer_hierarchy_c11;
    GET DIAGNOSTICS v_insert_count:= ROW_COUNT;
    v_endtimestamp:=(select current_timestamp);
    RAISE NOTICE 'INSERTED RECORDS INTO % %; RECORD COUNT:%', v_source_system, v_table_name, v_insert_count;
	v_exit_code=0;
    
	update edw.edw_stage.job_master
	set job_state=case when v_exit_code=0 then 'SUCCESS' ELSE 'FAILED' end,
	last_extract_timestamp=current_timestamp,
	etl_updt_ts=current_timestamp
	where job_id=v_job_id;
	RAISE NOTICE 'RECORD UPDATED IN JOB_MASTER';

    update EDW.EDW_STAGE.job_history
	set job_status=case when v_exit_code=0 then 'SUCCESS' ELSE 'FAILED' end,
	end_timestamp=v_endtimestamp,
	insert_count=v_insert_count,
	delete_count=v_delete_count,
	etl_updt_ts=current_timestamp
	where run_id=v_runid and job_id=v_job_id and run_seq=v_run_seq;
    RAISE NOTICE 'RECORD UPDATED IN JOB_HISTORY';
    RAISE NOTICE 'LOAD FOR % % COMPLETED SUCCESSFULLY', v_source_system, v_table_name;
	
    GRANT SELECT  ON ALL TABLES IN SCHEMA OTIF TO GROUP "G-ADA-OTIFOpt-RO";
    
	-----EXCEPTION HANDLING------
	EXCEPTION
       WHEN others THEN
       	  RAISE EXCEPTION 'GOT EXCEPTION:SQLSTATE: % SQLERRM: % FOR JOB: % AND JOB ID:%', SQLSTATE, SQLERRM,v_job_name,v_job_id;
end;



$$
;