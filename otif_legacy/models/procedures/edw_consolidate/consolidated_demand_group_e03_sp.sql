CREATE OR REPLACE PROCEDURE edw_consolidated.consolidated_demand_group_e03_sp()
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
	v_job_id=15;
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
   
    drop table if exists  edw.edw_stage.demand_group_e03_determination_stg;
	create table edw.edw_stage.demand_group_e03_determination_stg as
	with 
	co as (
	        select distinct source_sys, salesorg_cons , soldto_cons , material_cons
			from edw.edw_consolidated.consolidated_orders  
			where source_sys  = 'E03'
		  )
 , cc as (
			select  distinct customer_cons ,country_cons , industkey_cons
			from edw.edw_consolidated.consolidated_customer   
			where source_sys  = 'E03'
		 )
 , ccs as (
			select distinct customer_cons ,sales_office_cons
			, salesdiv_cons , salesdist_cons , salesorg_cons
			from edw.edw_consolidated.consolidated_customer_sales   
			where source_sys  = 'E03'
		  )
 , h as (
			select *
			from edw.edw_consolidated.consolidated_customer_hierarchy
			where source_sys  = 'E03'
			and hier_typ ='B'
		)
 , m as (
			select distinct material_cons , prodhl1_e03 , prodhl2_e03 , brand_cons
			from edw.edw_consolidated.consolidated_material cm 
			where source_sys  = 'E03'
		)
		select distinct co.salesorg_cons , co.soldto_cons ,h.customer_cons,h.HIER_TYP,h.PARENT_NAME,h.PARENT_CONS,h.ACCT_GRP,h.HIER_LEVEL,h.demand_group_logic_level,
		cc.country_cons ,cc.industkey_cons,ccs.sales_office_cons,m.prodhl1_e03 ,m.prodhl2_e03 ,m.brand_cons , m.material_cons
		from co 
		join cc on (cc.customer_cons =  co.soldto_cons)
		join ccs on (ccs.customer_cons =  co.soldto_cons)
		join ccs as ccs_salesorg on (co.soldto_cons =ccs_salesorg.customer_cons and  ccs_salesorg.salesorg_cons=co.salesorg_cons)
		join h on (h.customer_cons =  co.soldto_cons and h.salesorg_cons=co.salesorg_cons)
		join m on (m.material_cons =  co.material_cons);
	
	  RAISE NOTICE 'STAGE TABLE 1 CREATED';
		
		
		drop table if exists edw.edw_stage.demand_group_e03_determination_stg1;
		create table edw.edw_stage.demand_group_e03_determination_stg1 as
		select 
			MANDT,
			zseqid as SEQID	,
			zseqmch as SEQMCHID,
			VKORG,
			VTWEG,
			zbrand as brand_cons,
			land1  as country_cons,
			VKBUR  as sales_office_cons,
			BRSCH  as industkey_cons,
			kunag  as KUNNR,
			zphsbu as prodhl1_e03,
			zphdivi as prodhl2_e03,
			zzdemandgrp  as ZDMDGRP,
			coalesce(kunnr, '') 
			|| coalesce(prodhl1_e03, '') 
			||coalesce(prodhl2_e03, '')
			||coalesce(brand_cons, '') 
			||coalesce(country_cons, '')
			||coalesce(sales_office_cons, '') 
			||coalesce(industkey_cons, '') as zdmdgrp_factors,
			row_number() over (partition by kunag order by zseqid,zseqmch) as rn
		from edw.sape03.zdspdmseqmch_current zc ;
		
		RAISE NOTICE 'STAGE TABLE 2 CREATED';
		
		drop table if exists edw.edw_stage.demand_group_e03_determination_stg2;
		create table edw.edw_stage.demand_group_e03_determination_stg2 as
	
		with a as(
		select  *
		from edw.edw_stage.demand_group_e03_determination_stg1 a
		
		)
		, b as (
		select  *  from edw.edw_stage.demand_group_e03_determination_stg b
		)
		
		, dmnd_grp as (
		select distinct 
		(  ( case when  a.kunnr is null then '' else  coalesce(b.PARENT_CONS,'')  end)
		|| ( case when a.prodhl1_e03 is null then '' else coalesce(b.prodhl1_e03,'') end )
		|| ( case when a.prodhl2_e03 is null then '' else coalesce(substring(b.prodhl2_e03,2,2),'') end )
		|| ( case when a.brand_cons is null then '' else coalesce(b.brand_cons,'') end )
		|| ( case when a.country_cons is null then '' else coalesce(b.country_cons,'') end )
		|| ( case when a.sales_office_cons is null then '' else coalesce(b.sales_office_cons,'') end )
		|| ( case when a.industkey_cons is null then '' else coalesce(b.industkey_cons,'') end )
		) as zdmdgrp_factors_trans
		,b.material_cons
		,b.CUSTOMER_CONS, b.salesorg_cons
		,b.PARENT_CONS as kunnr_trans
		,a.*
		,b.hier_typ
		,b.HIER_LEVEL
		,b.demand_group_logic_level
		from a 
		join b 
		on ( 
			(a.kunnr=b.PARENT_CONS  or  a.kunnr is null) and  (a.VKORG=b.salesorg_cons )		
			))
		
		,min_dgrp as (
		select * from(
		select *
		, min(rn+demand_group_logic_level) over (partition by customer_cons,material_cons,salesorg_cons,hier_typ) 
		 as dgrp
		 
		from dmnd_grp
		where zdmdgrp_factors = zdmdgrp_factors_trans 
		)
		where dgrp = (rn+demand_group_logic_level))
	    
	   
	   select * from(
	   select *,ROW_NUMBER() OVER (PARTITION BY customer_cons,material_cons,salesorg_cons,hier_typ ORDER BY demand_group_logic_level asc) as rownum
	   from min_dgrp)
	   where rownum=1;	
		
		RAISE NOTICE 'STAGE TABLE 3 CREATED';
	    
		delete from edw.edw_consolidated.consolidated_demand_group where source_sys='E03';
	    GET DIAGNOSTICS v_delete_count:= ROW_COUNT;
        RAISE NOTICE 'DELETED RECORDS FROM % % ', v_source_system, v_table_name;
	
	    insert into edw.edw_consolidated.consolidated_demand_group
	 	select 'E03' as Source_Sys,
	 	customer_cons as customer_cons,
	 	salesorg_cons,
	 	material_cons,
	 	zdmdgrp as demand_group,
	 	v_job_name as ETL_CRTE_USER,
	 	current_timestamp ETL_CRTE_TS ,
	 	null as ETL_UPDT_USER,
	 	null ETL_UPDT_TS
	 	from edw.edw_stage.demand_group_e03_determination_stg2;
	    
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