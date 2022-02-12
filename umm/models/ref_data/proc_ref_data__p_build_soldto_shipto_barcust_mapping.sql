
CREATE OR REPLACE PROCEDURE ref_data.p_build_soldto_shipto_barcust_mapping()
 LANGUAGE plpgsql
AS $$
--DECLARE variables
BEGIN  
	
	drop table if exists stage_soldtocust_shiptocust_barcust_mapping;
	create temporary table stage_soldtocust_shiptocust_barcust_mapping
	diststyle all
	as 
		with 
			cte_base as (	
				select	distinct 
						cast(lower(soldtocust) as varchar(22)) as soldtocust,
						cast(lower(shiptocust) as varchar(22)) as shiptocust,
						cast(lower(bar_custno) as varchar(30)) as bar_custno,
						cast((case when s.postdate = '' then null else postdate end) as date)  as postdate
				from 	{{ source('bods', 'c11_0ec_pca3') }} s
				inner join ref_data.entity rbh on s.bar_entity = rbh.name
						---only accounts thats contributes to sgm pnl structure
						inner join (
							select 	distinct bar_acct 
							from 	ref_data.pnl_acct
						) acct 
							on 	lower(s.bar_acct) = lower(acct.bar_acct) 
				where s.bar_acct is not null 
					and s.bar_entity is not null 
					and s.bar_acct <> ''
					and rbh.level4 = 'GTS_NA'
--					and s.bar_bu in ('GTS')
--					and s.bar_acct not in ('IGNORE')
--					and s.bar_currtype in ('USD' ,'CAD')
					and case when cast(soldtocust as varchar(50)) = '' then null else  cast(soldtocust as varchar(50)) end is not null 
					and case when cast(shiptocust as varchar(50)) = '' then null else  cast(shiptocust as varchar(50)) end is not null 
					and case when cast(bar_custno as varchar(50)) = '' then null else cast(bar_custno as varchar(50)) end is not null
					and cast((case when s.postdate = '' then null else postdate end) as date)  >= cast('2018-12-30' as date) 
					--and cast('2021-12-01' as date) 
		
					-- TESTING
					--and material = 'CMAS261290'
			)
			,cte_base_next as (
				select 	base.soldtocust,
						base.shiptocust,
						base.bar_custno,
						base.postdate,
						lead(base.bar_custno) over(partition by base.soldtocust, base.shiptocust order by base.postdate) as bar_custno_next
				from 	cte_base base
			)
			,cte_base_historical as (
				select 	nxt.soldtocust,
						nxt.shiptocust,
						nxt.bar_custno,
						nxt.postdate,
						
						nxt.bar_custno_next,
						lead(nxt.postdate) over (partition by nxt.soldtocust, nxt.shiptocust order by nxt.postdate) as postdate_next,
						row_number() over (partition by nxt.soldtocust order by nxt.postdate) rnk
				from 	cte_base_next nxt
				where 	nxt.bar_custno != nxt.bar_custno_next or 
						nxt.bar_custno_next is null
			)
		select 	hist.soldtocust,
				hist.shiptocust,
				hist.bar_custno,				
				case
					when hist.rnk = 1 then cast('1900-01-01' as date) 
					else cast(hist.postdate as date) 
				end as start_date,
				
				case
					when hist.bar_custno_next is null then cast('9999-12-31' as date) 
					else cast(dateadd(day,-1,hist.postdate_next) as date)
				end as end_date,
				
				case when hist.bar_custno_next is null then 1 else 0 end as current_flag,
					
				getdate() as audit_loadts
					
		from 	cte_base_historical as hist
	;

--Select soldtocust, count(1)
--from stage_soldtocust_barcust_mapping
--where current_flag =1 
--group by soldtocust 
--having count(1) >1 ;
----
--Select *
--from stage_soldtocust_shiptocust_barcust_mapping
--Where soldtocust ='0001077198';
--Select *
--from stage_soldtocust_shiptocust_barcust_mapping
--where post
	raise info 'insert into ref_data.soldto_shipto_barcust_mapping';
	truncate table ref_data.soldto_shipto_barcust_mapping;
	insert into ref_data.soldto_shipto_barcust_mapping (
				soldtocust,
				shiptocust,
				bar_custno,
				start_date,
				end_date,
				current_flag,
				audit_loadts
		)
		select 	soldtocust,
				shiptocust,
				bar_custno,
				start_date,
				end_date,
				current_flag,
				getdate() as audit_loadts
		from 	stage_soldtocust_shiptocust_barcust_mapping;
exception
when others then raise exception 'exception occur while ingesting in ref_data.soldto_shipto_barcust_mapping';
end;
$$
;