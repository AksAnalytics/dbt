
CREATE OR REPLACE PROCEDURE ref_data.p_build_reference_rsa_bible()
 LANGUAGE plpgsql
AS $_$
BEGIN 
	/*
	 * 
	 * 		call ref_data.p_build_reference_rsa_bible ()
	 * 		select source_system, count(*) from ref_data.rsa_bible group by source_system;
	 */
	
	DROP TABLE IF EXISTS rsa_bible_us
	;
	CREATE TEMPORARY TABLE rsa_bible_us (
		region 				varchar(30),
		demand_group		varchar(30),
		customer			varchar(50),
		division 			varchar(30),
		brand				varchar(30),
		sku					varchar(50),
		yr					int,
		month_num			int,
		amt					varchar(30), 	
		pcr	 				varchar(300),
		mgsv  				varchar(30)
	) diststyle all
	;
	
	insert into rsa_bible_us (
				region,
				demand_group,
				customer,
				division,
				brand,
				sku,
				yr,
				month_num,
				amt,
				pcr,
				mgsv
		)
		select 	rc.region,
				rc.demandgroup,
				rc.customer,
				rc.division,
				rc.brand,
				rc.sku,
				cast(rc.year as integer) as year,
				cast(rc.period as integer) as period,
				rc.rsa_amt,
				rc.pcr,
				rc.mgsv
		from 	{{ source('sftpgtsi', 'rsabible') }} rc 
		where 	lower(rc.region) = 'us'
	;

	DROP TABLE IF EXISTS rsa_bible_cad
	;
	CREATE TEMPORARY TABLE rsa_bible_cad (
		region 				varchar(30),
		demand_group		varchar(30),
		customer			varchar(50),
		division 			varchar(30),
		brand				varchar(30),
		sku					varchar(50),
		yr					int,
		month_num			int,
		amt					varchar(30), 	
		pcr	 				varchar(300),
		mgsv  				varchar(30)
	) diststyle all
	;
	
	insert into rsa_bible_cad (
				region,
				demand_group,
				customer,
				division,
				brand,
				sku,
				yr,
				month_num,
				amt,
				pcr,
				mgsv
		)
		select 	rc.region,
				rc.demandgroup,
				rc.customer,
				rc.division,
				rc.brand,
				rc.sku,
				cast(rc.year as integer) as year,
				cast(rc.period as integer) as period,
				rc.rsa_amt,
				rc.pcr,
				rc.mgsv
		from 	{{ source('sftpgtsi', 'rsabible') }} rc 
		where 	lower(rc.region) = 'cad'
	;
	delete from  ref_data.rsa_bible;
	insert into ref_data.rsa_bible (
				source_system,
				demand_group,
				division,
				brand,
				sku,
				fiscal_month_id,
				amt,
				amt_str,
				pcr,
				mgsv
		)
		select 	'rsa_bible_cad' as source_system,
				ltrim(rtrim(demand_group)) as demand_group,
				case 
					when ltrim(rtrim(division)) = '' then null 
					else right('0' || ltrim(rtrim(division)), 2)
				end as division,
				ltrim(rtrim(brand)) as brand,
				ltrim(rtrim(sku)) as sku,
				((yr * 100) + month_num) as fiscal_month_id,
				CASE 
					when ltrim(rtrim(replace(amt,'$',''))) = '-' then 0.0
					when ltrim(rtrim(replace(amt,'$',''))) = '' then null
					when charindex(')', ltrim(rtrim(amt))) > 0 then 
						cast(replace(replace(replace(replace(ltrim(rtrim(amt)),',','' ),'(',''),')',''),'$','') as decimal(38,8) ) * -1
					else 
						cast(replace(replace(replace(replace(ltrim(rtrim(amt)),',','' ),'(',''),')',''),'$','') as decimal(38,8) )
				END as amt,
				ltrim(rtrim(amt)) as amt_str,
				ltrim(rtrim(pcr)) as pcr,
				ltrim(rtrim(mgsv)) as mgsv
		from 	rsa_bible_cad
		where 	((yr * 100) + month_num) is not null
		union all
		select 	'rsa_bible_us' as source_system,
				ltrim(rtrim(demand_group)) as demand_group,
				case 
					when ltrim(rtrim(division)) = '' then null 
					else right('0' || ltrim(rtrim(division)), 2)
				end as division,
				ltrim(rtrim(brand)) as brand,
				ltrim(rtrim(sku)) as sku,
				((yr * 100) + month_num) as fiscal_month_id,
				CASE 
					when ltrim(rtrim(replace(amt,'$',''))) = '-' then 0.0
					when ltrim(rtrim(replace(amt,'$',''))) = '' then null
					when charindex(')', ltrim(rtrim(amt))) > 0 then 
						cast(replace(replace(replace(replace(ltrim(rtrim(amt)),',','' ),'(',''),')',''),'$','') as decimal(38,8) ) * -1
					else 
						cast(replace(replace(replace(replace(ltrim(rtrim(amt)),',','' ),'(',''),')',''),'$','') as decimal(38,8) )
				END as amt,
				ltrim(rtrim(amt)) as amt_str,
				ltrim(rtrim(pcr)) as pcr,
				ltrim(rtrim(mgsv)) as mgsv
		from 	rsa_bible_us
		where 	((yr * 100) + month_num) is not null
	;
end  
$_$
;

CREATE OR REPLACE PROCEDURE ref_data.p_build_sku_barbrand_mapping()
 LANGUAGE plpgsql
AS $$
BEGIN 
	/*  create mapping table for material -> bar_brand 
	 * 	based on historical transactions
	 */
	drop table if exists stage_sku_barbrand_mapping
	;
	create temporary table stage_sku_barbrand_mapping
	diststyle all
	as 
		with 
			cte_base as (	
				select	distinct 
						cast(material as varchar(30)) as material,
						cast(bar_brand as varchar(14)) as bar_brand,
						cast((case when s.postdate = '' then null else postdate end) as date)  as postdate
				from 	{{ source('bods', 'c11_0ec_pca3') }} s
				inner join ref_data.entity rbh on s.bar_entity = rbh.name
						---only accounts thats contributes to sgm pnl structure
						inner join (
							select 	distinct bar_acct 
							from 	ref_data.pnl_acct
						) acct 
							on 	lower(s.bar_acct) = lower(acct.bar_acct) 
				where 	s.bar_acct is not null 
					and s.bar_entity is not null 
					and s.bar_acct <> ''
					and rbh.level4 = 'GTS_NA'
					--and s.bar_bu in ('GTS')
--					and s.bar_acct not in ('IGNORE')
--					and s.bar_currtype in ('USD' ,'CAD')
					and case when cast(material as varchar(30)) = '' then null else  cast(material as varchar(30)) end is not null 
					and cast((case when s.postdate = '' then null else postdate end) as date)  >= cast('2018-12-30' as date)  
		
					-- TESTING
					--and material = 'CMAS261290'
			)
			,cte_base_next as (
				select 	base.material,
						base.bar_brand,
						base.postdate,
						lead(base.bar_brand) over(partition by base.material order by base.postdate) as bar_brand_next
				from 	cte_base base
			)
			,cte_base_historical as (
				select 	nxt.material,
						nxt.bar_brand,
						nxt.postdate,
						
						nxt.bar_brand_next,
						lead(nxt.postdate) over (partition by nxt.material order by nxt.postdate) as postdate_next,
						row_number() over (partition by nxt.material order by nxt.postdate) rnk
				from 	cte_base_next nxt
				where 	nxt.bar_brand != nxt.bar_brand_next or 
						nxt.bar_brand_next is null
			)
		select 	hist.material,
				hist.bar_brand,				
		--		hist.postdate,
		--		hist.bar_brand_next,
		--		hist.rnk,
				case
					when hist.rnk = 1 then cast('1900-01-01' as date) 
					else cast(hist.postdate as date) 
				end as start_date,
				
				case
					when hist.bar_brand_next is null then cast('9999-12-31' as date) 
					else cast(dateadd(day,-1,hist.postdate_next) as date)
				end as end_date,
				
				case when hist.bar_brand_next is null then 1 else 0 end as current_flag,
					
				getdate() as audit_loadts
					
		from 	cte_base_historical as hist
	;
--	select * from stage_sku_barbrand_mapping
--	where material = 'CMAS261290'
	drop table if exists stage_sku_barbrand_mapping_gpp; 
	create temporary table stage_sku_barbrand_mapping_gpp
	diststyle all
	as  
	SELECT  distinct cast(cmac.matnr as varchar(30)) as material,
				  brnd.wgbez as brand,
				  cast('1900-01-01' as date) start_date, 
				  cast('12-31-9999' as date) end_date, 
				  1 as current_flag,
				  getdate() as audit_loadts
	FROM 	{{ source('sapc11', 'mara') }} cmac 
	left join {{ source('sapc11', 't023t') }} brnd on cmac.matkl = brnd.matkl 
	WHERE 	'P' + SPLIT_PART(cmac.wrkst, '-', 4) is not null
	--and left(brnd.matkl,1) = 'T'
	and brnd.spras = 'E'
	and brnd.wgbez is not null;

	delete from ref_data.sku_barbrand_mapping
	;
	---insert materials from gpp hierarchy, and left overs from c11 transactions
	insert into ref_data.sku_barbrand_mapping (
				material,
				bar_brand,
				start_date,
				end_date,
				current_flag,
				audit_loadts
		)
		select 	material,
				brand,
				start_date,
				end_date,
				current_flag,
				getdate() as audit_loadts
		from 	stage_sku_barbrand_mapping_gpp 
	;
	
	---Add leftovers now 
		insert into ref_data.sku_barbrand_mapping (
				material,
				bar_brand,
				start_date,
				end_date,
				current_flag,
				audit_loadts
		)
		select 	tran_based.material,
				tran_based.bar_brand,
				tran_based.start_date,
				tran_based.end_date,
				tran_based.current_flag,
				getdate() as audit_loadts
		from 	stage_sku_barbrand_mapping  tran_based 
		left join stage_sku_barbrand_mapping_gpp gpp_based on tran_based.material = gpp_based.material 
		where gpp_based.material is null
	;
	exception
		when others then raise info 'exception occur while ingesting data in ref_data.barcust_soldto_mapping';
end
$$
;