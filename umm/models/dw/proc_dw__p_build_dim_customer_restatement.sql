
CREATE OR REPLACE PROCEDURE dw.p_build_dim_customer_restatement()
 LANGUAGE plpgsql
AS $$
BEGIN
	/*
	 * 		drop PROCEDURE dw.p_build_dim_customer_restatement()
	 * 		call dw.p_build_dim_customer_restatement();
	 * 		select count(*) from dw.dim_customer_restatement;
	 * 		grant execute on procedure dw.p_build_dim_customer_restatement() to group "g-ada-rsabible-sb-ro";
	 * 
	 * 
	 * 	STEPS:
	 *      Step 1 - map soldto -> demand group (sapc11/sapp10 kna1_current)                                                              
	 *      Step 2 - add bar customer via demand group (manual mapping table from SSG -> ref_data.demand_group_to_bar_customer_mapping)   
	 *      Step 3 - add bar hierarchy ({{ source('bods', 'drm_customer') }})                                                                        
	 *      Step 4 - add A/B/C hierarchies (sapc11/sapp10 -> knvh_current & kna1_current)                                                 
	 *      Step 5 - add Commercial Hierarchy (manual mapping table from SSG -> ref_data.customer_commercial_hierarchy)                   
	 * 
	 */
	/* ================================================================================
	 * 		MASTER DATA -> SoldTo->DemandGroup
	 * ================================================================================
	 */
	drop table if exists master_soldto_demand_grp_mapping
	;
	create temporary table master_soldto_demand_grp_mapping
	DISTSTYLE ALL
	as
		select 	lower(c11.kunnr) as soldto_num,
				c11.bran1 AS demand_group
		from 	{{ source('sapc11', 'kna1') }} as c11
		where 	coalesce(trim(rtrim(c11.kunnr)),'') != '' and
				coalesce(trim(rtrim(c11.bran1)),'') != ''
	;
	insert into master_soldto_demand_grp_mapping (soldto_num, demand_group)
		select 	lower(p10.kunnr) as soldto_num,
				p10.bran1 AS demand_group
		from 	{{ source('sapp10', 'kna1') }} as p10
				left outer join master_soldto_demand_grp_mapping as mapping
					on 	lower(p10.kunnr) = lower(mapping.soldto_num)
		where 	coalesce(trim(rtrim(p10.kunnr)),'') != '' and
				coalesce(trim(rtrim(p10.bran1)),'') != '' and
				mapping.soldto_num is null
	;
--select * from master_soldto_demand_grp_mapping
--where soldto_num in ('0001013303', '0001013307');
	/*	BASE CASE: soldtos that can be...
	 * 		1. mapped to demand group (via master data)
	 * 			AND
	 * 	 	2. mapped to bar customer via demand group (manual) mapping table 
	 */
	drop table if exists restate_base_case
	;
	create temporary table restate_base_case
	DISTSTYLE ALL
	as
	select 	dmdgrp.soldto_num,
			barcust.bar_customer as bar_custno,
			dmdgrp.demand_group
	from 	master_soldto_demand_grp_mapping as dmdgrp
			inner join ref_data.demand_group_to_bar_customer_mapping as barcust
				on 	lower(barcust.demand_group) = lower(dmdgrp.demand_group)
		and NOT (barcust.demand_group = 'FARM' and barcust.bar_customer = 'TSC')
	;
--select * from restate_base_case
--where soldto_num in ('0001013303', '0001013307');
/* DEBUG: in = out */
--select 	count(*), count(distinct soldto_num) from master_soldto_demand_grp_mapping;
--select 	count(*), count(distinct soldto_num) from restate_base_case;

--select 	*
--from 	restate_base_case
--where 	soldto_num = '0001027455'
	/* ================================================================================
	 * 		EXCEPTIONS 
	 * ================================================================================
	 */
	drop table if exists exceptions_soldto
	;
	create temporary table exceptions_soldto as 
		SELECT 	distinct 
				lower(f.alloc_soldtocust) as soldto_num,
				lower(f.mapped_bar_custno) as bar_custno
		FROM	dw.fact_pnl_commercial_stacked as f
		where 	lower(f.mapped_bar_custno) IN ('customer_none', 'psd_oth') OR 
				lower(f.mapped_bar_custno) LIKE ('%_oth') OR
				lower(f.alloc_soldtocust) IN (
					'adj_royalty',
					'adj_fob_no_cust',
					'adj_fob',
					'adj_rsa',
					'adj_service',
					'adj_rebuild',
					'adj_no_prod',
					'adj_no_cust',
					'adj_psd',
					'',
					'unknown'
				)
	;
--select 	* 
--from 	exceptions_soldto
--where 	soldto_num IN ('0000000068','0001010009')
--;
--select 	*
--from 	exceptions_soldto
--where 	soldto_num = '0001027455'
--
--select 	distinct alloc_soldtocust, mapped_bar_custno
--from 	dw.fact_pnl_commercial_stacked
--where 	alloc_soldtocust = '0001027455'
	/* ================================================================================
	 * 		EDGE CASE MAPPINGS
	 * 		stage.core_tran_delta 
	 * ================================================================================
	 */
	/* 	EDGE CASES: soldtos that could NOT be...
	 * 		1. mapped to demand group (via master data)
	 * 			OR
	 * 		2. mapped to bar customer via demand group (manual) mapping table
	 * 
	 * 	These will be mapped to bar_customer via BODS transactions.
	 * 
	 */
	drop table if exists restate_edge_case_base
	;
	create temporary table restate_edge_case_base as 
		SELECT 	distinct 
				lower(f.alloc_soldtocust) as soldto_num
		FROM	dw.fact_pnl_commercial_stacked as f
				left outer join restate_base_case as base_case
					on 	lower(base_case.soldto_num) = lower(f.alloc_soldtocust)
		WHERE 	lower(base_case.soldto_num) is null
	;
/* DEBUG: in = out */
--select 	count(*), count(distinct soldto_num) from restate_edge_case_base;
--select 	*
--from 	restate_edge_case_base
--where 	soldto_num = '0001042383'

	drop table if exists _bods_trans_phase0_mapping
	;
	create temporary table _bods_trans_phase0_mapping as 
	with
		cte_base as (
			select 	distinct 
					src.soldtocust as soldto_num,
					src.mapped_bar_custno as bar_custno,
					src.postdate
			from 	stage.core_tran_delta_cleansed as src
					left join ref_data.data_processing_rule as dpr
						on 	dpr.dataprocessing_hash = src.mapped_dataprocessing_hash 
			where 	dpr.data_processing_ruleid = 1 
		),
		cte_next as (
			select 	base.soldto_num,
					base.bar_custno,
					base.postdate,
					lead(base.bar_custno) 
						over(partition by base.soldto_num order by base.postdate) as bar_custno_next
			from 	cte_base as base 
		),
		cte_historical as (
			select 	nxt.soldto_num,
					nxt.bar_custno,
					nxt.postdate,
					nxt.bar_custno_next,
					lead(nxt.postdate) 
						over (partition by nxt.soldto_num order by nxt.postdate) as postdate_next,
					row_number() 
						over (partition by nxt.soldto_num order by nxt.postdate) as rnk
			from 	cte_next as nxt 
			where 	nxt.bar_custno != nxt.bar_custno_next or 
					nxt.bar_custno_next is null
		)
		select 	hist.soldto_num,
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
		from 	cte_historical as hist
	;
	drop table if exists restate_edge_case
	;
	create temporary table restate_edge_case 
	DISTSTYLE ALL
	as
		select 	base.soldto_num,
				mapping.bar_custno as bar_custno,
				COALESCE(dmdgrp.demand_group, 'unknown') as demand_group
		from 	(
					select 	distinct soldto_num
					from 	restate_edge_case_base
				) as base
				inner join _bods_trans_phase0_mapping as mapping
					on 	lower(mapping.soldto_num) = lower(base.soldto_num) and
						mapping.current_flag = 1
				left outer join master_soldto_demand_grp_mapping as dmdgrp
					on 	lower(dmdgrp.soldto_num) = lower(base.soldto_num)
	;
--select 	*
--from 	restate_edge_case
--where 	soldto_num = '0001042383'
	/* ================================================================================
	 * 		RESTATEMENT BASE
	 * ================================================================================
	 */
	drop table if exists tmp_restate
	;
	create temporary table tmp_restate 
	DISTSTYLE ALL
	as
	with 
		cte_base as (
			select 	soldto_num,
					bar_custno, 
					demand_group
			from 	restate_edge_case
			union all 
			select 	soldto_num,
					bar_custno, 
					demand_group
			from 	restate_base_case
		)
		select 	cte_base.soldto_num,
				cte_base.bar_custno, 
				cte_base.demand_group
		from 	cte_base
				left outer join exceptions_soldto as ex
					on 	lower(ex.soldto_num) = lower(cte_base.soldto_num) and 
						lower(ex.bar_custno) = lower(cte_base.bar_custno)
		where 	ex.soldto_num is null and 
				lower(cte_base.bar_custno) NOT IN ('customer_none', 'psd_oth') AND 
				lower(cte_base.bar_custno) NOT LIKE ('%_oth') 
	;
--select 	bar_custno
--from 	tmp_restate
--group by bar_custno
--order by 1
--
--select 	* 
--from 	restate_base_case
--where 	soldto_num IN ('0000000068','0001010009')
--;
--
--select 	* 
--from 	tmp_restate
--where 	soldto_num IN ('0000000068','0001010009')
--;
	/* ================================================================================
	 * 		BAR CUSTOMER HIERARCHY
	 * ================================================================================
	 */
	drop table if exists tmp_customer_bar_hierarchy
	;
	create temporary table tmp_customer_bar_hierarchy 
	DISTSTYLE ALL
	as
	with
		cte_cust_current as (
			select 	name as leaf,
					case generation
						when 1  then null
						when 2  then level1 
						when 3  then level2 
						when 4  then level3 
						when 5  then level4 
						when 6  then level5 
						when 7  then level6 
						when 8  then level7 
						when 9  then level8 
						when 10 then level9 
						when 11 then level10 
					end as parent,
					description, 
					membertype,
					cast(generation as int) as generation,
					level1 as level01,
					level2 as level02,
					level3 as level03,
					level4 as level04,
					level5 as level05,
					level6 as level06,
					level7 as level07,
					level8 as level08,
					level9 as level09,
					level10,
					level11
			from 	{{ source('bods', 'drm_customer') }}
			where 	loaddts = (select max(loaddts) from {{ source('bods', 'drm_customer') }})
				and membertype != 'Parent'
		)
		select 	leaf as bar_custno,
				description as bar_customer_desc,
				level01 as bar_customer_level01,
				case when generation <= 2  then case when leaf = 'Customer_None' then leaf else parent end else level02 end as bar_customer_level02,
				case when generation <= 3  then case when leaf = 'Customer_None' then leaf else parent end else level03 end as bar_customer_level03,
				case when generation <= 4  then case when leaf = 'Customer_None' then leaf else parent end else level04 end as bar_customer_level04,
				case when generation <= 5  then case when leaf = 'Customer_None' then leaf else parent end else level05 end as bar_customer_level05,
				case when generation <= 6  then case when leaf = 'Customer_None' then leaf else parent end else level06 end as bar_customer_level06,
				case when generation <= 7  then case when leaf = 'Customer_None' then leaf else parent end else level07 end as bar_customer_level07,
				case when generation <= 8  then case when leaf = 'Customer_None' then leaf else parent end else level08 end as bar_customer_level08,
				case when generation <= 9  then case when leaf = 'Customer_None' then leaf else parent end else level09 end as bar_customer_level09,
				case when generation <= 10 then case when leaf = 'Customer_None' then leaf else parent end else level10 end as bar_customer_level10,
				leaf as bar_customer_level11
		from 	cte_cust_current
		where 	level02 != 'CSToBeRemoved' AND 
				lower(leaf) NOT IN ('customer none', 'psd_oth') AND 
				lower(leaf) NOT LIKE ('%_oth')
	;
	/* ================================================================================
	 * 		COMMERCIAL HIERARCHY
	 * 			ref_data.customer_commercial_hierarchy
	 * 			(this is a flat file in S3)
	 * ================================================================================
	 */
--	select * from ref_data.customer_commercial_hierarchy order by 6,5,4,3,2;
	

	/* ================================================================================
	 * 		A1/A2 Hierarchy
	 * ================================================================================
	 */
--	drop table if exists tmp_A1_hierarchy
--	;
--	create temporary table tmp_A1_hierarchy
--	DISTSTYLE ALL
--	as
--		select 	HA1_id.soldto,
--				HA1_id.hierarchy_a1_id,
--				HA1_desc.hierarchy_a1_desc
--		from 	(
--					select 	distinct 
--							knvh.kunnr as soldto,
--							knvh.hkunnr as hierarchy_a1_id,
--							row_number() over (
--								partition by knvh.kunnr
--								order by knvh.datab desc 
--							) as rnk
--					from 	{{ source('sapc11', 'knvh') }} knvh
--					where 	knvh.HITYP='A' and 
--							knvh.DATBI = '99991231' and 
--							knvh.HVKORG = knvh.VKORG
--				) HA1_id
--				left outer join (
--					select 	distinct 
--							kna1.kunnr as hierarchy_a1_id,
--							kna1.name1 as hierarchy_a1_desc
--					from 	{{ source('sapc11', 'kna1') }} kna1
--				) HA1_desc
--					on 	HA1_desc.hierarchy_a1_id = HA1_id.hierarchy_a1_id
--		where	HA1_id.rnk = 1
--	;
--	insert into tmp_A1_hierarchy (soldto, hierarchy_a1_id, hierarchy_a1_desc)
--		select 	HA1_id.soldto,
--				HA1_id.hierarchy_a1_id,
--				HA1_desc.hierarchy_a1_desc
--		from 	(
--					select 	distinct 
--							knvh.kunnr as soldto,
--							knvh.hkunnr as hierarchy_a1_id,
--							row_number() over (
--								partition by knvh.kunnr
--								order by knvh.datab desc 
--							) as rnk
--					from 	{{ source('sapp10', 'knvh') }} knvh
--					where 	knvh.HITYP='A' and 
--							knvh.DATBI = '99991231' and 
--							knvh.HVKORG = knvh.VKORG
--				) HA1_id
--				left outer join (
--					select 	distinct 
--							kna1.kunnr as hierarchy_a1_id,
--							kna1.name1 as hierarchy_a1_desc
--					from 	{{ source('sapp10', 'kna1') }} kna1
--				) HA1_desc
--					on 	HA1_desc.hierarchy_a1_id = HA1_id.hierarchy_a1_id
--				left outer join tmp_A1_hierarchy mapping
--					on 	lower(mapping.soldto) = lower(HA1_id.soldto)
--		where	HA1_id.rnk = 1 and 
--				mapping.soldto is null
--	;
--	drop table if exists tmp_A2_hierarchy
--	;
--	create temporary table tmp_A2_hierarchy
--	DISTSTYLE ALL
--	as
--		select 	HA2_id.soldto,
--				HA2_id.hierarchy_a2_id,
--				HA2_desc.hierarchy_a2_desc
--		from 	(
--					select 	distinct 
--							knvh.kunnr as soldto,
--							knvh.hkunnr as hierarchy_a2_id,
--							row_number() over (
--								partition by knvh.kunnr
--								order by knvh.datab desc 
--							) as rnk
--					from 	{{ source('sapc11', 'knvh') }} knvh
--					where 	knvh.HITYP='A' and 
--							knvh.DATBI = '99991231' and 
--							knvh.HVKORG = knvh.VKORG
--				) HA2_id
--				left outer join (
--					select 	distinct 
--							kna1.kunnr as hierarchy_a2_id,
--							kna1.name1 as hierarchy_a2_desc
--					from 	{{ source('sapc11', 'kna1') }} kna1
--				) HA2_desc
--					on 	HA2_desc.hierarchy_a2_id = HA2_id.hierarchy_a2_id
--		where	HA2_id.rnk = 1
--	;
--	insert into tmp_A2_hierarchy (soldto, hierarchy_a2_id, hierarchy_a2_desc)
--		select 	HA2_id.soldto,
--				HA2_id.hierarchy_a2_id,
--				HA2_desc.hierarchy_a2_desc
--		from 	(
--					select 	distinct 
--							knvh.kunnr as soldto,
--							knvh.kunnr as hierarchy_a2_id,
--							row_number() over (
--								partition by knvh.kunnr
--								order by knvh.datab desc 
--							) as rnk
--					from 	{{ source('sapp10', 'knvh') }} knvh
--					where 	knvh.HITYP='A' and 
--							knvh.DATBI = '99991231' and 
--							knvh.HVKORG = knvh.VKORG
--				) HA2_id
--				left outer join (
--					select 	distinct 
--							kna1.kunnr as hierarchy_a2_id,
--							kna1.name1 as hierarchy_a2_desc
--					from 	{{ source('sapp10', 'kna1') }} kna1
--				) HA2_desc
--					on 	HA2_desc.hierarchy_a2_id = HA2_id.hierarchy_a2_id
--				left outer join tmp_A2_hierarchy mapping
--					on 	lower(mapping.soldto) = lower(HA2_id.soldto)
--		where	HA2_id.rnk = 1 and
--			 	mapping.soldto is null
--	;

/* 	temp table for A1/A2 hierarchies */
drop table if exists _H2_to_H1;
create table _H2_to_H1 
as (
select 	HA2_id.hierarchy_a2_id,
				HA2_id.hierarchy_a1_id,
				HA2_desc.hierarchy_a1_desc
		from 	(
					select 	distinct 
							knvh.kunnr as hierarchy_a2_id,
							knvh.hkunnr as hierarchy_a1_id,
							row_number() over (
								partition by knvh.kunnr
								order by knvh.datab desc 
							) as rnk
					from 	{{ source('sapc11', 'knvh') }} knvh
					where 	knvh.HITYP='A' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG
				) HA2_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_a1_id,
							kna1.name1 as hierarchy_a1_desc
					from 	{{ source('sapc11', 'kna1') }} kna1
				) HA2_desc
					on 	HA2_desc.hierarchy_a1_id = HA2_id.hierarchy_a1_id
		where	HA2_id.rnk = 1
		--and soldto = '0001710948'
		and hierarchy_a2_id like '00080%'
);
insert into _H2_to_H1 (hierarchy_a2_id, hierarchy_a1_id, hierarchy_a1_desc)
		select 	HA2_id.hierarchy_a2_id ,
				HA2_id.hierarchy_a1_id,
				HA2_desc.hierarchy_a1_desc
		from 	(
					select 	distinct 
							knvh.kunnr as hierarchy_a2_id,
							knvh.hkunnr as hierarchy_a1_id,
							row_number() over (
								partition by knvh.kunnr
								order by knvh.datab desc 
							) as rnk
					from 	{{ source('sapp10', 'knvh') }} knvh
					where 	knvh.HITYP='A' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG
				) HA2_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_a1_id,
							kna1.name1 as hierarchy_a1_desc
					from 	{{ source('sapp10', 'kna1') }} kna1
				) HA2_desc
					on 	HA2_desc.hierarchy_a1_id = HA2_id.hierarchy_a1_id
				left outer join _H2_to_H1 mapping
					on 	lower(mapping.hierarchy_a2_id) = lower(HA2_id.hierarchy_a2_id)
		where	HA2_id.rnk = 1  
				and HA2_id.hierarchy_a2_id like '00080%'
				and mapping.hierarchy_a2_id is null;
			
			
--select * from _H2
drop table if exists _soldto_h2;
create temporary table _soldto_h2 
as
(
	select 	HA1_id.soldto,
			HA1_id.hierarchy_a2_id,
			HA1_desc.hierarchy_a2_desc
		from 	(
					select 	distinct 
							knvh.kunnr as soldto,
							knvh.hkunnr as hierarchy_a2_id,
							row_number() over (
								partition by knvh.kunnr
								order by knvh.datab desc 
							) as rnk
					from 	{{ source('sapc11', 'knvh') }} knvh
					where 	knvh.HITYP='A' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG
				) HA1_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_a2_id,
							kna1.name1 as hierarchy_a2_desc
					from 	{{ source('sapc11', 'kna1') }} kna1
				) HA1_desc
					on 	HA1_desc.hierarchy_a2_id = HA1_id.hierarchy_a2_id
			
		where	HA1_id.rnk = 1
);
insert into _soldto_h2 (soldto,hierarchy_a2_id,hierarchy_a2_desc  )
select 	HA1_id.soldto,
			HA1_id.hierarchy_a2_id,
			HA1_desc.hierarchy_a2_desc
		from 	(
					select 	distinct 
							knvh.kunnr as soldto,
							knvh.hkunnr as hierarchy_a2_id,
							row_number() over (
								partition by knvh.kunnr
								order by knvh.datab desc 
							) as rnk
					from 	{{ source('sapp10', 'knvh') }} knvh
					where 	knvh.HITYP='A' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG
				) HA1_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_a2_id,
							kna1.name1 as hierarchy_a2_desc
					from 	{{ source('sapp10', 'kna1') }} kna1
				) HA1_desc
					on 	HA1_desc.hierarchy_a2_id = HA1_id.hierarchy_a2_id
			left join _soldto_h2 on HA1_id.soldto = _soldto_h2.soldto
		where	HA1_id.rnk = 1
		and _soldto_h2.soldto is null
;
drop table if exists tmp_A_hierarchy;
create temporary table tmp_A_hierarchy 
as (
select _soldto_h2.soldto, _soldto_h2.hierarchy_a2_id, _soldto_h2.hierarchy_a2_desc, _H2_to_H1.hierarchy_a1_id, _H2_to_H1.hierarchy_a1_desc
from _soldto_h2 
left join _H2_to_H1 on _soldto_h2.hierarchy_a2_id = _H2_to_H1.hierarchy_a2_id
);







	/* ================================================================================
	 * 		B Hierarchy
	 * ================================================================================
	 */
	drop table if exists tmp_B_hierarchy
	;
	create temporary table tmp_B_hierarchy
	DISTSTYLE ALL
	as
		select 	HB_id.shipto,
				HB_id.hierarchy_b_id,
				HB_desc.hierarchy_b_desc
		from 	(
					select 	distinct 
							knvh.kunnr as shipto,
							knvh.hkunnr as hierarchy_b_id
					from 	{{ source('sapc11', 'knvh') }} knvh
					where 	knvh.HITYP='B' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG and 
							knvh.VKORG IN ('0020', '0010')
				) HB_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_b_id,
							kna1.name1 as hierarchy_b_desc
					from 	{{ source('sapc11', 'kna1') }} kna1
				) HB_desc
					on 	HB_desc.hierarchy_b_id = HB_id.hierarchy_b_id
	;
	insert into tmp_B_hierarchy (shipto, hierarchy_b_id, hierarchy_b_desc)
		select 	HB_id.shipto,
				HB_id.hierarchy_b_id,
				HB_desc.hierarchy_b_desc
		from 	(
					select 	distinct 
							knvh.kunnr as shipto,
							knvh.hkunnr as hierarchy_b_id
					from 	{{ source('sapp10', 'knvh') }} knvh
					where 	knvh.HITYP='B' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG and 
							knvh.VKORG IN ('0020', '0010')
				) HB_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_b_id,
							kna1.name1 as hierarchy_b_desc
					from 	{{ source('sapp10', 'kna1') }} kna1
				) HB_desc
					on 	HB_desc.hierarchy_b_id = HB_id.hierarchy_b_id
				left outer join tmp_B_hierarchy mapping
					on 	lower(mapping.shipto) = lower(HB_id.shipto)
		where 	mapping.shipto is null
	;
	/* ================================================================================
	 * 		C Hierarchy
	 * ================================================================================
	 */
	drop table if exists tmp_C_hierarchy
	;
	create temporary table tmp_C_hierarchy
	DISTSTYLE ALL
	as
		select 	HC_id.soldto,
				HC_id.hierarchy_c_id,
				HC_desc.hierarchy_c_desc
		from 	(
					select 	distinct 
							knvh.kunnr as soldto,
							knvh.hkunnr as hierarchy_c_id,
							row_number() over (
								partition by knvh.kunnr
								order by knvh.datab desc 
							) as rnk
					from 	{{ source('sapc11', 'knvh') }} knvh
					where 	knvh.HITYP='C' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG and 
							knvh.VKORG IN ('0020', '0010')
				) HC_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_c_id,
							kna1.name1 as hierarchy_c_desc
					from 	{{ source('sapc11', 'kna1') }} kna1
				) HC_desc
					on 	HC_desc.hierarchy_c_id = HC_id.hierarchy_c_id
		where	HC_id.rnk = 1
	;
	insert into tmp_C_hierarchy (soldto, hierarchy_c_id, hierarchy_c_desc)
		select 	HC_id.soldto,
				HC_id.hierarchy_c_id,
				HC_desc.hierarchy_c_desc
		from 	(
					select 	distinct 
							knvh.kunnr as soldto,
							knvh.hkunnr as hierarchy_c_id
					from 	{{ source('sapp10', 'knvh') }} knvh
					where 	knvh.HITYP='C' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG and 
							knvh.VKORG IN ('0020', '0010')
				) HC_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_c_id,
							kna1.name1 as hierarchy_c_desc
					from 	{{ source('sapp10', 'kna1') }} kna1
				) HC_desc
					on 	HC_desc.hierarchy_c_id = HC_id.hierarchy_c_id
				left outer join tmp_C_hierarchy mapping
					on 	lower(mapping.soldto) = lower(HC_id.soldto)
		where 	mapping.soldto is null
	;

	/* ================================================================================
	 * 		Build Final Restatement Table
	 * ================================================================================
	 */
	drop table if exists tmp_restate_final
	;
	create temporary table tmp_restate_final
	DISTSTYLE ALL
	as
		select 	tmp.soldto_num as soldto_number,
				tmp.bar_custno as base_customer,
				
				bar_hier.bar_customer_desc as base_customer_desc,
				bar_hier.bar_customer_level01 as level01_BAR,
				bar_hier.bar_customer_level02 as level02_BAR,
				bar_hier.bar_customer_level03 as level03_BAR,
				bar_hier.bar_customer_level04 as level04_BAR,
				bar_hier.bar_customer_level05 as level05_BAR,
				bar_hier.bar_customer_level06 as level06_BAR,
				bar_hier.bar_customer_level07 as level07_BAR,
				bar_hier.bar_customer_level08 as level08_BAR,
				bar_hier.bar_customer_level09 as level09_BAR,
				bar_hier.bar_customer_level10 as level10_BAR,
				bar_hier.bar_customer_level11 as level11_BAR,
				
				tmp.demand_group,
				
				COALESCE( comm_hier.total_customer, 'unknown' ) as level01_commercial,
				COALESCE( comm_hier.segment, 'unknown' ) as level02_commercial,
				COALESCE( comm_hier.channel, 'unknown' ) as level03_commercial,
				COALESCE( comm_hier.market, 'unknown' ) as level04_commercial,
				COALESCE( comm_hier.major_customer, 'unknown' ) as level05_commercial,
				COALESCE( comm_hier.base_customer, 'unknown' ) as level06_commercial,
				
				COALESCE( a_hier.hierarchy_a2_id,'unknown') as a2,
				COALESCE( a_hier.hierarchy_a1_id,'unknown') as a1,
				COALESCE( a_hier.hierarchy_a2_desc,'unknown') as a2_desc,
				COALESCE( a_hier.hierarchy_a1_desc,'unknown') as a1_desc,
				
				/* these are keyed off of shipto */
--				COALESCE( b_hier.hierarchy_b_id, 'unknown' ) as hierarchy_b_id,
--				COALESCE( b_hier.hierarchy_b_desc, 'unknown' ) as hierarchy_b_desc,
				'unknown' as hierarchy_b_id,
				'unknown' as hierarchy_b_desc,
				
				COALESCE( c_hier.hierarchy_c_id, 'unknown' ) as hierarchy_c_id,
				COALESCE( c_hier.hierarchy_c_desc, 'unknown' ) as hierarchy_c_desc
		from 	tmp_restate as tmp
	
				left outer join tmp_customer_bar_hierarchy as bar_hier
					on 	lower(bar_hier.bar_custno) = lower(tmp.bar_custno)
					
				left outer join ref_data.customer_commercial_hierarchy as comm_hier
					on 	lower(comm_hier.base_customer) = lower(tmp.bar_custno)
				
--				left outer join tmp_A1_hierarchy as a1_hier
--					on 	lower(a1_hier.soldto) = lower(tmp.soldto_num)
--				left outer join tmp_A2_hierarchy as a2_hier
--					on 	lower(a2_hier.soldto) = lower(tmp.soldto_num)
				
				LEFT OUTER JOIN tmp_A_hierarchy	AS a_hier
					on lower(a_hier.soldto) = lower(tmp.soldto_num)
					
--				left outer join tmp_B_hierarchy as b_hier
--					on 	lower(b_hier.shipto) = lower(tmp.soldto)
				left outer join tmp_C_hierarchy as c_hier
					on 	lower(c_hier.soldto) = lower(tmp.soldto_num)
	;

/* DEBUG: counts IN = OUT */
--select count(*), count(distinct soldto_num) from tmp_restate;
--select count(*), count(distinct soldto_number) from tmp_restate_final;


	delete from dw.dim_customer_restatement;
	insert into dw.dim_customer_restatement (
				soldto_number,
				base_customer,
				base_customer_desc,
				level01_BAR,
				level02_BAR,
				level03_BAR,
				level04_BAR,
				level05_BAR,
				level06_BAR,
				level07_BAR,
				level08_BAR,
				level09_BAR,
				level10_BAR,
				level11_BAR,
				demand_group,
				a2,
				a1,
				a2_desc,
				a1_desc,
				level01_commercial,
				level02_commercial,
				level03_commercial,
				level04_commercial,
				level05_commercial,
				level06_commercial,
				hierarchy_b_id,
				hierarchy_b_desc,
				hierarchy_c_id,
				hierarchy_c_desc
		)
		select 	stg.soldto_number,
				stg.base_customer,
				stg.base_customer_desc,
				stg.level01_BAR,
				stg.level02_BAR,
				stg.level03_BAR,
				stg.level04_BAR,
				stg.level05_BAR,
				stg.level06_BAR,
				stg.level07_BAR,
				stg.level08_BAR,
				stg.level09_BAR,
				stg.level10_BAR,
				stg.level11_BAR,
				stg.demand_group,
				stg.a2,
				stg.a1,
				stg.a2_desc,
				stg.a1_desc,
				stg.level01_commercial,
				stg.level02_commercial,
				stg.level03_commercial,
				stg.level04_commercial,
				stg.level05_commercial,
				stg.level06_commercial,
				stg.hierarchy_b_id,
				stg.hierarchy_b_desc,
				stg.hierarchy_c_id,
				stg.hierarchy_c_desc
		from 	tmp_restate_final as stg
	;
/* DEBUG: counts IN = OUT */
--select 	count(distinct soldto_number) from dw.dim_customer dc;
--select 	count(distinct soldto_number) from dw.dim_customer_restatement dc;

exception
when others then raise info 'exception occur while ingesting data in dim_customer_restatement';
end
$$
;