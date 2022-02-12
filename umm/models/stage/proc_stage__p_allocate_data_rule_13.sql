
CREATE OR REPLACE PROCEDURE stage.p_allocate_data_rule_13(fmthid integer)
 LANGUAGE plpgsql
AS $$
BEGIN   
	
	--TESTING
	--delete from stage.sgm_allocated_data_rule_13;
	--call stage.p_allocate_data_rule_13 (202007)
	--call stage.p_allocate_data_rule_13 (202006)
	--select count(*) from stage.sgm_allocated_data_rule_13
	--select fiscal_month_id, count(*) from stage.sgm_allocated_data_rule_13 group by fiscal_month_id order by 1
/*
 *	This procedure manages the allocations for Rule ID #13
 *
 *		Allocation Group: Product - Partial Customer 
 *		Known: 	 sku, bar_product, & bar_custno
 *		Unknown: soldto (shipto)
 *
 * 		Final Table(s): 
 *			stage.sgm_allocated_data_rule_13
 *
 * 		Rule Logic:	
 * 			Allocate to all past historical shipto, soldto combinations 
 * 			for historical records of SKU purchase for soldtos within 
 *			bar customer hierarchy
 *
 *		Implementation Steps:
 * 			Part 01: Allocate transactions across soldtos found in the 
 *					 base rate table for the same bar_custno, material, 
 *					 & bar_product
 *  		Part 02: Capture Leakage
 *			Part 03: Load results into stage.sgm_allocated_data_rule_13
 *
 */
	
	/* create temp table for selected period */
	drop table if exists vtbl_date_range
	;
	create temporary table vtbl_date_range as 
		select 	cast(min(dt.dy_dte) as date) as range_start_date ,
				cast(max(dt.dy_dte) as date) as range_end_date
		from 	ref_data.calendar dt
		where 	dt.fmth_id = fmthid
	;
	/* copy transactions to be allocated from bods_tran_agg */
	drop table if exists _trans_unalloc
	;
	create temporary table _trans_unalloc as 
		select 	 tran.audit_rec_src as source_system
				,tran.org_tranagg_id
				,tran.posting_week_enddate
				,tran.fiscal_month_id
				,tran.bar_entity
				,tran.bar_acct
				
				,tran.org_bar_brand
				,tran.org_bar_custno
				,tran.org_bar_product
				
				,tran.mapped_bar_brand
				,tran.mapped_bar_custno
				,tran.mapped_bar_product
				
				,tran.shiptocust
				,tran.soldtocust as org_soldtocust
				,tran.material
				
				,tran.bar_currtype
				
				,tran.bar_amt as unallocated_bar_amt
				
				,tran.org_dataprocessing_ruleid
				,tran.mapped_dataprocessing_ruleid
				
		from 	stage.bods_core_transaction_agg as tran
				inner join ref_data.data_processing_rule as dpr
					on  dpr.data_processing_ruleid = tran.mapped_dataprocessing_ruleid 
				inner join vtbl_date_range as dt_rng
					on 	tran.posting_week_enddate between dt_rng.range_start_date and dt_rng.range_end_date
		where 	0=0
			and dpr.data_processing_ruleid = 13
			and tran.audit_rec_src in  ('sap_c11', 'sap_lawson', 'sap_p10')
			
			/* filter for examples */
--			and tran.bar_custno in ('rona')
--			and tran.bar_custno in ('rona', 'ace')
--			and tran.bar_custno in ('rona', 'ace', 'ind_oth')
--			and tran.bar_custno in ('rona', 'retail_oth')
	;
--	/* create list of unique bar_custno from unallocated trans */
--	drop table if exists _list_BarCust
--	;
--	create temporary table _list_BarCust as 
--		select 	distinct trans.mapped_bar_custno as bar_custno
--		from 	_trans_unalloc trans
--	;

	/* create list of unique bar_custno|material|bar_product from unallocated trans */
	drop table if exists _list_BarCust_Material
	;
	create temporary table _list_BarCust_Material as 
		select 	distinct 
				trans.mapped_bar_custno as bar_custno, 
				trans.material, 
				trans.mapped_bar_product as bar_product,
				trans.bar_currtype, 
				trans.source_system
		from 	_trans_unalloc trans
	;
	/* grab subset of rate base table w/ matching bar_custno, bar_product, material */
	drop table if exists _rate_rule13_part01
	;
	create temporary table _rate_rule13_part01 as 
		select 	rb.bar_entity,
				rb.soldtocust,
				rb.bar_custno,
				rb.material,
				rb.bar_product,
				rb.total_bar_amt,
				rb.bar_currtype,
				rb.source_system,
				sum(rb.total_bar_amt) 
					over( 
						partition by
							rb.bar_entity,
							rb.bar_custno, 
							rb.material, 
							rb.bar_product,
							rb.bar_currtype,
							rb.source_system
					) as total_bar_amt_bar_custno 
		from 	_list_BarCust_Material as bcm
				inner join stage.rate_base rb 
					on 	rb.bar_custno = bcm.bar_custno and 
						rb.material = bcm.material and 
						rb.bar_product = bcm.bar_product and 
						rb.bar_currtype  = bcm.bar_currtype and
						rb.source_system  = bcm.source_system
				inner join vtbl_date_range dt 
					on 	dt.range_start_date <= rb.range_start_date and 
						dt.range_end_date >= rb.range_end_date 
	;
	/* Part 01: Allocations @ bar_custno */
	drop table if exists _part01_allocated_trans_rate
	;
	create temporary table _part01_allocated_trans_rate as 
		select 	 tran.source_system
				,tran.org_tranagg_id
				,tran.posting_week_enddate
				,tran.fiscal_month_id
				,tran.bar_entity
				,tran.bar_acct
				
				,tran.org_bar_brand
				,tran.org_bar_custno
				,tran.org_bar_product
				,tran.mapped_bar_brand
				,tran.mapped_bar_custno
				,tran.mapped_bar_product
				
				,tran.shiptocust
				,tran.org_soldtocust
				,tran.material
				
				,rb.soldtocust as alloc_soldtocust
				
				,tran.bar_currtype
				,tran.org_dataprocessing_ruleid
				,tran.mapped_dataprocessing_ruleid
				
				,tran.unallocated_bar_amt
				
				,rb.total_bar_amt
				,rb.total_bar_amt_bar_custno
			
				,(rb.total_bar_amt / rb.total_bar_amt_bar_custno) as rate_p01
				
				,tran.unallocated_bar_amt * 
					(rb.total_bar_amt / rb.total_bar_amt_bar_custno) as allocated_bar_amt
				
		from 	_trans_unalloc as tran 
				inner join _rate_rule13_part01 as rb 
					on  rb.bar_custno = tran.mapped_bar_custno and
						rb.material = tran.material and 
						rb.bar_product = tran.mapped_bar_product and 
						rb.bar_entity = tran.bar_entity and 
						rb.bar_currtype = tran.bar_currtype and 
						rb.source_system = tran.source_system
		where 	rb.total_bar_amt_bar_custno != 0
	;
	-- create list of transactions (org_tranagg_id)
	drop table if exists _part01_allocated_trans
	;
	create temporary table _part01_allocated_trans as 
		select 	distinct org_tranagg_id
		from 	_part01_allocated_trans_rate
	;

-- 	/* validation 01: allocated = unallocated on the transactions that were allocated */
--	select 	'Allocated' as resultset, sum(allocated_bar_amt) as amt
--	from 	_part01_allocated_trans_rate
--	union all
--	select 	'Unallocated' as resultset, sum(unallocated_bar_amt) as amt
--	from 	_trans_unalloc tr
--			inner join _part01_allocated_trans tra 
--				on 	tra.org_tranagg_id = tr.org_tranagg_id
--	;
--
-- 	/* validation 02: allocations != 100% */
--	select 	alloc_trans.org_tranagg_id, sum(alloc_trans.rate_p01)
--	from 	_part01_allocated_trans_rate alloc_trans
--	group by alloc_trans.org_tranagg_id
--	having 	round(abs(sum(alloc_trans.rate_p01) * 100),0) != 100
--	order by sum(alloc_trans.rate_p01)
--	;
	

/* ------------------------------------------------------------------ 
 * 	Part 02: Capture Leakage
 * ------------------------------------------------------------------
 */
	drop table if exists _part02_leakage
	;
	/* Part 04: transactions that couldn't be allocated in previous 3 parts 
	 * 		i.e. no combination of bar_custno/material in rate_base
	 */
	create temporary table _part02_leakage as 
		select 	org_tranagg_id
		from 	_trans_unalloc
		except (
			select 	org_tranagg_id from _part01_allocated_trans
		)
	;
/* ------------------------------------------------------------------ 
 * 	Part 03: Load results into stage.sgm_allocated_data_rule_13
 * ------------------------------------------------------------------
 */
	/* remove any existing transactions for the current batch being processed */
	delete 
	from 	stage.sgm_allocated_data_rule_13
	where 	posting_week_enddate between 
			(select range_start_date from vtbl_date_range) and 
			(select range_end_date from vtbl_date_range)
	;
	/* load allocated transactions */
	insert into stage.sgm_allocated_data_rule_13 (
	
				source_system,
				org_tranagg_id,
				
				posting_week_enddate,
				fiscal_month_id,
				
				bar_entity,
				bar_acct,
				
				org_bar_brand,
				org_bar_custno,
				org_bar_product,
				mapped_bar_brand,
				mapped_bar_custno,
				mapped_bar_product,
				
				org_shiptocust,
				org_soldtocust,
				org_material,
				
				alloc_shiptocust,
				alloc_soldtocust,
				alloc_material,
				alloc_bar_product,
				
				bar_currtype,
								
				org_dataprocessing_ruleid,
				mapped_dataprocessing_ruleid,
				dataprocessing_outcome_id,
				dataprocessing_phase,
				
				allocated_amt,
				
				audit_loadts			
		)
		select 	tran.source_system,
				tran.org_tranagg_id,
				
				tran.posting_week_enddate,
				tran.fiscal_month_id,
				
				tran.bar_entity,
				tran.bar_acct,
				
				tran.org_bar_brand,
				tran.org_bar_custno,
				tran.org_bar_product,
				tran.mapped_bar_brand,
				tran.mapped_bar_custno,
				tran.mapped_bar_product,
				
				tran.shiptocust as org_shiptocust,
				tran.org_soldtocust,
				tran.material as org_material,
				
				tran.shiptocust as alloc_shiptocust,
				tran.alloc_soldtocust,
				tran.material as alloc_material,
				tran.mapped_bar_product as alloc_bar_product,
				
				bar_currtype,
								
				tran.org_dataprocessing_ruleid,
				tran.mapped_dataprocessing_ruleid,
				1 as dataprocessing_outcome_id,
				'phase 1' as dataprocessing_phase,
				
				tran.allocated_bar_amt as allocated_amt,
				getdate() as audit_loadts
		from 	_part01_allocated_trans_rate tran
	;

	/* load leakage (original transactions) */
	insert into stage.sgm_allocated_data_rule_13 (
	
				source_system,
				org_tranagg_id,
				
				posting_week_enddate,
				fiscal_month_id,
				
				bar_entity,
				bar_acct,
				
				org_bar_brand,
				org_bar_custno,
				org_bar_product,
				mapped_bar_brand,
				mapped_bar_custno,
				mapped_bar_product,
				
				org_shiptocust,
				org_soldtocust,
				org_material,
				
				alloc_shiptocust,
				alloc_soldtocust,
				alloc_material,
				alloc_bar_product,
				
				bar_currtype,
								
				org_dataprocessing_ruleid,
				mapped_dataprocessing_ruleid,
				dataprocessing_outcome_id,
				dataprocessing_phase,
				
				allocated_amt,
				
				audit_loadts
		)
		select 	tran.source_system,
				tran.org_tranagg_id,
				
				tran.posting_week_enddate,
				tran.fiscal_month_id,
				
				tran.bar_entity,
				tran.bar_acct,
				
				tran.org_bar_brand,
				tran.org_bar_custno,
				tran.org_bar_product,
				tran.mapped_bar_brand,
				tran.mapped_bar_custno,
				tran.mapped_bar_product,
				
				tran.shiptocust as org_shiptocust,
				tran.org_soldtocust,
				tran.material as org_material,
				
				tran.shiptocust as alloc_shiptocust,
				tran.org_soldtocust as alloc_soldtocust,
				tran.material as alloc_material,
				tran.mapped_bar_product as alloc_bar_product,
				
				bar_currtype,
								
				tran.org_dataprocessing_ruleid,
				tran.mapped_dataprocessing_ruleid,
				2 as dataprocessing_outcome_id,
				'phase 100' as dataprocessing_phase,
				
				tran.unallocated_bar_amt as allocated_amt,
				
				getdate() as audit_loadts
		from 	_part02_leakage leak
				inner join _trans_unalloc as tran
					on 	tran.org_tranagg_id = leak.org_tranagg_id
	;
	/* 	Validation: compare total amount between original unallocated transactions
	 *    and the allocated table;
	 */
--	select 	'orig' as recordset, round(sum(unallocated_bar_amt),2) as amt,
--			count(distinct org_tranagg_id) orig_trans_count
--	from _trans_unalloc
--	union all
--	select 	'result' as recordset, round(sum(allocated_amt),2) as amt,
--			count(distinct org_tranagg_id) orig_trans_count
--	from stage.sgm_allocated_data_rule_13
--	union all
--	select 	'result-allocated' as recordset, round(sum(allocated_amt),2) as amt,
--			count(distinct org_tranagg_id) orig_trans_count
--	from stage.sgm_allocated_data_rule_13
--	where 	dataprocessing_outcome_id = 1
--	union all
--	select 	'result-unallocated' as recordset, round(sum(allocated_amt),2) as amt,
--			count(distinct org_tranagg_id) orig_trans_count
--	from stage.sgm_allocated_data_rule_13
--	where 	dataprocessing_outcome_id = 2
--	order by 1
--	;
--
--Select sum(bar_amt), audit_rec_src
--from stage.bods_core_transaction_agg bcta 
--where mapped_dataprocessing_ruleid =13 
--and fiscal_month_id = 202001
--group by audit_rec_src;
----
----
----
--Select sum(allocated_amt), source_system 
--from stage.sgm_allocated_data_rule_13 bcta 
--where mapped_dataprocessing_ruleid =13 
--and fiscal_month_id = 202001
--group by source_system;
	
exception
when others then raise info 'exception occur while ingesting data in stage.sgm_allocated_data_rule_13';
end;
$$
;