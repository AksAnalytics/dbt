
CREATE OR REPLACE PROCEDURE stage.p_allocate_data_rule_agm_105(fmthid integer)
 LANGUAGE plpgsql
AS $$
BEGIN   
	
/*
 * 		truncate table stage.agm_allocated_data_rule_105;
 * 		call stage.p_allocate_data_rule_agm_105(202101)
 * 		select count(*) from stage.agm_allocated_data_rule_105;
 * 		grant execute on procedure stage.p_allocate_data_rule_agm_105(fmthid integer) to group "g-ada-rsabible-sb-ro";
 */
/*
 *		Description: logic for AGM Rule ID #105 (Reported Labor/OH)
 *
 * 		Final Table(s): 
 *			stage.agm_allocated_data_rule_105
 *
 * 		Rule Logic:	
 *			Step 1: Extract cost for allocation at SBU level
 *			Step 2: Build allocation rate table (% of COGS) for P&L cost assignment
 *			Step 3: Allocate cost to SKU transactions
 *			Step 4: Allocate BOD gap to BA&R hyperion cost (at super SBU level)
 *
 *		TODO:
 *			test w/ real data
 *			
 */
	
	/*
	 * 	201901: Target
	 * 			-10,105,649.6
	 * 			Actual
	 * 			
	 * 
	 */
	
	
	/* create temp table for selected period */
	drop table if exists vtbl_date_range
	;
	create temporary table vtbl_date_range as 
		select 	cast(min(dt.dy_dte) as date) as range_start_date ,
				cast(max(dt.dy_dte) as date) as range_end_date,
				max(dt.fmth_id) AS fiscal_month_id
		from 	ref_data.calendar dt
		where 	dt.fmth_id = fmthid
	;
	/* create temp table for exchange_rate */
	drop table if exists vtbl_exchange_rate
	;
	create temporary table vtbl_exchange_rate as 
		select 	rt.fiscal_month_id, 
				rt.from_currtype,
				rt.fxrate
		from 	{{ source('ref_data', 'hfmfxrates') }} rt
				inner join vtbl_date_range dt
					on 	dt.fiscal_month_id = rt.fiscal_month_id 
		where 	lower(rt.to_currtype) = 'usd'
	;
	
	/* Step 1: Hyperion Amounts for month */
	drop table if exists _hyp_amt
	;
	create temporary table _hyp_amt as 
		select 	bar.fiscal_month_id,
				sum(bar.amt_reported * agm_acct.multiplication_factor) as amt_usd
		from 	ref_data.agm_bnr_financials_extract bar
				inner join ref_data.pnl_acct_agm as agm_acct 
					on 	agm_acct.bar_acct = bar.account 
				inner join vtbl_date_range as dt
					on 	bar.fiscal_month_id = dt.fiscal_month_id
				inner join (
					/* this table is no longer distinct on entity_name */
					select 	distinct name, level4
					from 	ref_data.entity
				) as rbh
					on 	bar.entity = rbh.name
		where 	agm_acct.acct_category = 'Reported Labor / OH' and 
				bar.scenario = 'Actual_Ledger' and
				rbh.level4 = 'GTS_NA'
		group by bar.fiscal_month_id
	;

	/* Step 2: sku/cust/entity with postive net sales (A40110) in current month */
	drop table if exists sku_positive_sales
	;
	create temporary table sku_positive_sales as 
		select 	dp.material
		from 	dw.fact_pnl_commercial_stacked as f
				inner join dw.dim_product dp on dp.product_key = f.product_key 
				inner join vtbl_date_range as dt_rng
					on  dt_rng.fiscal_month_id = f.fiscal_month_id 
		where 	0=0
			and f.bar_acct = 'A40110' 
		group by dp.material
		having 	sum(f.amt_usd) > 0
	;
	/* Step 2: create rate table based on standard cost */
	drop table if exists rate_base_cogs_pct_of_total
	;
	create temporary table rate_base_cogs_pct_of_total as 
		WITH
			cte_base AS (
				select 	rb_cogs.fiscal_month_id,
						rb_cogs.material,
						rb_cogs.audit_rec_src,
						rb_cogs.bar_entity,
						rb_cogs.bar_currtype,
						rb_cogs.soldtocust,
						rb_cogs.shiptocust,
						rb_cogs.bar_custno,
						rb_cogs.bar_product,
						rb_cogs.bar_brand,
						rb_cogs.cost_pool,
						sum(rb_cogs.total_bar_amt_usd) as total_bar_amt_usd
				from 	stage.rate_base_cogs as rb_cogs
						inner join vtbl_date_range as dt_rng
							on  dt_rng.fiscal_month_id = rb_cogs.fiscal_month_id 
						inner join sku_positive_sales sku_list
							on 	lower(sku_list.material) = lower(rb_cogs.material)
				group by rb_cogs.fiscal_month_id,
						rb_cogs.material,
						rb_cogs.audit_rec_src,
						rb_cogs.bar_entity,
						rb_cogs.bar_currtype,
						rb_cogs.soldtocust,
						rb_cogs.shiptocust,
						rb_cogs.bar_custno,
						rb_cogs.bar_product,
						rb_cogs.bar_brand,
						rb_cogs.cost_pool
				having 	sum(rb_cogs.total_bar_amt_usd) < 0
			),
			cte_rate_base_cogs as (
				select 	rb.fiscal_month_id,
						rb.material,
						rb.audit_rec_src,
						rb.bar_entity,
						rb.bar_currtype,
						rb.soldtocust,
						rb.shiptocust,
						rb.bar_custno,
						rb.bar_product,
						rb.bar_brand,
						rb.cost_pool,
						rb.total_bar_amt_usd,
						sum(rb.total_bar_amt_usd) over( partition by rb.fiscal_month_id ) as total_bar_amt_usd_partition
				from 	cte_base as rb
			)
		select 	cte_rb.fiscal_month_id,
				cte_rb.material,
				cte_rb.audit_rec_src,
				cte_rb.bar_entity,
				cte_rb.bar_currtype,
				cte_rb.soldtocust,
				cte_rb.shiptocust,
				cte_rb.bar_custno,
				cte_rb.bar_product,
				cte_rb.bar_brand,
				cte_rb.cost_pool,
				cte_rb.total_bar_amt_usd,
				cte_rb.total_bar_amt_usd_partition,
				CAST(cte_rb.total_bar_amt_usd as decimal(20,8))
					/ CAST(cte_rb.total_bar_amt_usd_partition as decimal(20,8)) as pct_of_total
		from 	cte_rate_base_cogs cte_rb
		where 	total_bar_amt_usd_partition != 0
	;

/* DEBUG: clusters with cumulative pct of total != 100% */
--select 	rt.fiscal_month_id, sum(rt.pct_of_total)
--from 	rate_base_cogs_pct_of_total rt
--group by rt.fiscal_month_id
--having round(sum(rt.pct_of_total),4) != 1
--order by 2 asc
--;
	/* Step 2: allocate to full transaction level via COGS Rate */
	drop table if exists _hyp_allocated
	;
	create temporary table _hyp_allocated as 
		select 	rt.fiscal_month_id,
				rt.audit_rec_src,
				rt.bar_entity,
				rt.bar_currtype,
				rt.soldtocust,
				rt.shiptocust,
				rt.bar_custno,
				rt.material,
				rt.bar_product,
				rt.bar_brand,
				rt.cost_pool,
				tran.amt_usd,
				rt.pct_of_total,
				rt.pct_of_total * tran.amt_usd as allocated_amt_usd
		from 	_hyp_amt as tran
				inner join rate_base_cogs_pct_of_total as rt
					on 	rt.fiscal_month_id = tran.fiscal_month_id
	;
	

/* DEBUG: compare input / output*/
--select 	1 as ord, 'Input-HYP' as category, 
--		sum(round(amt_usd,4)) as amt_usd, count(*)
--from 	_hyp_amt
--union all
--select 	2 as ord, 'Output-Cost2SSBU' as category, 
--		sum(round(allocated_amt_usd,4)) as amt_usd, count(*)
--from 	_hyp_allocated
--order by 1
--;

	/* remove any existing transactions for the current batch being processed */
	delete 
	from 	stage.agm_allocated_data_rule_105
	where 	fiscal_month_id = (select fiscal_month_id from vtbl_date_range)
	;
	/* load to final transaction table (AGM: Labor/OH Adj) */
	INSERT INTO stage.agm_allocated_data_rule_105 (
				source_system,
				fiscal_month_id,
				posting_week_enddate,
			
				bar_entity,
				bar_acct,
				
				material,
				bar_product,
				bar_brand,
				
				soldtocust,
				shiptocust,
				bar_custno,
				
				dataprocessing_ruleid,
				dataprocessing_outcome_id,
				dataprocessing_phase,
				
				cost_pool,
				super_sbu,
				
				bar_currtype,
				allocated_amt,
				allocated_amt_usd,
				
				audit_loadts
		)
		select 	stg.audit_rec_src as source_system,
				stg.fiscal_month_id,
				dt.range_end_date as posting_week_enddate,
			
				stg.bar_entity,
				'AGM_ADJ_LABOH' as bar_acct,
				
				stg.material,
				stg.bar_product,
				stg.bar_brand as bar_brand,
				
				stg.soldtocust,
				'unknown' as shiptocust,
				stg.bar_custno,
				
				105 as dataprocessing_ruleid,
				1 as dataprocessing_outcome_id,
				'phase 25' as dataprocessing_phase,
				
				/* these are N/A because they are no longer factors in the allocation logic */
				'n/a' as cost_pool,
				'n/a' as super_sbu,
				
				stg.bar_currtype,
				case 
					when fx.from_currtype is null then stg.allocated_amt_usd
					else CAST(stg.allocated_amt_usd as decimal(20,8))
						/ CAST(fx.fxrate as decimal(20,8))
				end as allocated_amt,
				stg.allocated_amt_usd,
				
				getdate() audit_loadts
		from 	_hyp_allocated as stg
				inner join vtbl_date_range as dt
					on 	dt.fiscal_month_id = stg.fiscal_month_id				
				left outer join vtbl_exchange_rate as fx
					on 	fx.fiscal_month_id = stg.fiscal_month_id and 
						lower(fx.from_currtype) = lower(stg.bar_currtype)
	;
/* DEBUG: compare BODS Input vs BAR Input */
--select 	1 as ord, 'Input-BODS' as category, 
--		sum(round(allocated_amt_usd,4)) as amt,
--		count(*)
--from 	_hyp_allocated
--union all
--select 	2 as ord, 'Output-StageAGM' as category, 
--		sum(round(allocated_amt_usd,4)) as amt,
--		count(*)
--from 	stage.agm_allocated_data_rule_105
--where 	fiscal_month_id = 202101
--order by ord
--;

exception
when others then raise info 'exception occur while ingesting data in stage.p_allocate_data_rule_agm_105';
end;
$$
;