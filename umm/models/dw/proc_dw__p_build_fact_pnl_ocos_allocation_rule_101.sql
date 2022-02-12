
CREATE OR REPLACE PROCEDURE dw.p_build_fact_pnl_ocos_allocation_rule_101(fmthid integer)
 LANGUAGE plpgsql
AS $$
BEGIN 
/*
 * 		call dw.p_build_fact_pnl_ocos_allocation_rule_101(202101)
 * 		grant execute on procedure dw.p_build_fact_pnl_ocos_allocation_rule_101(fmthid integer) to group "g-ada-rsabible-sb-ro";
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
	drop table if exists tmp_fact_pnl_ocos_allocation_rule_101
	;
	create temporary table tmp_fact_pnl_ocos_allocation_rule_101
	diststyle even
	sortkey (posting_week_enddate)
	AS
		SELECT 	tr.posting_week_enddate,
				tr.fiscal_month_id, 
				tr.bar_acct,
				tr.bar_currtype,
				tr.bar_entity,
				tr.dataprocessing_ruleid,
				tr.dataprocessing_outcome_id,
				tr.dataprocessing_phase,
				tr.material,
				tr.bar_product,
				tr.bar_brand,
				tr.soldtocust,
				tr.shiptocust,
				tr.bar_custno,
				tr.cost_pool,
				tr.super_sbu,
				tr.product_id,
				tr.customer_id,
				tr.scenario_id,
				tr.amt,
				tr.amt_usd,
				dp.product_key,
				dc.customer_key,
				dbu.business_unit_key,
				ddo.dataprocessing_outcome_key,
				dss.source_system_id,
				
				0 as reported_inventory_adjustment,
				tr.amt as reported_warranty_cost,
				0 as reported_duty_tariffs,
				0 as reported_freight,
				0 as reported_ppv,
				0 as reported_labor_overhead,
				
				0 as tran_volume,
				0 as sales_volume,
				null as uom
				
		FROM 	(
					SELECT	 f.posting_week_enddate 
							,f.fiscal_month_id 
							,f.bar_acct
							,f.bar_currtype 
							,f.bar_entity				
							
							,f.dataprocessing_ruleid
							,f.dataprocessing_outcome_id
							,f.dataprocessing_phase
							
							,f.material
							,f.bar_product
							,f.bar_brand
							
							,f.soldtocust
							,f.shiptocust
							,f.bar_custno
							,f.cost_pool
							,f.super_sbu
							
							,f.material || '|' || f.bar_product || '|' || f.bar_brand as product_id
							,f.soldtocust || '|' || f.shiptocust || '|' || f.bar_custno as customer_id
							
							,cast(1 as integer) as scenario_id  -- Hard coded to Actuals
							,f.source_system
							
							,f.allocated_amt as amt
							,f.allocated_amt_usd as amt_usd
							
					from 	stage.agm_allocated_data_rule_101 f 
							inner join vtbl_date_range dd 
								on 	dd.fiscal_month_id = f.fiscal_month_id
				) as tr
				LEFT OUTER JOIN dw.dim_product dp on lower(tr.product_id) = lower(dp.product_id) 
				LEFT OUTER JOIN dw.dim_business_unit dbu on lower(tr.bar_entity) = lower(dbu.bar_entity)
				LEFT OUTER JOIN dw.dim_dataprocessing_outcome ddo 
					on 	ddo.dataprocessing_outcome_id = tr.dataprocessing_outcome_id and 
						lower(ddo.dataprocessing_phase) = lower(tr.dataprocessing_phase) 
				LEFT OUTER JOIN dw.dim_customer dc on lower(dc.customer_id) = lower(tr.customer_id)
				LEFT OUTER JOIN dw.dim_source_system dss on lower(tr.source_system) = lower(dss.source_system)
	;

--select 	bar_acct,
--		count(*) row_count,
--		sum(case when customer_key is null then 1 else 0 end) as missing_cust_key,
--		sum(case when product_key is null then 1 else 0 end) as missing_prod_key,
--		sum(case when business_unit_key is null then 1 else 0 end) as missing_bu_key,
--		sum(case when dataprocessing_outcome_key is null then 1 else 0 end) as missing_outcome_key,
--		sum(case when source_system_id is null then 1 else 0 end) as missing_source_key
--from 	tmp_fact_pnl_ocos_allocation_rule_13
--group by bar_acct
--;

	/* remove any existing transactions for the current batch being processed */
	delete 
	from 	dw.fact_pnl_ocos 
	where 	dataprocessing_ruleid  = 101 and 
			posting_week_enddate between 
				(select range_start_date from vtbl_date_range) and 
				(select range_end_date from vtbl_date_range)
	;
	/* insert statement */
	insert into dw.fact_pnl_ocos (
				org_tranagg_id,
				dataprocessing_ruleid,
				dataprocessing_outcome_key,
				
				bar_acct,
				bar_currtype,
				
				posting_week_enddate,
				fiscal_month_id,
				
				scenario_id,
				source_system_id,
				business_unit_key,
				customer_key,
				product_key,
				
				soldtocust, 
			    shiptocust,
			    bar_custno,
			    cost_pool,
			    super_sbu,
				
				material,
				bar_product,
				bar_brand,
				
				amt,
				amt_usd,
				
				reported_inventory_adjustment,
				reported_warranty_cost,
				reported_duty_tariffs,
				reported_freight,
				reported_ppv,
				reported_labor_overhead,
				
				tran_volume,
				sales_volume,
				uom,
				audit_loadts
		)
		select	-1 as org_tranagg_id,
				tmp.dataprocessing_ruleid,
				tmp.dataprocessing_outcome_key,
				
				tmp.bar_acct,
				tmp.bar_currtype,
				
				tmp.posting_week_enddate,
				tmp.fiscal_month_id,
				
				tmp.scenario_id,
				tmp.source_system_id,
				tmp.business_unit_key,
				tmp.customer_key,
				tmp.product_key,
				
				tmp.soldtocust, 
			     tmp.shiptocust,
			     tmp.bar_custno,
			     tmp.cost_pool,
			     tmp.super_sbu,
				
				tmp.material,
				tmp.bar_product,
				tmp.bar_brand,
				
				tmp.amt,
				tmp.amt_usd,
				
				tmp.reported_inventory_adjustment,
				tmp.reported_warranty_cost,
				tmp.reported_duty_tariffs,
				tmp.reported_freight,
				tmp.reported_ppv,
				tmp.reported_labor_overhead,
				
				tmp.tran_volume,
				tmp.sales_volume,
				tmp.uom,
				getdate() as audit_loadts
		from 	tmp_fact_pnl_ocos_allocation_rule_101 tmp
	;
exception when others then raise info 'exception occur while ingesting data in fact_pnl_commercial for rule_101';
end
$$
;