
CREATE OR REPLACE PROCEDURE dw.p_build_fact_pnl_ocos_stacked(fmthid integer)
 LANGUAGE plpgsql
AS $$
BEGIN 
/*
 * 		call dw.p_build_fact_pnl_ocos_stacked(202101)
 * 		select count(*) from dw.fact_pnl_ocos_stacked where dataprocessing_ruleid = 100;
 * 		grant execute on procedure dw.p_build_fact_pnl_ocos_stacked(fmthid integer) to group "g-ada-rsabible-sb-ro";
 */
	/* create temp table for selected period */
	drop table if exists vtbl_date_range
	;
	create temporary table vtbl_date_range as 
		select 	cast(min(dt.dy_dte) as date) as range_start_date ,
				cast(max(dt.dy_dte) as date) as range_end_date,
				max(dt.fmth_id) as fiscal_month_id
		from 	ref_data.calendar dt
		where 	dt.fmth_id = fmthid
	;
	/* remove any existing transactions for the current batch being processed */
	delete 
	from 	dw.fact_pnl_ocos_stacked 
	where 	posting_week_enddate between 
			(select range_start_date from vtbl_date_range) and 
			(select range_end_date from vtbl_date_range)
	;
	
	
	INSERT INTO dw.fact_pnl_ocos_stacked (
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
				
				material,
				bar_product,
				bar_brand,
				
				cost_pool,
				super_sbu,
				
				amt,
				amt_usd,
				
				tran_volume,
				sales_volume,
				uom
		)
		select	fpo.org_tranagg_id,
				fpo.dataprocessing_ruleid,
				fpo.dataprocessing_outcome_key,
				
				fpo.bar_acct,
				fpo.bar_currtype,
				
				fpo.posting_week_enddate,
				fpo.fiscal_month_id,
				
				fpo.scenario_id,
				fpo.source_system_id,
				fpo.business_unit_key,
				fpo.customer_key,
				fpo.product_key,
				
				fpo.soldtocust, 
			    fpo.shiptocust,
			    fpo.bar_custno,
				
				fpo.material,
				fpo.bar_product,
				fpo.bar_brand,
				
				fpo.cost_pool,
				fpo.super_sbu,
				
				fpo.amt,
				fpo.amt_usd,
				
				fpo.tran_volume,
				fpo.sales_volume,
				fpo.uom
		from 	dw.fact_pnl_ocos fpo 
		where 	fpo.posting_week_enddate between 
				(select range_start_date from vtbl_date_range) and 
				(select range_end_date from vtbl_date_range)
	;
EXCEPTION
	when others then raise info 'exception occur while building fact_pnl_commercial_stacked';
END;
$$
;