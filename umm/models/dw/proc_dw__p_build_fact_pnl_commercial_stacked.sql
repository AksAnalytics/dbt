
CREATE OR REPLACE PROCEDURE dw.p_build_fact_pnl_commercial_stacked(fmthid integer)
 LANGUAGE plpgsql
AS $$
BEGIN 

	/* create temp table for selected period */
	drop table if exists vtbl_date_range
	;
	create temporary table vtbl_date_range as 
		select 	cast(min(dt.dy_dte) as date) as range_start_date ,
				cast(max(dt.dy_dte) as date) as range_end_date
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
		where 	lower(rt.to_currtype) = 'usd'
	;
	/* remove any existing transactions for the current batch being processed */
	delete 
	from 	dw.fact_pnl_commercial_stacked 
	where 	posting_week_enddate between 
			(select range_start_date from vtbl_date_range) and 
			(select range_end_date from vtbl_date_range)
	;
	
	
	INSERT INTO dw.fact_pnl_commercial_stacked (
				org_tranagg_id,
				posting_week_enddate,
				fiscal_month_id,
				bar_acct,
				bar_currtype,
				customer_key,
				product_key,
				org_dataprocessing_ruleid,
				mapped_dataprocessing_ruleid,
				dataprocessing_outcome_key,
				business_unit_key,
				scenario_id,
				source_system_id,
				org_bar_custno,
				org_bar_product,
				org_bar_brand,
				mapped_bar_custno,
				mapped_bar_product,
				mapped_bar_brand,
				org_soldtocust, 
			    org_shiptocust,
			    org_material,
			    alloc_soldtocust, 
			    alloc_shiptocust, 
			    alloc_material,
			    alloc_bar_product,
			    allocated_flag,
				amt,
				amt_usd,
				tran_volume,
				sales_volume,
				uom,
				dim_transactional_attributes_id
		)
		Select	fpc.org_tranagg_id,
				fpc.posting_week_enddate,
				fpc.fiscal_month_id,
				fpc.bar_acct,
				fpc.bar_currtype,
				fpc.customer_key,
			    fpc.product_key,
				fpc.org_dataprocessing_ruleid,
				fpc.mapped_dataprocessing_ruleid,
				fpc.dataprocessing_outcome_key,
				fpc.business_unit_key,
				fpc.scenario_id,
				fpc.source_system_id,
				fpc.org_bar_custno,
				fpc.org_bar_product,
				fpc.org_bar_brand,
				fpc.mapped_bar_custno,
				fpc.mapped_bar_product,
				fpc.mapped_bar_brand,
				fpc.org_soldtocust, 
			    fpc.org_shiptocust,
			    fpc.org_material,
			    fpc.alloc_soldtocust, 
			    fpc.alloc_shiptocust, 
			    fpc.alloc_material,
			    fpc.alloc_bar_product,
			    case when fpc.mapped_dataprocessing_ruleid != 1 
			         then true
			         when fpc.mapped_dataprocessing_ruleid =1 
			     	    and (dc.level11_bar = 'Customer_None' or dp.level09_bar = 'Product_None' )
			    	    then true
			    	else false
			    	end as allocated_flag,
				fpc.amt,
				case 
					when rt.fxrate is not null then rt.fxrate * fpc.amt 
					else fpc.amt 
				end as amt_usd,
				fpc.tran_volume,
				fpc.sales_volume,
				fpc.uom,
				fpc.dim_transactional_attributes_id
		from 	dw.fact_pnl_commercial fpc 
				left outer join vtbl_exchange_rate rt
					on 	rt.fiscal_month_id = fpc.fiscal_month_id and 
						lower(rt.from_currtype) = lower(fpc.bar_currtype)
				left join dw.dim_customer dc on fpc.customer_key = dc.customer_key 
				left join dw.dim_product dp on fpc.product_key = dp.product_key 
		where 	fpc.posting_week_enddate between 
				(select range_start_date from vtbl_date_range) and 
				(select range_end_date from vtbl_date_range)
	;
EXCEPTION
	when others then raise info 'exception occur while building fact_pnl_commercial_stacked';
END;
$$
;