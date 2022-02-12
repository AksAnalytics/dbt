
CREATE OR REPLACE PROCEDURE dw.p_build_fact_pnl_commercial_orig(fmthid integer)
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
	from 	dw.fact_pnl_commercial_orig 
	where 	posting_week_enddate between 
			(select range_start_date from vtbl_date_range) and 
			(select range_end_date from vtbl_date_range)
	;
	
	
	INSERT INTO dw.fact_pnl_commercial_orig (
				org_tranagg_id,
				posting_week_enddate,
				fiscal_month_id,
				bar_currtype,
				amt,
				amt_usd,
				tran_volume,
				sales_volume,
				uom
		)
		Select	fpc.org_tranagg_id,
				fpc.posting_week_enddate,
				fpc.fiscal_month_id,
				fpc.bar_currtype,
				fpc.bar_amt as amt,
				case 
					when rt.fxrate is not null then rt.fxrate * fpc.bar_amt 
					else fpc.bar_amt 
				end as amt_usd,
				fpc.tran_volume,
				fpc.sales_volume,
				fpc.uom
		from 	stage.bods_core_transaction_agg fpc 
				left outer join vtbl_exchange_rate rt
					on 	rt.fiscal_month_id = fpc.fiscal_month_id and 
						lower(rt.from_currtype) = lower(fpc.bar_currtype)
		where 	fpc.mapped_dataprocessing_ruleid != 1 and
				fpc.posting_week_enddate between 
				(select range_start_date from vtbl_date_range) and 
				(select range_end_date from vtbl_date_range)
	;
EXCEPTION
	when others then raise info 'exception occur while building fact_pnl_commercial_orig';
END;
$$
;