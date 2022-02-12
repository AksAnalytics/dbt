
CREATE OR REPLACE PROCEDURE stage.p_build_stage_rate_base(p_fmth_id integer)
 LANGUAGE plpgsql
AS $$
BEGIN
/*
 * 	TODO:
 * 		Handle Currency Conversion for non USD transactions
 * 			Use stage.currency_exchange_rate
 * 		
 */	
	
	/* create temp table for selected period */
	drop table if exists vtbl_date_range
	;
	create temporary table vtbl_date_range as 
		select 	cast(min(dt.dy_dte) as date) as range_start_date ,
				cast(max(dt.dy_dte) as date) as range_end_date
		from 	ref_data.calendar dt
		where 	dt.fmth_id = p_fmth_id
	;
	/* delete rate_base table for selected period */
	delete 	
	from 	stage.rate_base
	where 	0=0
		and range_start_date <= (select range_start_date from vtbl_date_range)
		and range_end_date = (select range_end_date from vtbl_date_range)
	;
	/* insert into rate_base table for selected period */
	INSERT INTO stage.rate_base (
				range_start_date, 
				range_end_date,
				Source_system,
				bar_entity,
				soldtocust, 
				bar_custno, 
				material,
				bar_product, 
				bar_currtype,
				total_bar_amt
		)
		select 	
				dt_rng.range_start_date,
				dt_rng.range_end_date,
				tran.audit_rec_src,
				tran.bar_entity,
				tran.soldtocust as soldtocust,
				tran.mapped_bar_custno as bar_custno,
				tran.material as material,
				tran.mapped_bar_product as bar_product,
				tran.bar_currtype,
				SUM(tran.bar_amt) as total_bar_amt
			
		from 	stage.bods_core_transaction_agg as tran
				inner join ref_data.data_processing_rule as dpr 
					on  dpr.data_processing_ruleid = tran.mapped_dataprocessing_ruleid 
				inner join vtbl_date_range as dt_rng
					on  dt_rng.range_start_date <= tran.posting_week_enddate and 
						dt_rng.range_end_date >= tran.posting_week_enddate
				
		where 	dpr.dataprocessing_group = 'perfect-data' and 
				dpr.soldtoflag = '1' and 
				dpr.skuflag = '1' and
				---remove mgsv sku's from allocation
				lower(tran.material) not like 'mgsv%' and 
				/* exclude transactions with zero sales amt */
				tran.bar_amt != 0 and
				/* sales invoice */
				tran.bar_acct = 'A40110'
		group by  tran.audit_rec_src,
				dt_rng.range_start_date,
				dt_rng.range_end_date,
				
				tran.bar_entity,
				tran.soldtocust,
				tran.mapped_bar_custno,
				tran.material,
				tran.mapped_bar_product,
				tran.bar_currtype
				
		/* exclude combinations with net-zero sales amt */
		having 	SUM(tran.bar_amt) != 0
	;

exception
when others then raise info 'exception occur while ingesting data in stage.rate_base';
end;
$$
;