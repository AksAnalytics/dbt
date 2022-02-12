
CREATE OR REPLACE PROCEDURE stage.p_build_stage_rate_base_cogs(fmthid integer)
 LANGUAGE plpgsql
AS $$
BEGIN
	/*
	 * 		call stage.p_build_stage_rate_base_cogs (202101)
	 * 		select count(*) from stage.rate_base_cogs;
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
/* mapping gpp portfolio to super-SBU */
	drop table if exists map_gpp_portfolio_to_supersbu
	;
	create temporary table map_gpp_portfolio_to_supersbu as 
	with
		cte_base as (
			select 	name as bar_product, 
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
					description as bar_product_desc,
					case when bar_product is null then 'unknown' else bar_product end as portfolio,
					cast(generation as int) as generation,
					case when level4 is null then 'unknown' else level4 end as level04_bar,
					case when level7 is null then 'unknown' else level7 end as level07_bar
			from 	{{ source('bods', 'drm_product') }}
			where 	loaddts = ( select max(loaddts) from {{ source('bods', 'drm_product') }} dpc )
				and membertype != 'Parent'
		)
		select 	portfolio as gpp_portfolio,
				case when generation <= 4  then case when bar_product = 'Product_None' then bar_product else parent end else level04_bar end as super_sbu,
				case when generation <= 7  then case when bar_product = 'Product_None' then bar_product else parent end else level07_bar end as division
		from 	cte_base 
	;
	delete from stage.rate_base_cogs where fiscal_month_id = fmthid; 
	/* rate table based on standard cost */
	insert into stage.rate_base_cogs (
			 audit_rec_src
			 
			,fiscal_month_id
		
			,bar_entity
			,bar_currtype
			
			,soldtocust
			,shiptocust
			,bar_custno
			
			,material
			,bar_product
			,bar_brand
			
			,super_sbu
			,cost_pool
			
			,total_bar_amt
			,total_bar_amt_usd
	
	)
	
		select 	dss.source_system as audit_rec_src,
				tran.fiscal_month_id,
		
				dbu.bar_entity,
				tran.bar_currtype,
				
				dc.soldto_number as soldtocust,
				dc.shipto_number as shiptocust,
				dc.base_customer as bar_custno,
				
				dp.material,
				dp.bar_product,
				dp.product_brand as bar_brand,
				
				dp.level04_bar as super_sbu,
				case when lower(dp.level04_bar) = 'ptg' then 'PTG' else 'Non-PTG' end as cost_pools,
				
				SUM(tran.amt) as  total_bar_amt,
				SUM(tran.amt_usd) as total_bar_amt_usd
		from 	dw.fact_pnl_commercial_stacked as tran
				inner join dw.dim_customer dc on dc.customer_key = tran.customer_key 
				inner join dw.dim_product dp on dp.product_key = tran.product_key 
				inner join dw.dim_source_system dss on dss.source_system_id = tran.source_system_id 
				inner join dw.dim_business_unit dbu on dbu.business_unit_key = tran.business_unit_key 
				inner join ref_data.data_processing_rule as dpr 
					on  dpr.data_processing_ruleid = tran.mapped_dataprocessing_ruleid 
				inner join vtbl_date_range as dt_rng
					on  dt_rng.fiscal_month_id = tran.fiscal_month_id
					
		where 	dpr.dataprocessing_group = 'perfect-data' and 
				--removing mgsv sku's from allocation
				lower(dp.material) not like 'mgsv%' and 
				tran.amt_usd != 0 and
				tran.bar_acct in (
					'A60110','A60111','A60112','A60113',
					'A60114','A60115','A60116','A60210',
					'A61110','A61210','A60410','A60510',
					'A60610','A60612','A60613','A62612',
					'A62613','A62210','A60710','A60310'
				)
		group by dss.source_system,
				tran.fiscal_month_id,
		
				dbu.bar_entity,
				tran.bar_currtype,
				
				dc.soldto_number,
				dc.shipto_number,
				dc.base_customer,
				
				dp.material,
				dp.bar_product,
				dp.product_brand,
				
				dp.level04_bar,
				case when lower(dp.level04_bar) = 'ptg' then 'PTG' else 'Non-PTG' end
		/* exclude combinations with net-zero sales amt */
		having 	SUM(tran.amt_usd) != 0
	;
	
exception
when others then raise info 'exception occur while ingesting data in stage.rate_base_cogs';
end;
$$
;