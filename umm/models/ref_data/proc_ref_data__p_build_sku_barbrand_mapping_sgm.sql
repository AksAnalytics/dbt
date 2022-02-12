
CREATE OR REPLACE PROCEDURE ref_data.p_build_sku_barbrand_mapping_sgm(fmthid integer)
 LANGUAGE plpgsql
AS $$
BEGIN 
	/*
	 * 		drop procedure ref_data.p_build_sku_barbrand_mapping_sgm(fmthid integer)
	 * 		delete from ref_data.sku_barbrand_mapping_sgm;
	 * 		call ref_data.p_build_sku_barbrand_mapping_sgm(202101)
	 * 		select count(*) from ref_data.sku_barbrand_mapping_sgm;
	 * 		select distinct ss_fiscal_month_id from ref_data.sku_barbrand_mapping_sgm;
	 * 		grant execute on procedure ref_data.p_build_sku_barbrand_mapping_sgm(fmthid integer) to group "g-ada-rsabible-sb-ro";
	 */
	
	/*
	 *		This procedure creates a custom mapping: sku -> bar_brand 
	 *		
	 *		The final table is a snapshot by fiscal month where each material
	 *		is mapped to a single bar_brand based on the highest invoice sales 
	 *		transactions (A40110) from the beginning of time up to the current
	 *		fiscal period.
	 *
	 * 		Final Table(s): 
	 *			ref_data.sku_barbrand_mapping_sgm
	 *
	 */
	
	/*  create mapping table for material -> brand
	 */
	drop table if exists stage_sku_barbrand_mapping_sgm
	;
	create temporary table stage_sku_barbrand_mapping_sgm
	diststyle all
	as 
	with
		cte_base as (	
			SELECT 	fmthid as ss_fiscal_month_id,
					dp.material,
					dp.product_brand AS bar_brand,
					sum(f.amt_usd) as total_amt_usd
			FROM 	dw.dim_product AS dp
					INNER JOIN dw.fact_pnl_commercial_stacked AS f
						ON 	f.product_key = dp.product_key
			WHERE 	lower(dp.material) not in ('unknown') and 
					f.bar_acct = 'A40110' and 
					f.fiscal_month_id <= fmthid
			group by dp.material,
					dp.product_brand
		),
		cte_rank as (
			select 	base.ss_fiscal_month_id,
					base.material,
					base.bar_brand,
					base.total_amt_usd,
					rank() over(
						partition by base.material
						order by base.total_amt_usd desc, base.bar_brand
					) as rnk
			from 	cte_base as base
		)
		select 	rnk.ss_fiscal_month_id,
				rnk.material,
				rnk.bar_brand
		from 	cte_rank as rnk
		where 	rnk.rnk = 1
	;
	delete 
	from 	ref_data.sku_barbrand_mapping_sgm
	where 	ss_fiscal_month_id = fmthid
	;
	insert into ref_data.sku_barbrand_mapping_sgm (
				ss_fiscal_month_id,
				material,
				bar_brand,
				audit_loadts
		)
		select 	stg.ss_fiscal_month_id,
				stg.material,
				stg.bar_brand,
				getdate() as audit_loadts
		from 	stage_sku_barbrand_mapping_sgm stg
	;
	
	exception
		when others then raise info 'exception occur while ingesting data in ref_data.p_build_sku_barbrand_mapping_sgm';
end
$$
;