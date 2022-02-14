
CREATE OR REPLACE PROCEDURE dw.p_build_dim_product_restatement()
 LANGUAGE plpgsql
AS $$
Begin
	
	/*
	 * 		call dw.p_build_dim_product_restatement ();
	 * 		grant all on procedure dw.p_build_dim_product_restatement() to group "g-ada-rsabible-sb-ro";
	 * 		select count(*) from dw.dim_product_restatement;
	 * 
	 * 
	 */
	
	/************************************************************************************************
	 * STEPS : This process is full load for each run
	 * 1. Get All distinct allocated materials from fact sgm exluding exception rules
	 * {{ source('ref_data', 'sku_gpp_mapping') }}
	 * 3. Anything left over - will be mapped to sequence below
	 * 		ref_data.sku_barproduct_mapping_c11_bods
	 * 		ref_data.sku_barproduct_mapping_p10_bods
	 * 		ref_data.sku_barproduct_mapping_lawson_bods
	 * 4. validate it provides 100% coverage
	 * 5. map the material, bar_product to bods_drm_product table to get rest of hierarchy details
	 * 6. load the table
	 * 
	 */
	
	drop table if exists stage_material_to_map; 
	
	create temporary table stage_material_to_map
	diststyle all 
	as 
	select 	distinct 
			fpcs.alloc_material as material,
			fpcs.mapped_bar_product as bar_product,
			fpcs.source_system_id 
	from 	dw.fact_pnl_commercial_stacked fpcs 
	where	not(
				lower(fpcs.alloc_material) in (  
					'adj_royalty',
					'adj_fob_nocust', 
					'adj_fob_no_cust', --need to consolidate 
					'adj_royalty', 
					'unknown',  
					'adj_fob',
					'adj_rsa',
					'adj_service',
					'adj_rebuild',
					'adj_no_prod',
					'adj_no_cust',
					'mgsv-sku'
				) or 
				(
					lower(fpcs.mapped_bar_product) like '%_oth' or 
					lower(fpcs.mapped_bar_product) in ('product_none', 'p60999','oth_service')
				)
	   		)
	;
--select 	*
--from 	stage_material_to_map
--where 	material = '28-242'
--
--select 	distinct alloc_material, mapped_bar_product
--from 	dw.fact_pnl_commercial_stacked
--where 	alloc_material = '28-242'

--select	material, bar_product, count(*)
--from 	stage_material_to_map
--group by material, bar_product
--having count(*) > 1
--order by count(*) desc
--  
--select material,bar_product, sum(amt_usd) as amt_usd 
--from (
--	select distinct mat.material, 
--			coalesce(gpp_portfolio,sbmcb.bar_product,sbmpb.bar_product,sbmlb.bar_product,'unknown') as portfolio, 
--			mat.bar_product,
--			mat.source_system_id 
--	from stage_material_to_map mat
--	left join ref_data.sku_gpp_mapping sgm on lower(mat.material) = lower(sgm.material) and sgm.current_flag =1 
--	left join ref_data.sku_barproduct_mapping_c11_bods sbmcb  on lower(mat.material) = lower(sbmcb.material) and sbmcb.current_flag =1   
--	left join ref_data.sku_barproduct_mapping_p10_bods sbmpb  on lower(mat.material) = lower(sbmpb.material) and sbmpb.current_flag =1  
--	left join ref_data.sku_barproduct_mapping_lawson_bods sbmlb  on lower(mat.material) = lower(sbmlb.material) and sbmlb.current_flag =1  
--	where coalesce(gpp_portfolio,sbmcb.bar_product,sbmpb.bar_product,sbmlb.bar_product) is null
--) a 
--inner join dw.fact_pnl_commercial_stacked fpcs on loweR(a.material) = lower(fpcs.alloc_material)
--where bar_acct= 'A40110' 
--and fiscal_month_id between 201901 and 201912
--group by material,bar_product
--order by 3 desc;
	
	drop table if exists stage_material_to_restate; 
	
	create temporary table stage_material_to_restate
	diststyle all 
	as 
  	select distinct lower(mat.material) as material, 
			coalesce(gpp_portfolio,sbmcb.bar_product,sbmpb.bar_product,sbmlb.bar_product,'unknown') as portfolio
	from stage_material_to_map mat
	left join ref_data.sku_gpp_mapping sgm on lower(mat.material) = lower(sgm.material) and sgm.current_flag =1 
	left join ref_data.sku_barproduct_mapping_c11_bods sbmcb  on lower(mat.material) = lower(sbmcb.material) and sbmcb.current_flag =1   
	left join ref_data.sku_barproduct_mapping_p10_bods sbmpb  on lower(mat.material) = lower(sbmpb.material) and sbmpb.current_flag =1  
	left join ref_data.sku_barproduct_mapping_lawson_bods sbmlb  on lower(mat.material) = lower(sbmlb.material) and sbmlb.current_flag =1 ;

--select	count(*), count(distinct lower(material))
--from 	stage_material_to_restate
	/* Validate : If material have more than one portfolio */ 
	/*************************************************************
	select count(distinct portfolio ), material
	from stage_material_to_restate
	group by material 
	having count(distinct portfolio )>1;
	*******************************************************************/
	
	
	
	/* current version of bar_product hierarchy */
	drop table if exists tmp_bar_product_hierarchy;
	create temporary table tmp_bar_product_hierarchy 
	diststyle all 
	as 
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
					case when membertype is null then 'unknown' else membertype end as membertype,
					case when bar_product is null then 'unknown' else bar_product end as portfolio,
					case when bar_product_desc is null then 'unknown' else bar_product_desc end as portfolio_desc,
					cast(generation as int) as generation,
					case when level1 is null then 'unknown' else level1 end as level01_bar,
					case when level2 is null then 'unknown' else level2 end as level02_bar,
					case when level3 is null then 'unknown' else level3 end as level03_bar,
					case when level4 is null then 'unknown' else level4 end as level04_bar,
					case when level5 is null then 'unknown' else level5 end as level05_bar,
					case when level6 is null then 'unknown' else level6 end as level06_bar,
					case when level7 is null then 'unknown' else level7 end as level07_bar,
					case when level8 is null then 'unknown' else level8 end as level08_bar,
					case when level9 is null then 'unknown' else level9 end as level09_bar
			from 	{{ source('bods', 'drm_product') }}
			where 	loaddts = ( select max(loaddts) from {{ source('bods', 'drm_product') }} dpc )
				and membertype != 'Parent'
		)
		select 	bar_product,
				bar_product_desc,
				membertype,
				portfolio,
				portfolio_desc,
				generation,
				level01_bar,
				case when generation <= 2  then case when bar_product = 'Product_None' then bar_product else parent end else level02_bar end as level02_bar,
				case when generation <= 3  then case when bar_product = 'Product_None' then bar_product else parent end else level03_bar end as level03_bar,
				case when generation <= 4  then case when bar_product = 'Product_None' then bar_product else parent end else level04_bar end as level04_bar,
				case when generation <= 5  then case when bar_product = 'Product_None' then bar_product else parent end else level05_bar end as level05_bar,
				case when generation <= 6  then case when bar_product = 'Product_None' then bar_product else parent end else level06_bar end as level06_bar,
				case when generation <= 7  then case when bar_product = 'Product_None' then bar_product else parent end else level07_bar end as level07_bar,
				case when generation <= 8  then case when bar_product = 'Product_None' then bar_product else parent end else level08_bar end as level08_bar,
				bar_product as level09_bar
		from 	cte_base 
		where 	lower(bar_product) not in ('product_none', 'p60999','oth_service')
	;
		/* Validate : bar_product & portfolio are same & no duplicate portfolio  */ 
		/******************************************************************************
		select count(1), bar_product
		from tmp_bar_product_hierarchy
		group by bar_product
		having count(1) >1;
		******************************************************************************/
		delete from dw.dim_product_restatement;
	
		insert into dw.dim_product_restatement(
				material,
				portfolio,
				portfolio_desc,
				bar_product,
				bar_product_desc,
				member_type,
				generation,
				level01_bar,
				level02_bar,
				level03_bar,
				level04_bar,
				level05_bar,
				level06_bar,
				level07_bar,
				level08_bar,
				level09_bar,
				start_date,
				end_date,
				audit_loadts)
		select mat.material, 
			  mat.portfolio, 
			  isnull(bph.portfolio_desc,'unknown') portfolio_desc,
			  isnull(bph.bar_product, 'unknown') bar_product,
			  isnull(bph.bar_product_desc,'unknown') bar_product_desc,
			  isnull(bph.membertype,'Base') membertype ,
			  isnull(bph.generation,9) generation,
			  isnull(bph.level01_bar,'unknown') level01_bar,
			  isnull(bph.level02_bar,'unknown') level02_bar,
			  isnull(bph.level03_bar,'unknown') level03_bar,
			  isnull(bph.level04_bar,'unknown') level04_bar,
			  isnull(bph.level05_bar,'unknown') level05_bar,
			  isnull(bph.level06_bar,'unknown') level06_bar,
			  isnull(bph.level07_bar,'unknown') level07_bar,
			  isnull(bph.level08_bar,'unknown') level08_bar,
			  isnull(bph.level09_bar,'unknown') level09_bar,
			  cast('1900-01-01' as date) as start_date, 
			  cast('9999-12-31' as date) as end_date, 
			  cast(getdate() as timestamp) as audit_loadts
		from stage_material_to_restate mat
		left join tmp_bar_product_hierarchy bph on lower(bph.portfolio) = lower(mat.portfolio) 
	;

EXCEPTION
		when others then raise info 'exception occur while ingesting data in dim_material_restate_hierarchy';
END
$$
;