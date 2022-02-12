
CREATE OR REPLACE PROCEDURE dw.p_build_dim_product(flag_reload integer)
 LANGUAGE plpgsql
AS $$
Begin
	/*
	 * 			call dw.p_build_dim_product(0) -- incremental
	 * 			call dw.p_build_dim_product(1) -- kill n fill
	 * 			select * from dw.dim_product where material like '%ADJ_ROYALTY%'
	 */
	
	/* check for full reload */
	if(flag_reload = 1)
	then
		delete from dw.dim_product;
	end if;
	
	/* current version of every material 
	 * 	all of spras E + Z (where E doesn't exists)
	 * 	for c11 & P10 & Lawson
	 * 
	 */
	drop table if exists tmp_material_master
	;
	create temporary table tmp_material_master as 
	with
		cte_mm_E as (
			select 	mm.matnr as material,
					mm.maktx as material_desc
			from 	{{ source('sapc11', 'makt') }} mm
			where 	mm.spras = 'E'
		)
		,cte_mm_Z as (
			select 	mm.matnr as material,
					mm.maktx as material_desc
			from 	{{ source('sapc11', 'makt') }} mm 
					left outer join cte_mm_E mm_E
						on	mm_E.material = mm.matnr
			where 	mm.spras = 'Z' and 
					mm_E.material is null 
		)
		,cte_c11 as (
			select 	mm_E.material,
					mm_E.material_desc
			from 	cte_mm_E mm_E
			union all
			select 	mm_Z.material,
					mm_Z.material_desc
			from 	cte_mm_Z mm_Z
		)
	select 	c11.material,
			c11.material_desc
	from 	cte_c11 as c11 	
	;
	insert into tmp_material_master (material, material_desc)
		with
			cte_mm_E as (
				select 	mm.matnr as material,
						mm.maktx as material_desc
				from 	{{ source('sapp10', 'makt') }} mm
				where 	mm.spras = 'E'
			)
			,cte_mm_Z as (
				select 	mm.matnr as material,
						mm.maktx as material_desc
				from 	{{ source('sapp10', 'makt') }} mm 
						left outer join cte_mm_E mm_E
							on	mm_E.material = mm.matnr
				where 	mm.spras = 'Z' and 
						mm_E.material is null 
			)
			,cte_p10 as (
				select 	mm_E.material,
						mm_E.material_desc
				from 	cte_mm_E mm_E
				union all
				select 	mm_Z.material,
						mm_Z.material_desc
				from 	cte_mm_Z mm_Z
			)
		select 	p10.material,
				p10.material_desc
		from 	cte_p10 as p10 
				left outer join tmp_material_master as mapping
					on 	lower(mapping.material) = lower(p10.material)
		where 	mapping.material is null
	;
	insert into tmp_material_master (material, material_desc)
		select 	lawson.prod_cd as material,
				MAX(lawson.prod_name) as material_desc
		from 	{{ source('bods', 'extr_lawson_mac_prod') }} as lawson
				left outer join tmp_material_master as mapping
					on 	lower(mapping.material) = lower(lawson.prod_cd)
		where 	lawson.div_cd = 'USM' and 
				mapping.material is NULL
		group by lawson.prod_cd
	;

	--05/01:shrikant k changes
	--cast columns to varchar50
	--add new combinations from rule 21 and 26
	/* trans-based mapping: material -> bar_brand / bar_product */
	drop table if exists tmp_trans_material_map
	;
	create temporary table tmp_trans_material_map as 
		select 	distinct 
				cast(lower(bcta.material) as varchar(50)) as material,
				cast(lower(bcta.mapped_bar_product) as varchar(50)) as bar_product,
				coalesce(cast(lower(bcta.mapped_bar_brand) as varchar(50)),'unknown') as bar_brand
		from 	stage.bods_core_transaction_agg bcta 
		where 	bcta.material is not null
		
	;
	insert into tmp_trans_material_map (material, bar_product, bar_brand)
		select 	distinct 
				lower(alloc.alloc_material) as material,
				lower(alloc.alloc_bar_product) as bar_product,
				coalesce(lower(alloc.mapped_bar_brand),'unknown') as bar_brand
		from 	stage.sgm_allocated_data_rule_09 alloc
				left outer join tmp_trans_material_map tr
					on 	lower(tr.material) = lower(alloc.alloc_material) and 
						lower(tr.bar_product) = lower(alloc.alloc_bar_product) and 
						coalesce(lower(tr.bar_brand),'unknown') = coalesce(lower(alloc.mapped_bar_brand),'unknown')
		where 	tr.material is null and 
				alloc.alloc_material is NOT NULL
		;
	insert into tmp_trans_material_map (material, bar_product, bar_brand)
		select 	distinct 
				lower(alloc.alloc_material) as material,
				lower(alloc.alloc_bar_product) as bar_product,
				coalesce(lower(alloc.mapped_bar_brand),'unknown') as bar_brand
		from 	stage.sgm_allocated_data_rule_21 alloc
				left outer join tmp_trans_material_map tr
					on 	lower(tr.material) = lower(alloc.alloc_material) and 
						lower(tr.bar_product) = lower(alloc.alloc_bar_product) and 
						coalesce(lower(tr.bar_brand),'unknown') = coalesce(lower(alloc.mapped_bar_brand),'unknown')
		where 	tr.material is null and 
				alloc.alloc_material is NOT NULL;
		
	insert into tmp_trans_material_map (material, bar_product, bar_brand)
		select 	distinct 
				lower(alloc.alloc_material) as material,
				lower(alloc.alloc_bar_product) as bar_product,
				coalesce(lower(alloc.mapped_bar_brand),'unknown') as bar_brand
		from 	stage.sgm_allocated_data_rule_26 alloc
				left outer join tmp_trans_material_map tr
					on 	lower(tr.material) = lower(alloc.alloc_material) and 
						lower(tr.bar_product) = lower(alloc.alloc_bar_product) and 
						coalesce(lower(tr.bar_brand),'unknown') = coalesce(lower(alloc.mapped_bar_brand),'unknown')
		where 	tr.material is null and 
				alloc.alloc_material is NOT NULL
	;	
	
	/* special members for Allocation Exception Royalty (A40910) */
	drop table if exists tmp_allocation_exception_royalty_material_map
	;
	create temporary table tmp_allocation_exception_royalty_material_map as 
		select 	distinct 
				'ADJ_ROYALTY' as material,
				lower(bcta.mapped_bar_product) as bar_product,
				coalesce(lower(bcta.mapped_bar_brand),'unknown') as bar_brand
		from 	stage.bods_core_transaction_agg bcta 
		where 	bcta.bar_acct = 'A40910'
	;
			
	/* special members for Allocation Exception CUSTOMER_NONE, PRODUCT_NONE (Rule 27) */
	drop table if exists tmp_allocation_exception_rule27_material_map
	;
	create temporary table tmp_allocation_exception_rule27_material_map as 
		select 	distinct 
				'ADJ_NO_PROD' as material,
				lower(alloc.mapped_bar_product) as bar_product,
				coalesce(lower(alloc.mapped_bar_brand),'unknown') as bar_brand
		from 	stage.sgm_allocated_data_rule_27 alloc
	;
			
	/* special members for Allocation Exception SERVICE (Rule 28) */
	drop table if exists tmp_allocation_exception_rule28_material_map
	;
	create temporary table tmp_allocation_exception_rule28_material_map as 
		select 	distinct 
				'ADJ_SERVICE' as material,
				lower(alloc.mapped_bar_product) as bar_product,
				coalesce(lower(alloc.mapped_bar_brand),'unknown') as bar_brand
		from 	stage.sgm_allocated_data_rule_28 alloc
		union all 
		select 	distinct 
				'ADJ_REBUILD' as material,
				lower(alloc.mapped_bar_product) as bar_product,
				coalesce(lower(alloc.mapped_bar_brand),'unknown') as bar_brand
		from 	stage.sgm_allocated_data_rule_28 alloc
	;
	/* special members for Allocation Exception RSA (reconcile) */
	drop table if exists tmp_allocation_exception_rsa_reconcile_material_map
	;
	create temporary table tmp_allocation_exception_rsa_reconcile_material_map as 
		Select	distinct
				'ADJ_RSA' as material,
				'ADJ_RSA' as bar_product,
				'N/A' as bar_brand,
				lower(coalesce(stg.rsa_reconcile_bar_division,'unknown')) as rsa_division,
				'ADJ_RSA' as bar_product_level08_category,
				'ADJ_RSA' as bar_product_level09_portfolio
		from 	stage.sgm_allocated_data_rule_23 stg
		where	stg.source_system = 'rsa_bible' and 
				stg.dataprocessing_outcome_id = 2
	;
	/* special members for Allocation Exception RSA (alloc) */
	insert into tmp_trans_material_map (material, bar_product, bar_brand)
		Select	distinct
				lower(alloc.alloc_material) as material,
				lower(alloc.alloc_bar_product) as bar_product,
				lower(alloc.mapped_bar_brand) as bar_brand
		from 	stage.sgm_allocated_data_rule_23 alloc
				left outer join tmp_trans_material_map tr
					on 	lower(tr.material) = lower(alloc.alloc_material) and 
						lower(tr.bar_product) = lower(alloc.alloc_bar_product) and 
						lower(tr.bar_brand) = lower(alloc.mapped_bar_brand) 
		where 	alloc.source_system = 'rsa_bible' and 
				alloc.dataprocessing_outcome_id != 2 and 
				tr.material is NULL and 
				alloc.alloc_material is NOT NULL
	;
		
	/* current version of bar_product hierarchy */
	drop table if exists tmp_bar_product_hierarchy
	;
	create temporary table tmp_bar_product_hierarchy as 
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
	;
	/* current version of commercial product hierarchy */
	drop table if exists tmp_bar_gpp_commercial_hierarchy
	;
	create temporary table tmp_bar_gpp_commercial_hierarchy as 
		select 	gpp.material,
				gpp.gpp_portfolio,
				gpp.gpp_division as gpp_division_code,
				hier.gts as prd_comm_level01_gts,
				hier.super_bu as prd_comm_level02_super_bu,
				hier.subcategory as prd_comm_level03_subcategory,
				hier.category as prd_comm_level04_category,
				gpp.gpp_portfolio as prd_comm_level05_gpp_portfolio
		from 	ref_data.sku_gpp_mapping gpp
				inner join ref_data.product_commercial_hierarchy hier
					ON lower(CONCAT('P', hier.gpp_portfolio)) = lower(gpp.gpp_portfolio)
	;
	/* current version of bar_product hierarchy */
	drop table if exists tmp_bar_product_hierarchy_rsa_reconcile
	;
	create temporary table tmp_bar_product_hierarchy_rsa_reconcile as
		select  distinct 
				bph.level01_bar,
				bph.level02_bar,
				bph.level03_bar,
				bph.level04_bar,
				bph.level05_bar,
				bph.level06_bar,
				bph.level07_bar
		from 	tmp_bar_product_hierarchy bph
				inner join tmp_allocation_exception_rsa_reconcile_material_map rsa
					on 	lower(rsa.rsa_division) = lower(bph.level07_bar)
	;

	/* ------------------------------------------------------------------ 
	 * 	Part 02: Create stage table to build from scratch
	 * ------------------------------------------------------------------
	 */
	DROP TABLE IF EXISTS stage_dim_product;
	CREATE TEMPORARY TABLE stage_dim_product (
		product_id varchar(200) NULL,
		material varchar(100) NULL,
		bar_product varchar(100) NULL,
		product_brand varchar(100) NULL,
		
		sku varchar(200) NULL,
		
		portfolio varchar(100) NULL,
		portfolio_desc varchar(200) NULL,
		
		member_type varchar(50) NULL,
		generation int4 NULL,
		level01_bar varchar(100) NULL,
		level02_bar varchar(100) NULL,
		level03_bar varchar(100) NULL,
		level04_bar varchar(100) NULL,
		level05_bar varchar(100) NULL,
		level06_bar varchar(100) NULL,
		level07_bar varchar(100) NULL,
		level08_bar varchar(100) NULL,
		level09_bar varchar(100) NULL,
		start_date date NOT NULL,
		end_date date NOT NULL,
		audit_loadts timestamp NOT NULL,
		
		gpp_division_code varchar(10) NULL,
		prd_comm_level01_gts varchar(100) NULL,
		prd_comm_level02_super_bu varchar(100) NULL,
		prd_comm_level03_subcategory varchar(100) NULL,
		prd_comm_level04_category varchar(100) NULL,
		prd_comm_level05_gpp_portfolio varchar(100) NULL
	) DISTSTYLE ALL 
	;
	/* ------------------------------------------------------------------ 
	 * 	Part 03: Load stage table
	 * ------------------------------------------------------------------
	 */
	insert into stage_dim_product (
				product_id, 
				material,
				bar_product,
				product_brand,
				
				sku,
				
				portfolio, 
				portfolio_desc,
				
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
				audit_loadts,
		
				gpp_division_code,
				prd_comm_level01_gts,
				prd_comm_level02_super_bu,
				prd_comm_level03_subcategory,
				prd_comm_level04_category,
				prd_comm_level05_gpp_portfolio
		)
		select  'unknown|unknown|unknown' as product_id,
				'unknown' as material,
			    'unknown' as bar_product,
			    'unknown' as product_brand,
			    
			    'unknown' as sku,
			    
			    'unknown' as portfolio, 
			    'unknown' as portfolio_desc,
			    
			    'unknown' as member_type,
				cast(null as int) as generation,
				'unknown' as level01_bar,
				'unknown' as level02_bar,
				'unknown' as level03_bar,
				'unknown' as level04_bar,
				'unknown' as level05_bar,
				'unknown' as level06_bar,
				'unknown' as level07_bar,
				'unknown' as level08_bar,
				'unknown' as level09_bar,
			    cast('01-01-1900' as date) start_date,
	  			cast('12-31-9999' as date) as end_date,
	  			getdate() as audit_loadts,
		
				NULL as gpp_division_code,
				'unknown' as prd_comm_level01_gts,
				'unknown' as prd_comm_level02_super_bu,
				'unknown' as prd_comm_level03_subcategory,
				'unknown' as prd_comm_level04_category,
				'unknown' as prd_comm_level05_gpp_portfolio
	  	union all 
		select 	prd_map.material 
				|| '|' || coalesce(prd_map.bar_product,'unknown') 
				|| '|' || coalesce(prd_map.bar_brand,'unknown') as product_id,
				
				prd_map.material as material,
				coalesce(prd_map.bar_product,'unknown') as bar_product,
				coalesce(prd_map.bar_brand,'unknown') as product_brand,
				
				mm.material_desc as sku,
				
				case when hier.bar_product is null then 'unknown' else hier.portfolio       end as portfolio,
				case when hier.bar_product is null then 'unknown' else hier.portfolio_desc  end as portfolio_desc,
				
				case when hier.bar_product is null then 'unknown' else hier.membertype      end as membertype,
				hier.generation,
				case when hier.level01_bar is null then 'unknown' else hier.level01_bar end as level01_bar,
				case when hier.level02_bar is null then 'unknown' else hier.level02_bar end as level02_bar,
				case when hier.level03_bar is null then 'unknown' else hier.level03_bar end as level03_bar,
				case when hier.level04_bar is null then 'unknown' else hier.level04_bar end as level04_bar,
				case when hier.level05_bar is null then 'unknown' else hier.level05_bar end as level05_bar,
				case when hier.level06_bar is null then 'unknown' else hier.level06_bar end as level06_bar,
				case when hier.level07_bar is null then 'unknown' else hier.level07_bar end as level07_bar,
				case when hier.level08_bar is null then 'unknown' else hier.level08_bar end as level08_bar,
				case when hier.level09_bar is null then 'unknown' else hier.level09_bar end as level09_bar,
			    cast('01-01-1900' as date) start_date,
	  			cast('12-31-9999' as date) as end_date,
	  			getdate() as audit_loadts,
		
				COALESCE( comm_hier.gpp_division_code, 'unknown') as gpp_division_code,
				COALESCE( comm_hier.prd_comm_level01_gts, 'unknown') as prd_comm_level01_gts,
				COALESCE( comm_hier.prd_comm_level02_super_bu, 'unknown') as prd_comm_level02_super_bu,
				COALESCE( comm_hier.prd_comm_level03_subcategory, 'unknown') as prd_comm_level03_subcategory,
				COALESCE( comm_hier.prd_comm_level04_category, 'unknown') as prd_comm_level04_category,
				COALESCE( comm_hier.prd_comm_level05_gpp_portfolio, 'unknown') as prd_comm_level05_gpp_portfolio
	  			
		from 	tmp_trans_material_map prd_map
				left outer join tmp_material_master mm
					on 	lower(mm.material) = lower(prd_map.material)
				left outer join tmp_bar_product_hierarchy as hier
					on 	lower(hier.bar_product) = lower(prd_map.bar_product)
				left outer join tmp_bar_gpp_commercial_hierarchy comm_hier
					on 	lower(comm_hier.material) = lower(mm.material)
		;
	

	/* allocation exception Royalty */
	insert into stage_dim_product (
				product_id, 
				material,
				bar_product,
				product_brand,
				
				sku,
				
				portfolio, 
				portfolio_desc,
				
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
				audit_loadts,
		
				gpp_division_code,
				prd_comm_level01_gts,
				prd_comm_level02_super_bu,
				prd_comm_level03_subcategory,
				prd_comm_level04_category,
				prd_comm_level05_gpp_portfolio
		)
		select 	prd_map.material || '|' || prd_map.bar_product || '|' || prd_map.bar_brand as product_id,
				
				prd_map.material,
				prd_map.bar_product,
				prd_map.bar_brand as product_brand,
				
				prd_map.material as sku,
				
				case when hier.bar_product is null then 'unknown' else hier.portfolio       end as portfolio,
				case when hier.bar_product is null then 'unknown' else hier.portfolio_desc  end as portfolio_desc,
				
				case when hier.bar_product is null then 'unknown' else hier.membertype      end as membertype,
				hier.generation,
				case when hier.level01_bar is null then 'unknown' else hier.level01_bar end as level01_bar,
				case when hier.level02_bar is null then 'unknown' else hier.level02_bar end as level02_bar,
				case when hier.level03_bar is null then 'unknown' else hier.level03_bar end as level03_bar,
				case when hier.level04_bar is null then 'unknown' else hier.level04_bar end as level04_bar,
				case when hier.level05_bar is null then 'unknown' else hier.level05_bar end as level05_bar,
				case when hier.level06_bar is null then 'unknown' else hier.level06_bar end as level06_bar,
				case when hier.level07_bar is null then 'unknown' else hier.level07_bar end as level07_bar,
				case when hier.level08_bar is null then 'unknown' else hier.level08_bar end as level08_bar,
				case when hier.level09_bar is null then 'unknown' else hier.level09_bar end as level09_bar,
			    cast('01-01-1900' as date) start_date,
	  			cast('12-31-9999' as date) as end_date,
	  			getdate() as audit_loadts,
		
				'unknown' as gpp_division_code,
				'unknown' as prd_comm_level01_gts,
				'unknown' as prd_comm_level02_super_bu,
				'unknown' as prd_comm_level03_subcategory,
				'unknown' as prd_comm_level04_category,
				'unknown' as prd_comm_level05_gpp_portfolio
	  			
		from 	tmp_allocation_exception_royalty_material_map prd_map
				left outer join tmp_bar_product_hierarchy as hier
					on 	lower(hier.bar_product) = lower(prd_map.bar_product)
	;
	
	/* allocation exception PRODUCT_NONE / CUSTOMER_NONE (Rule 27) */
	insert into stage_dim_product (
				product_id, 
				material,
				bar_product,
				product_brand,
				
				sku,
				
				portfolio, 
				portfolio_desc,
				
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
				audit_loadts,
		
				gpp_division_code,
				prd_comm_level01_gts,
				prd_comm_level02_super_bu,
				prd_comm_level03_subcategory,
				prd_comm_level04_category,
				prd_comm_level05_gpp_portfolio
		)
		select 	prd_map.material || '|' || prd_map.bar_product || '|' || prd_map.bar_brand as product_id,
				
				prd_map.material,
				prd_map.bar_product,
				prd_map.bar_brand as product_brand,
				
				prd_map.material as sku,
				
				case when hier.bar_product is null then 'unknown' else hier.portfolio       end as portfolio,
				case when hier.bar_product is null then 'unknown' else hier.portfolio_desc  end as portfolio_desc,
				
				case when hier.bar_product is null then 'unknown' else hier.membertype      end as membertype,
				hier.generation,
				case when hier.level01_bar is null then 'unknown' else hier.level01_bar end as level01_bar,
				case when hier.level02_bar is null then 'unknown' else hier.level02_bar end as level02_bar,
				case when hier.level03_bar is null then 'unknown' else hier.level03_bar end as level03_bar,
				case when hier.level04_bar is null then 'unknown' else hier.level04_bar end as level04_bar,
				case when hier.level05_bar is null then 'unknown' else hier.level05_bar end as level05_bar,
				case when hier.level06_bar is null then 'unknown' else hier.level06_bar end as level06_bar,
				case when hier.level07_bar is null then 'unknown' else hier.level07_bar end as level07_bar,
				case when hier.level08_bar is null then 'unknown' else hier.level08_bar end as level08_bar,
				case when hier.level09_bar is null then 'unknown' else hier.level09_bar end as level09_bar,
			    cast('01-01-1900' as date) start_date,
	  			cast('12-31-9999' as date) as end_date,
	  			getdate() as audit_loadts,
		
				'unknown' as gpp_division_code,
				'unknown' as prd_comm_level01_gts,
				'unknown' as prd_comm_level02_super_bu,
				'unknown' as prd_comm_level03_subcategory,
				'unknown' as prd_comm_level04_category,
				'unknown' as prd_comm_level05_gpp_portfolio
	  			
		from 	tmp_allocation_exception_rule27_material_map as prd_map
				left outer join tmp_bar_product_hierarchy as hier
					on 	lower(hier.bar_product) = lower(prd_map.bar_product)
	;
	
	/* allocation exception SERVICE (Rule 28) */
	insert into stage_dim_product (
				product_id, 
				material,
				bar_product,
				product_brand,
				
				sku,
				
				portfolio, 
				portfolio_desc,
				
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
				audit_loadts,
		
				gpp_division_code,
				prd_comm_level01_gts,
				prd_comm_level02_super_bu,
				prd_comm_level03_subcategory,
				prd_comm_level04_category,
				prd_comm_level05_gpp_portfolio
		)
		select 	prd_map.material || '|' || prd_map.bar_product || '|' || prd_map.bar_brand as product_id,
				
				prd_map.material,
				prd_map.bar_product,
				prd_map.bar_brand as product_brand,
				
				prd_map.material as sku,
				
				case when hier.bar_product is null then 'unknown' else hier.portfolio       end as portfolio,
				case when hier.bar_product is null then 'unknown' else hier.portfolio_desc  end as portfolio_desc,
				
				case when hier.bar_product is null then 'unknown' else hier.membertype      end as membertype,
				hier.generation,
				case when hier.level01_bar is null then 'unknown' else hier.level01_bar end as level01_bar,
				case when hier.level02_bar is null then 'unknown' else hier.level02_bar end as level02_bar,
				case when hier.level03_bar is null then 'unknown' else hier.level03_bar end as level03_bar,
				case when hier.level04_bar is null then 'unknown' else hier.level04_bar end as level04_bar,
				case when hier.level05_bar is null then 'unknown' else hier.level05_bar end as level05_bar,
				case when hier.level06_bar is null then 'unknown' else hier.level06_bar end as level06_bar,
				case when hier.level07_bar is null then 'unknown' else hier.level07_bar end as level07_bar,
				case when hier.level08_bar is null then 'unknown' else hier.level08_bar end as level08_bar,
				case when hier.level09_bar is null then 'unknown' else hier.level09_bar end as level09_bar,
			    cast('01-01-1900' as date) start_date,
	  			cast('12-31-9999' as date) as end_date,
	  			getdate() as audit_loadts,
		
				'unknown' as gpp_division_code,
				'unknown' as prd_comm_level01_gts,
				'unknown' as prd_comm_level02_super_bu,
				'unknown' as prd_comm_level03_subcategory,
				'unknown' as prd_comm_level04_category,
				'unknown' as prd_comm_level05_gpp_portfolio
	  			
		from 	tmp_allocation_exception_rule28_material_map as prd_map
				left outer join tmp_bar_product_hierarchy as hier
					on 	lower(hier.bar_product) = lower(prd_map.bar_product)
	;

	/* allocation exception RSA */
	insert into stage_dim_product (
				product_id, 
				material,
				bar_product,
				product_brand,
				
				sku,
				
				portfolio, 
				portfolio_desc,
				
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
				audit_loadts,
		
				gpp_division_code,
				prd_comm_level01_gts,
				prd_comm_level02_super_bu,
				prd_comm_level03_subcategory,
				prd_comm_level04_category,
				prd_comm_level05_gpp_portfolio
		)
		select 	prd_map.material || '|' || 
					prd_map.rsa_division || '|' || 
					prd_map.bar_brand
					as product_id,
				
				prd_map.material as material,
				prd_map.bar_product as bar_product,
				prd_map.bar_brand as product_brand,
				
				prd_map.material as sku,
				
				prd_map.bar_product as portfolio,
				prd_map.bar_product as portfolio_desc,
				
				null as membertype,
				null generation,
				case when hier.level01_bar is null then 'unknown' else hier.level01_bar end as level01_bar,
				case when hier.level02_bar is null then 'unknown' else hier.level02_bar end as level02_bar,
				case when hier.level03_bar is null then 'unknown' else hier.level03_bar end as level03_bar,
				case when hier.level04_bar is null then 'unknown' else hier.level04_bar end as level04_bar,
				case when hier.level05_bar is null then 'unknown' else hier.level05_bar end as level05_bar,
				case when hier.level06_bar is null then 'unknown' else hier.level06_bar end as level06_bar,
				case when hier.level07_bar is null then 'unknown' else hier.level07_bar end as level07_bar,
--				prd_map.bar_product_level08_category as level08_bar,
--				prd_map.bar_product_level09_portfolio as level09_bar,
				case when hier.level07_bar is null then 'unknown' else hier.level07_bar end as level08_bar,
				case when hier.level07_bar is null then 'unknown' else hier.level07_bar end as level09_bar,
			    cast('01-01-1900' as date) start_date,
	  			cast('12-31-9999' as date) as end_date,
	  			getdate() as audit_loadts,
		
				'unknown' as gpp_division_code,
				'unknown' as prd_comm_level01_gts,
				'unknown' as prd_comm_level02_super_bu,
				'unknown' as prd_comm_level03_subcategory,
				'unknown' as prd_comm_level04_category,
				'unknown' as prd_comm_level05_gpp_portfolio
	  			
		from 	tmp_allocation_exception_rsa_reconcile_material_map prd_map
				left outer join tmp_bar_product_hierarchy_rsa_reconcile as hier
					on 	lower(hier.level07_bar) = lower(prd_map.rsa_division)
				left outer join stage_dim_product stg
					on 	lower(stg.material) = lower(prd_map.material) and 
						lower(stg.bar_product) = lower(prd_map.bar_product) and
						lower(stg.product_brand) = lower(prd_map.bar_brand)
		where 	stg.material is null
	;

	/* ------------------------------------------------------------------ 
	 * 	Part 04: Create placeholder rows for records in hierarchy 
	 *		that don't exist in any transactions that have been
	 *		processed.
	 *
	 *	2021-04-29 BA: crossjoin w/ brand
	 * ------------------------------------------------------------------
	 */
	insert into stage_dim_product (
				product_id, 
				material,
				bar_product,
				product_brand,
				
				sku,
				
				portfolio, 
				portfolio_desc,
				
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
				audit_loadts,
		
				gpp_division_code,
				prd_comm_level01_gts,
				prd_comm_level02_super_bu,
				prd_comm_level03_subcategory,
				prd_comm_level04_category,
				prd_comm_level05_gpp_portfolio
		)
		select 	'BA&R placeholder|' || hier.bar_product || '|' || brand.mapped_bar_brand as product_id,
				'BA&R placeholder' as material,
				hier.bar_product as bar_product,
				brand.mapped_bar_brand as product_brand,
				
				'BA&R placeholder' as sku,
				
				case when hier.bar_product is null then 'unknown' else hier.portfolio       end as portfolio,
				case when hier.bar_product is null then 'unknown' else hier.portfolio_desc  end as portfolio_desc,
				
				case when hier.bar_product is null then 'unknown' else hier.membertype      end as membertype,
				hier.generation,
				case when hier.level01_bar is null then 'unknown' else hier.level01_bar end as level01_bar,
				case when hier.level02_bar is null then 'unknown' else hier.level02_bar end as level02_bar,
				case when hier.level03_bar is null then 'unknown' else hier.level03_bar end as level03_bar,
				case when hier.level04_bar is null then 'unknown' else hier.level04_bar end as level04_bar,
				case when hier.level05_bar is null then 'unknown' else hier.level05_bar end as level05_bar,
				case when hier.level06_bar is null then 'unknown' else hier.level06_bar end as level06_bar,
				case when hier.level07_bar is null then 'unknown' else hier.level07_bar end as level07_bar,
				case when hier.level08_bar is null then 'unknown' else hier.level08_bar end as level08_bar,
				case when hier.level09_bar is null then 'unknown' else hier.level09_bar end as level09_bar,
			    cast('01-01-1900' as date) start_date,
	  			cast('12-31-9999' as date) as end_date,
	  			getdate() as audit_loadts,
		
				NULL as gpp_division_code,
				'unknown' as prd_comm_level01_gts,
				'unknown' as prd_comm_level02_super_bu,
				'unknown' as prd_comm_level03_subcategory,
				'unknown' as prd_comm_level04_category,
				'unknown' as prd_comm_level05_gpp_portfolio
		from 	tmp_bar_product_hierarchy hier
				cross join (
					select  distinct lower(bta.mapped_bar_brand) as mapped_bar_brand
					from 	stage.bods_core_transaction_agg bta
					where 	bta.mapped_bar_brand is not null
					union all
					select 	'BA&R placeholder' as mapped_bar_brand
				) brand
				left outer join dw.dim_product dp
					on 	lower(dp.level01_bar) = lower(case when hier.level01_bar is null then 'unknown' else hier.level01_bar end) and 
						lower(dp.level02_bar) = lower(case when hier.level02_bar is null then 'unknown' else hier.level02_bar end) and 
						lower(dp.level03_bar) = lower(case when hier.level03_bar is null then 'unknown' else hier.level03_bar end) and 
						lower(dp.level04_bar) = lower(case when hier.level04_bar is null then 'unknown' else hier.level04_bar end) and 
						lower(dp.level05_bar) = lower(case when hier.level05_bar is null then 'unknown' else hier.level05_bar end) and  
						lower(dp.level06_bar) = lower(case when hier.level06_bar is null then 'unknown' else hier.level06_bar end) and 
						lower(dp.level07_bar) = lower(case when hier.level07_bar is null then 'unknown' else hier.level07_bar end) and 
						lower(dp.level08_bar) = lower(case when hier.level08_bar is null then 'unknown' else hier.level08_bar end) and 
						lower(dp.level09_bar) = lower(case when hier.level09_bar is null then 'unknown' else hier.level09_bar end)
		where 	dp.level09_bar is null
	;

	/* ------------------------------------------------------------------ 
	 * 	Part 05: Update existing rows in target table
	 *		TODO: add hash check to skip rows w/ no changes
	 * ------------------------------------------------------------------
	 */
	UPDATE 	dw.dim_product
	SET 	sku = stg.sku,
			
			portfolio = stg.portfolio, 
			portfolio_desc = stg.portfolio_desc, 
			
			member_type = stg.member_type, 
			generation = stg.generation,
			level01_bar = stg.level01_bar,
			level02_bar = stg.level02_bar,
			level03_bar = stg.level03_bar,
			level04_bar = stg.level04_bar,
			level05_bar = stg.level05_bar,
			level06_bar = stg.level06_bar,
			level07_bar = stg.level07_bar,
			level08_bar = stg.level08_bar,
			level09_bar = stg.level09_bar,
			start_date = stg.start_date,
			end_date = stg.end_date,
			audit_loadts = stg.audit_loadts,
		
			gpp_division_code = stg.gpp_division_code,
			prd_comm_level01_gts = stg.prd_comm_level01_gts,
			prd_comm_level02_super_bu = stg.prd_comm_level02_super_bu,
			prd_comm_level03_subcategory = stg.prd_comm_level03_subcategory,
			prd_comm_level04_category = stg.prd_comm_level04_category,
			prd_comm_level05_gpp_portfolio = stg.prd_comm_level05_gpp_portfolio
			
	FROM 	stage_dim_product stg 
	WHERE 	lower(stg.product_id) = lower(dim_product.product_id)
		AND lower(dim_product.material) != lower('BA&R placeholder')
	;
	/* ------------------------------------------------------------------ 
	 * 	Part 06: Update existing 'placeholder' rows in target table
	 * 		that now have a product id, sku_name, and/or brand
	 * ------------------------------------------------------------------
	 */
  
	/* ------------------------------------------------------------------ 
	 * 	Part 07: Insert any new rows
	 * ------------------------------------------------------------------
	 */
	insert into dw.dim_product (
				product_id, 
				material,
				bar_product,
				product_brand,
				
				sku,
				portfolio, 
				portfolio_desc,
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
				audit_loadts,
		
				gpp_division_code,
				prd_comm_level01_gts,
				prd_comm_level02_super_bu,
				prd_comm_level03_subcategory,
				prd_comm_level04_category,
				prd_comm_level05_gpp_portfolio
		)
		select 	stg.product_id, 
				UPPER(stg.material) as material,
				stg.bar_product,
				UPPER(stg.product_brand) as product_brand,
				
				stg.sku,
				stg.portfolio, 
				stg.portfolio_desc,
				stg.member_type,  
				stg.generation,
				stg.level01_bar,
				stg.level02_bar,
				stg.level03_bar,
				stg.level04_bar,
				stg.level05_bar,
				stg.level06_bar,
				stg.level07_bar,
				stg.level08_bar,
				stg.level09_bar,
				stg.start_date, 
				stg.end_date, 
				stg.audit_loadts,
		
				stg.gpp_division_code,
				stg.prd_comm_level01_gts,
				stg.prd_comm_level02_super_bu,
				stg.prd_comm_level03_subcategory,
				stg.prd_comm_level04_category,
				stg.prd_comm_level05_gpp_portfolio
		from 	stage_dim_product stg
				left outer join dw.dim_product dp
					on 	lower(stg.product_id) = lower(dp.product_id)
		where	dp.product_id is null
	;
exception
when others then raise info 'exception occur while ingesting data in dim_prod';
END
$$
;