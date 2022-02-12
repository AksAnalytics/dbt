
CREATE OR REPLACE PROCEDURE ref_data.p_build_sku_barproduct_mapping_p10_bods()
 LANGUAGE plpgsql
AS $$
BEGIN 
	
	/*  create mapping table for material -> bar_product from c11 raw boads data
	 * 	based on [{{ source('bods', 'c11_0material_attr') }}]
	 */
	drop table if exists stage_sku_barproduct_mapping
	;
	create temporary table stage_sku_barproduct_mapping
	diststyle all
	as 
	with cte_base as (	
	select case when cast(prod_cd as varchar(30)) = '' then null else cast(prod_cd as varchar(30)) end as  material, 
		 case when cast(s.bar_product  as varchar(22)) = '' then null else cast(s.bar_product  as varchar(22)) end as  bar_product, 
		 cast(s.cpudt as date)  as postdate , 
		 sum(bar_amt) as dollartotal
	from {{ source('bods', 'p10_0ec_pca_3_trans') }} s
	inner join ref_data.entity rbh on s.bar_entity = rbh.name
	---only accounts thats contributes to sgm pnl structure
	inner join (select 	distinct bar_acct from 	ref_data.pnl_acct) acct on lower(s.bar_acct) = lower(acct.bar_acct) 
	where s.bar_acct is not null 
		and s.bar_entity is not null 
		and s.bar_acct <> ''
		and rbh.level4 = 'GTS_NA'	
		and material not in ('')
		and left(bar_product, 1) in ('P')
		and length(bar_product) = 6
		and bar_product not in ('P60999')
		--and bar_year >= 2019
		group by  case when cast(prod_cd as varchar(30)) = '' then null else cast(prod_cd as varchar(30)) end , 
				case when cast(s.bar_product  as varchar(22)) = '' then null else cast(s.bar_product  as varchar(22)) end, 
				cast(s.cpudt as date)
		),cte_base_next as (
					select 	base.material,
							base.bar_product,
							base.postdate,
							lead(base.bar_product) over(partition by base.material order by base.postdate) as bar_product_next
					from 	cte_base base
				)
				,cte_base_historical as (
					select 	nxt.material,
							nxt.bar_product,
							nxt.postdate,
							nxt.bar_product_next,
							lead(nxt.postdate) over (partition by nxt.material order by nxt.postdate) as postdate_next,
							row_number() over (partition by nxt.material order by nxt.postdate) rnk
					from 	cte_base_next nxt
					where 	nxt.bar_product != nxt.bar_product_next or 
							nxt.bar_product_next is null
				)select 	hist.material,
						hist.bar_product,				
					case
						when hist.rnk = 1 then cast('1900-01-01' as date) 
						else cast(hist.postdate as date) 
					end as start_date,
					case
						when hist.bar_product_next is null then cast('9999-12-31' as date) 
						else cast(dateadd(day,-1,hist.postdate_next) as date)
					end as end_date,
					case when hist.bar_product_next is null then 1 else 0 end as current_flag,
					getdate() as audit_loadts
			from 	cte_base_historical as hist
		;
	
	delete from ref_data.sku_barproduct_mapping_p10_bods
	;
	insert into ref_data.sku_barproduct_mapping_p10_bods (
				material,
				bar_product,
				start_date,
				end_date,
				current_flag,
				audit_loadts
		)
		select 	material,
				bar_product,
				start_date,
				end_date,
				current_flag,
				getdate() as audit_loadts
		from 	stage_sku_barproduct_mapping
	;
	
--	select 	* 
--	from 	ref_data.sku_barproduct_mapping_p10_bods
--	where material = '3-203-156-41'
--	;
---0 overalaps	
--select count(1), material 
--from ref_data.sku_barproduct_mapping_p10_bods
--group by material 
--having count(1) >1;
	exception
		when others then raise info 'exception occur while ingesting data in ref_data.sku_barproduct_mapping_bods';
end
$$
;