
CREATE OR REPLACE PROCEDURE ref_data.p_build_sku_barproduct_mapping()
 LANGUAGE plpgsql
AS $$
BEGIN 
	
	/*  create mapping table for material -> gpp_portfolio 
	 * 	based on [{{ source('bods', 'c11_0material_attr') }}]
	 */
	drop table if exists stage_sku_barproduct_mapping
	;
	create temporary table stage_sku_barproduct_mapping
	diststyle all
	as 
	SELECT 	cast(cmac.matnr as varchar(30)) as material,
--			cmac.wrkst as gpp_code,
			'P' + SPLIT_PART(cmac.wrkst, '-', 4) as bar_product
	FROM 	{{ source('bods', 'c11_0material_attr') }} cmac 
	;
--	select 	* 
--	from 	stage_sku_barproduct_mapping
--	where material = 'CMAS261290'
--	;
	delete from ref_data.sku_barproduct_mapping
	;
	insert into ref_data.sku_barproduct_mapping (
				material,
				bar_product,
				start_date,
				end_date,
				current_flag,
				audit_loadts
		)
		select 	material,
				bar_product,
				cast('1900-01-01' as date) as start_date,
				cast('9999-12-31' as date) as end_date,
				1 current_flag,
				getdate() as audit_loadts
		from 	stage_sku_barproduct_mapping
	;
--	select 	* 
--	from 	ref_data.sku_barproduct_mapping
--	where material = 'CMAS261290'
--	;
	
	exception
		when others then raise info 'exception occur while ingesting data in ref_data.p_build_sku_barproduct_mapping';
end
$$
;