
CREATE OR REPLACE PROCEDURE ref_data.p_build_sku_gpp_mapping()
 LANGUAGE plpgsql
AS $$
BEGIN 
	
	/*  create mapping table for material -> gpp_portfolio/division
	 */
	drop table if exists stage_sku_gpp_mapping
	;
	create temporary table stage_sku_gpp_mapping
	diststyle all
	as 
	SELECT 	cast(cmac.matnr as varchar(30)) as material,
			cmac.wrkst as gpp_code,
			'P' + SPLIT_PART(cmac.wrkst, '-', 4) as gpp_portfolio,
			SPLIT_PART(cmac.wrkst, '-', 2) as gpp_division
	FROM 	{{ source('sapc11', 'mara') }} cmac 
	WHERE 	'P' + SPLIT_PART(cmac.wrkst, '-', 4) is not null
	;
	delete from ref_data.sku_gpp_mapping
	;
	insert into ref_data.sku_gpp_mapping (
				material,
				gpp_portfolio,
				gpp_division,
				start_date,
				end_date,
				current_flag,
				audit_loadts
		)
		select 	material,
				gpp_portfolio,
				gpp_division,
				cast('1900-01-01' as date) as start_date,
				cast('9999-12-31' as date) as end_date,
				1 current_flag,
				getdate() as audit_loadts
		from 	stage_sku_gpp_mapping
	;
	
	exception
		when others then raise info 'exception occur while ingesting data in ref_data.p_build_sku_barproduct_mapping';
end
$$
;