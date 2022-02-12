
CREATE OR REPLACE PROCEDURE ref_data.p_build_sku_brand_mapping_masterdata()
 LANGUAGE plpgsql
AS $$
BEGIN 
    /*
        ref_data.p_build_sku_brand_mapping_masterdata();
        select * from finance_stage.core_tran_delta_agg where source_system = 'E0194' limit 10;
        select count(*) from finance_stage.core_tran_delta_agg where source_system = 'E0194' and fiscal_month_id = 202012;
     */
    
    drop table if exists stage_sku_brand_mapping
    ;
    create temporary table stage_sku_brand_mapping as 
        SELECT  distinct 
                cast(cmac.matnr as varchar(30)) as material,
                cmac.matkl as brand_code,
                brnd.wgbez as brand_map
        FROM    {{ source('sapc11', 'mara') }} cmac 
                left join {{ source('sapc11', 't023t') }} brnd 
                    on  cmac.matkl = brnd.matkl 
                    and brnd.spras = 'E'
                    and brnd.wgbez is not null
    ;
    delete from ref_data.sku_brand_mapping_masterdata;
    insert into ref_data.sku_brand_mapping_masterdata (
                material,
                brand_code,
                brand_map
        )
        select  material,
                brand_code,
                brand_map
        FROM    stage_sku_brand_mapping
    ;
	exception
		when others then raise info 'exception occur while ingesting data in ref_data.p_build_sku_brand_mapping_masterdata';
end
$$
;