
CREATE OR REPLACE PROCEDURE ref_data.p_build_ref_data_volume_conv_sku()
 LANGUAGE plpgsql
AS $$
BEGIN 
	
delete from ref_data.volume_conv_sku; 
insert into ref_data.volume_conv_sku
Select 	CAST('08130N-PWR' AS varchar(30)) as SKU, cast(12 as numeric(19,2)) as ConversionRate union all 
Select 	CAST('0502SD-PWR'  AS varchar(30)) as SKU, cast(12 as numeric(19,2)) as ConversionRate union all 
Select 	CAST('DFM12720'  AS varchar(30))   as SKU,	cast(100  as numeric(19,2)) as ConversionRate union all 
Select 	CAST('DFM12722'  AS varchar(30))  as SKU,	cast(100  as numeric(19,2)) as ConversionRate union all 
Select 	CAST('DFM12728'  AS varchar(30))  as SKU, cast(100  as numeric(19,2)) as ConversionRate union all 
Select 	CAST('DFM12726'  AS varchar(30))  as SKU,	cast(100  as numeric(19,2)) as ConversionRate union all 
Select 	CAST('DFM12724' AS varchar(30))  as SKU,	cast(100  as numeric(19,2)) as ConversionRate union all 
Select 	CAST('ECC720-I'  AS varchar(30))  as SKU,	cast(4000  as numeric(19,2)) as ConversionRate;
end
$$
;