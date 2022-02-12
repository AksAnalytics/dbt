
CREATE OR REPLACE PROCEDURE dw.p_build_dim_source_system(flag_reload integer)
 LANGUAGE plpgsql
AS $$
Begin
	/* This table does not use identity based surrogate key, so does not need Insert / Update Strategy */
	
	/* check for full reload */
	if(flag_reload = 1)
	then
		delete from dw.dim_source_system;
	end if;

	delete from dw.dim_source_system;
	insert into dw.dim_source_system ( source_system_id , source_system )
		Select 1 as source_system_id, 
		       'sap_c11' as source_system 
		union all 
		Select 2 as source_system_id, 
		       'sap_p10' as source_system
		union all 
		Select 3 as source_system_id, 
		       'sap_lawson' as source_system
		UNION ALL 
		SELECT 4 AS source_system_id,
		       'hfm' AS source_system
		union all 
		Select 5 as source_system_id,
		   'ext_c11fob' as source_system
		union all 
		Select 6 as source_system_id,
		   'ext_c11std' as source_system
		union all 
		Select 7 as source_system_id,
		   'rsa_bible' as source_system
		union all 
		Select 8 as source_system_id,
		   'agm-inv-adj-gap' as source_system
		union all 
		Select 9 as source_system_id,
		   'adj-wa-tran' as source_system
		union all 
		Select 10 as source_system_id,
		   'adj-wa-tran-gap' as source_system
	;		
exception
when others then raise info 'exception occur while ingesting data in reference_dims';
end;
$$
;