
CREATE OR REPLACE PROCEDURE dw.p_build_dim_transactional_attributes(flag_reload integer)
 LANGUAGE plpgsql
AS $$
Begin
	
	/* This table does not use identity based surrogate key, so does not need Insert -Update Strategy */
	
	/* check for full reload */
	if(flag_reload = 1)
	then
		delete from dw.dim_transactional_attributes;
	end if;
	-- call dw.p_build_dim_transactional_attributes() 
	-- select * from dw.dim_transactional_attributes
	
	delete from dw.dim_transactional_attributes;
	insert into dw.dim_transactional_attributes ( dim_transactional_attributes_id, PCR )
		SELECT 	DISTINCT 
				lower(pcr) as dim_transactional_attributes_id,
				lower(pcr) as PCR
		FROM 	ref_data.rsa_bible 
		WHERE 	pcr IS NOT NULL and 
				pcr != ''
	;
exception
when others then raise info 'exception occur while ingesting data in dim_transactional_attributes';
end;
$$
;