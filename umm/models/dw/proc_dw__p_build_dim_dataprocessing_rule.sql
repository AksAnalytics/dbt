
CREATE OR REPLACE PROCEDURE dw.p_build_dim_dataprocessing_rule(flag_reload integer)
 LANGUAGE plpgsql
AS $$
--DECLARE variables
BEGIN  
	/* This table does not use identity based surrogate key, so does not need Insert / Update Strategy */
	
	/* check for full reload */
	if(flag_reload = 1)
	then
		delete from dw.dim_dataprocessing_rule;
	end if;

	delete from dw.dim_dataprocessing_rule;
	INSERT INTO dw.dim_dataprocessing_rule (
				data_processing_ruleid, 
				dataprocessing_group,
				soldtoflag, 
				barcustflag, 
				skuflag, 
				barproductflag, 
				barbrandflag,
				dataprocessing_rule_description, 
				dataprocessing_rule_steps,
				audit_loadts
		)
		SELECT	dpr.data_processing_ruleid, 
				dpr.dataprocessing_group,
				dpr.soldtoflag, 
				dpr.barcustflag, 
				dpr.skuflag, 
				dpr.barproductflag, 
				dpr.barbrandflag,
				dpr.dataprocessing_rule_description, 
				dpr.dataprocessing_rule_steps,
				getdate() as audit_loadts
		from 	ref_data.data_processing_rule dpr
	;	
	exception
		when others then raise info 'exception occur while ingesting data in dim_dataprocessing_rule';
END;
$$
;