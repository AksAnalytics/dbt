
CREATE OR REPLACE PROCEDURE dw.p_build_dim_scenario(flag_reload integer)
 LANGUAGE plpgsql
AS $$
Begin
	
	/* This table does not use identity based surrogate key, so does not need Insert / Update Strategy */
	
	/* check for full reload */
	if(flag_reload = 1)
	then
		delete from dw.dim_scenario;
	end if;

	delete from dw.dim_scenario;
	
	--'Actuals, Budget Forcast'
	insert into dw.dim_scenario (scenario_id ,Scenario)
	values (1,'Actuals');
	
	insert into dw.dim_scenario (scenario_id ,Scenario)
	values (2,'Budgeted');
	
	insert into dw.dim_scenario (scenario_id ,Scenario)
	values (3,'Forecast');
exception
when others then raise info 'exception occur while ingesting data in reference_dims';
END
$$
;