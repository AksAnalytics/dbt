CREATE OR REPLACE PROCEDURE stage.deployment_testing()
 LANGUAGE plpgsql
AS $$
Begin
	
insert into stage.deployment_testing ( deployment_date)
 select getdate();

END
$$
;