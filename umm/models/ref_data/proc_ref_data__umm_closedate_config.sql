
CREATE OR REPLACE PROCEDURE ref_data.umm_closedate_config()
 LANGUAGE plpgsql
AS $$
--call dw.p_build_dim_business_unit (1)
BEGIN
	
	--comment
	delete  from ref_data.umm_closedate_config; 

	drop table if exists stage_umm_closedate_config; 
	create temporary table stage_umm_closedate_config
	diststyle all
	as 
	Select row_number() over (order by fmth_id) as rownumber,
		  fmth_id as fiscal_month_id,
		  min(dy_dte) as fiscal_close_date, 
		  max(dy_dte) as fiscal_month_enddate,
		  dateadd(day,7,min(dy_dte)) as fiscal_wklyjob_start_date,
		  dateadd(day,5,min(dy_dte)) as finance_close_date 
	from ref_data.calendar 
	where fmth_id >= 201901
	group by fmth_id;

	insert into ref_data.umm_closedate_config
	select * from stage_umm_closedate_config;
	EXCEPTION
		when others then raise info 'exception occur while ingesting data in ref_data.umm_closedate_config';
END

$$
;