CREATE OR REPLACE PROCEDURE edw_stage.bods_hfm_currency_plrate()
	LANGUAGE plpgsql
AS $$
	
	
	
	
	
	
	
begin
drop table if exists edw.edw_stage.bods_hfm_currency_plrate;
create table edw.edw_stage.bods_hfm_currency_plrate as
select distinct year, period, year_period , 
      case when custom1 ='CNY' then 'RMB' 
           when custom1 ='MXP' then 'MXN'
           else custom1
      end as from_curr, 
  custom2 as to_curr , max(amt) amt
from 
(

select distinct year, period,year_period, acct, custom1, custom2, amt from ( 
SELECT distinct year, period,
year||(case when period = 'Jan' then '01' 
when period = 'Feb' then '02'
when period = 'Mar' then '03'
when period = 'Apr' then '04'
when period = 'May' then '05'
when period =  'Jun' then '06'
when period =  'Jul' then '07'
when period =  'Aug' then '08'
when period =  'Sep' then '09'
when period =  'Oct' then '10'
when period =  'Nov' then '11'
when period =  'Dec' then '12'
end) as year_period, acct, custom1, custom2, amt
FROM bods.hfm_vw_hfm_actual_trans_current aa
where bar_acct ='PLRATE' 
) where year = to_char(current_date, 'YYYY')  and year_period < (to_char(current_date, 'YYYY')||to_char(current_date, 'MM')) 

UNION 

select distinct year, period,year_period, acct, custom1, custom2, amt from ( 
SELECT distinct year, period,
year||(case when period = 'Jan' then '01' 
when period = 'Feb' then '02'
when period = 'Mar' then '03'
when period = 'Apr' then '04'
when period = 'May' then '05'
when period =  'Jun' then '06'
when period =  'Jul' then '07'
when period =  'Aug' then '08'
when period =  'Sep' then '09'
when period =  'Oct' then '10'
when period =  'Nov' then '11'
when period =  'Dec' then '12'
end) as year_period, acct, custom1, custom2, amt
FROM bods.hfm_vw_hfm_actual_trans_current aa
where bar_acct ='PLRATE' 
) where year <> to_char(current_date, 'YYYY')  


UNION 


SELECT distinct year, period,
year||(case when period = 'Jan' then '01' 
when period = 'Feb' then '02'
when period = 'Mar' then '03'
when period = 'Apr' then '04'
when period = 'May' then '05'
when period =  'Jun' then '06'
when period =  'Jul' then '07'
when period =  'Aug' then '08'
when period =  'Sep' then '09'
when period =  'Oct' then '10'
when period =  'Nov' then '11'
when period =  'Dec' then '12'
end) as year_period, acct, custom1, custom2, amt
FROM bods.hfm_vw_hfm_forecast_trans_current aa
where bar_acct ='PLRATE' 
) group by year, period, year_period ,  from_curr,  to_curr;
EXCEPTION
       WHEN others THEN
       	  RAISE EXCEPTION 'GOT EXCEPTION:SQLSTATE: % SQLERRM: % ', SQLSTATE, SQLERRM; 
end;






$$
;