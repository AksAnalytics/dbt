
CREATE OR REPLACE PROCEDURE {{ source('ref_data', 'p_build_hfmfxrates') }}()
 LANGUAGE plpgsql
AS $$
BEGIN 
	/*
	 * 
	 * 		call {{ source('ref_data', 'p_build_hfmfxrates') }}()
 * 			select * from {{ source('ref_data', 'hfmfxrates') }};
	 * 
	 */
DELETE FROM {{ source('ref_data', 'hfmfxrates') }};

----this is one time load and needs to be chnaged
INSERT INTO {{ source('ref_data', 'hfmfxrates') }} (fiscal_month_begin_date,bar_year, bar_period, fiscal_month_id,fxrate, from_currtype, to_currtype)
with hfm_rates as 
(
SELECT distinct --id,
			"year" as bar_year, 
			lower("period") as bar_period,
			"amt" as fxrate,
			case when lower(custom1)= 'cny' then 'rmb' else lower(custom1) end as from_currtype,
			lower(custom2) as to_currtype
FROM {{ source('bods', 'hfm_vw_hfm_actual_trans') }} hvhatc 
where custom2 = 'USD'
and "year" >='2018'
and rectype = 'Actual'
and bar_acct = 'PLRATE'
),hfm_rates_current as (
Select fiscal_month_begin_date,fyr_id as bar_year, cr.bar_period,c.fiscal_month_id,fxrate,from_currtype,to_currtype
from hfm_rates cr
inner join (select fyr_id, lower(SUBSTRING(fmth_name,1,3)) as bar_period, 
			   fmth_id as fiscal_month_id,
			   min(cast(fmth_begin_dte as date)) as fiscal_month_begin_date
		  from ref_data.calendar 
		  group by fyr_id, lower(SUBSTRING(fmth_name,1,3)), fmth_id
		  ) c on cast(cr.bar_year as integer) = c.fyr_id 
		and cr.bar_period = c.bar_period
)
select fiscal_month_begin_date,bar_year,bar_period,fiscal_month_id,fxrate,from_currtype,to_currtype
from hfm_rates_current
union 
select c.fiscal_month_begin_date,
	  c.bar_year,
	  c.bar_period,
	  c.fiscal_month_id,
	  cr.fxrate,
	  cr.from_currtype,
	  cr.to_currtype
from (
	select min(cast(fmth_begin_dte as date)) as fiscal_month_begin_date, fyr_id as bar_year, lower(SUBSTRING(fmth_name,1,3)) as bar_period,
	       fmth_id as fiscal_month_id
	from ref_data.calendar c
	where cast(dy_dte as date) >= cast(date_trunc('month', CURRENT_DATE) - INTERVAL '2 month' as date)
	and cast(dy_dte as date) <= cast(date_trunc('month', CURRENT_DATE) + INTERVAL '1 month' as date)
	and not exists (select 1 from hfm_rates_current cr where  cast(cr.bar_year as integer) = c.fyr_id and cr.bar_period = lower(SUBSTRING(fmth_name,1,3))) 
	group by fyr_id , lower(SUBSTRING(fmth_name,1,3)) ,fmth_id
	) c, 
	( select fiscal_month_begin_date,bar_year,bar_period,fiscal_month_id,fxrate,from_currtype,to_currtype
	  from hfm_rates_current
	  where fiscal_month_id in (select max(fiscal_month_id) from hfm_rates_current)
	)cr  
;
--order by 4;
	
end
$$
;