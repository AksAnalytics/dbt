
CREATE OR REPLACE PROCEDURE dw.p_build_dim_currency(flag_reload integer)
 LANGUAGE plpgsql
AS $$
Begin
	/* check for full reload */
	if(flag_reload = 1)
	then
		delete from dw.dim_currency;
	end if;
	
	drop table if exists stage_dim_currency;
	create temporary table stage_dim_currency
	diststyle all
	as 
	select 	currency_cd,
			currency_format,
			row_number() over(order by currency_cd) as currency_sort
	from 	(
				select 	distinct 
						bcta.bar_currtype as currency_cd,
						'#,0.00' as currency_format
				from 	stage.bods_core_transaction_agg bcta 
			)
	union all 
	Select 'All Converted to USD' as currency_cd,
		  '#,0.00' as 	currency_format,
		   99 as currency_sort
	;
	
	drop table if exists stage_dim_currency_i;
		   
	create temporary table stage_dim_currency_i
	diststyle all
	as
	select s.currency_cd,
		  s.currency_format,
		  s.currency_sort
	from stage_dim_currency s 
	left join dw.dim_currency t on s.currency_cd = t.currency_cd 
	where t.currency_cd is null; 
	insert into dw.dim_currency (currency_cd ,currency_format, currency_sort)
	select  currency_cd,
		   currency_format, 
		   currency_sort
	from stage_dim_currency_i; 
	update dw.dim_currency 
	set currency_format = s.currency_format, 
		   currency_sort = s.currency_sort
	from stage_dim_currency s 
	where dim_currency.currency_cd = s.currency_cd ;
	

	EXCEPTION
		when others then raise info 'exception occur while ingesting data in dim_currency';
END
$$
;