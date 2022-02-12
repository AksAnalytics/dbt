
CREATE OR REPLACE PROCEDURE stage.p_build_stage_currency_exchange_rate()
 LANGUAGE plpgsql
AS $$
BEGIN 
	truncate table stage.currency_exchange_rate;
	insert into stage.currency_exchange_rate (
			    YearMonthID,
				FromCurrencyCode,
				ToCurrencyCode,
				Rate
		)
		select 	DISTINCT 
				(CAST(rt."year" as int) * 100) +
					CASE rt."period"
						WHEN 'Jan' THEN 1
						WHEN 'Feb' THEN 2
						WHEN 'Mar' THEN 3
						WHEN 'Apr' THEN 4
						WHEN 'May' THEN 5
						WHEN 'Jun' THEN 6
						WHEN 'Jul' THEN 7
						WHEN 'Aug' THEN 8
						WHEN 'Sep' THEN 9
						WHEN 'Oct' THEN 10
						WHEN 'Nov' THEN 11
						WHEN 'Dec' THEN 12
					END as YearMonthID,
				rt.custom1 as FromCurrencyCode,
				rt.custom2 as ToCurrencyCode,
				rt.amt 
		from 	{{ source('source_poc', 'hfmfxrates') }} as rt
	;
	
exception
when others then raise info 'exception occur while ingesting data in stage.currency_exchange_rate';
end;
$$
;