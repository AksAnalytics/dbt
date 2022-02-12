
CREATE OR REPLACE PROCEDURE ref_data.p_build_reference_agm_bnr_financials_extract()
 LANGUAGE plpgsql
AS $_$
	
BEGIN 
	/*
	 * 
	 * 		call ref_data.p_build_reference_agm_bnr_financials_extract ()
	 * 		select count(*) from ref_data.agm_bnr_financials_extract;
	 * 		grant execute on procedure ref_data.p_build_reference_agm_bnr_financials_extract() to group "g-ada-rsabible-sb-ro";
	 */
	
	DROP TABLE IF EXISTS stg_agm_bnr_financials_extract
	;
	CREATE TEMPORARY TABLE stg_agm_bnr_financials_extract (
		Scenario 				varchar(50) NULL,
		Brand 					varchar(50) NULL,
		Customer 				varchar(50) NULL,
		"Ship-To Geography" 	varchar(50) NULL,
		"Function" 				varchar(50) NULL,
		Entity 					varchar(50) NULL,
		Product 				varchar(50) NULL,
		Years 					varchar(50) NULL,
		Period 					varchar(50) NULL,
		Account 				varchar(50) NULL,
		CurrencyLocalCur 		varchar(50) NULL,
		Reported 				varchar(50) NULL
	) diststyle all
	;
	copy stg_agm_bnr_financials_extract (
		Scenario,
		Brand,
		Customer,
		"Ship-To Geography",
		"Function",
		Entity,
		Product,
		Years,
		Period,
		Account,
		CurrencyLocalCur,
		Reported
	)
	from 's3://sbd-caspian-sandbox-staging/GTS_UMM/bnr_extract/cal_act_umm.txt' 
	iam_role 'arn:aws:iam::555157090578:role/RSABible_Redshift_Role' 
	region 'us-east-1'
	delimiter '|' 
	QUOTE '"'
	CSV
	FILLRECORD
	IGNOREHEADER 1 
	maxerror 1000; 
	DROP TABLE IF EXISTS stg_agm_bnr_financials_extract_append
	;
	CREATE TEMPORARY TABLE stg_agm_bnr_financials_extract_append (
		Scenario 				varchar(100) NULL,
		Brand 					varchar(100) NULL,
		Customer 				varchar(100) NULL,
		"Ship-To Geography" 	varchar(100) NULL,
		"Function" 				varchar(100) NULL,
		Entity 					varchar(100) NULL,
		Product 				varchar(100) NULL,
		Years 					varchar(100) NULL,
		Period 					varchar(100) NULL,
		Account 				varchar(100) NULL,
		CurrencyLocalCur 		varchar(100) NULL,
		Reported 				varchar(100) NULL
	) diststyle all
	;
	/* append 202104 */
	copy stg_agm_bnr_financials_extract_append (
		Scenario,
		Brand,
		Customer,
		"Ship-To Geography",
		"Function",
		Entity,
		Product,
		Years,
		Period,
		Account,
		CurrencyLocalCur,
		Reported
	)
	from 's3://sbd-caspian-sandbox-staging/GTS_UMM/bnr_extract/hyperion_202104.csv' 
	iam_role 'arn:aws:iam::555157090578:role/RSABible_Redshift_Role' 
	region 'us-east-1'
	delimiter ',' 
	QUOTE '"'
	CSV
	FILLRECORD
	IGNOREHEADER 1 
	maxerror 1000; 
	/* append 202105 */
	copy stg_agm_bnr_financials_extract_append (
		Scenario,
		Brand,
		Customer,
		"Ship-To Geography",
		"Function",
		Entity,
		Product,
		Years,
		Period,
		Account,
		CurrencyLocalCur,
		Reported
	)
	from 's3://sbd-caspian-sandbox-staging/GTS_UMM/bnr_extract/hyperion_202105.csv' 
	iam_role 'arn:aws:iam::555157090578:role/RSABible_Redshift_Role' 
	region 'us-east-1'
	delimiter ',' 
	QUOTE '"'
	CSV
	FILLRECORD
	IGNOREHEADER 1 
	maxerror 1000; 
	/* append 202106 */
	copy stg_agm_bnr_financials_extract_append (
		Scenario,
		Brand,
		Customer,
		"Ship-To Geography",
		"Function",
		Entity,
		Product,
		Years,
		Period,
		Account,
		CurrencyLocalCur,
		Reported
	)
	from 's3://sbd-caspian-sandbox-staging/GTS_UMM/bnr_extract/hyperion_202106.csv' 
	iam_role 'arn:aws:iam::555157090578:role/RSABible_Redshift_Role' 
	region 'us-east-1'
	delimiter ',' 
	QUOTE '"'
	CSV
	FILLRECORD
	IGNOREHEADER 1 
	maxerror 1000; 
	/* append 202107 */
	copy stg_agm_bnr_financials_extract_append (
		Scenario,
		Brand,
		Customer,
		"Ship-To Geography",
		"Function",
		Entity,
		Product,
		Years,
		Period,
		Account,
		CurrencyLocalCur,
		Reported
	)
	from 's3://sbd-caspian-sandbox-staging/GTS_UMM/bnr_extract/hyperion_202107.csv' 
	iam_role 'arn:aws:iam::555157090578:role/RSABible_Redshift_Role' 
	region 'us-east-1'
	delimiter '|' 
	QUOTE '"'
	CSV
	FILLRECORD
	IGNOREHEADER 1 
	maxerror 1000; 

	/* append 202108 */
	copy stg_agm_bnr_financials_extract_append (
		Scenario,
		Brand,
		Customer,
		"Ship-To Geography",
		"Function",
		Entity,
		Product,
		Years,
		Period,
		Account,
		CurrencyLocalCur,
		Reported
	)
	from 's3://sbd-caspian-sandbox-staging/GTS_UMM/bnr_extract/hyperion_202108.csv' 
	iam_role 'arn:aws:iam::555157090578:role/RSABible_Redshift_Role' 
	region 'us-east-1'
	delimiter ',' 
	QUOTE '"'
	CSV
	FILLRECORD
	IGNOREHEADER 1
	maxerror 0; 

/* append 202109 */
	copy stg_agm_bnr_financials_extract_append (
		Scenario,
		Brand,
		Customer,
		"Ship-To Geography",
		"Function",
		Entity,
		Product,
		Years,
		Period,
		Account,
		CurrencyLocalCur,
		Reported
	)
	from 's3://sbd-caspian-sandbox-staging/GTS_UMM/bnr_extract/HYPERIONACT202109.txt' 
	iam_role 'arn:aws:iam::555157090578:role/RSABible_Redshift_Role' 
	region 'us-east-1'
	delimiter '|' 
	QUOTE '"'
	CSV
	FILLRECORD
	IGNOREHEADER 1
	maxerror 0; 
/* append 202110 */
	copy stg_agm_bnr_financials_extract_append (
		Scenario,
		Brand,
		Customer,
		"Ship-To Geography",
		"Function",
		Entity,
		Product,
		Years,
		Period,
		Account,
		CurrencyLocalCur,
		Reported
	)
	from 's3://sbd-caspian-sandbox-staging/GTS_UMM/bnr_extract/HYPERION_ACT_202110.txt' 
	iam_role 'arn:aws:iam::555157090578:role/RSABible_Redshift_Role' 
	region 'us-east-1'
	delimiter '|' 
	QUOTE '"'
	CSV
	FILLRECORD
	IGNOREHEADER 1
	maxerror 0; 
/* append 202111 */
	copy stg_agm_bnr_financials_extract_append (
		Scenario,
		Brand,
		Customer,
		"Ship-To Geography",
		"Function",
		Entity,
		Product,
		Years,
		Period,
		Account,
		CurrencyLocalCur,
		Reported
	)
	from 's3://sbd-caspian-sandbox-staging/GTS_UMM/bnr_extract/HYPERION_ACT_202111.txt' 
	iam_role 'arn:aws:iam::555157090578:role/RSABible_Redshift_Role' 
	region 'us-east-1'
	delimiter '|' 
	QUOTE '"'
	CSV
	FILLRECORD
	IGNOREHEADER 1
	maxerror 0; 
/* append 202112 */
	copy stg_agm_bnr_financials_extract_append (
		Scenario,
		Brand,
		Customer,
		"Ship-To Geography",
		"Function",
		Entity,
		Product,
		Years,
		Period,
		Account,
		CurrencyLocalCur,
		Reported
	)
	from 's3://sbd-caspian-sandbox-staging/GTS_UMM/bnr_extract/HYPERION_ACT_202112.txt' 
	iam_role 'arn:aws:iam::555157090578:role/RSABible_Redshift_Role' 
	region 'us-east-1'
	delimiter '|' 
	QUOTE '"'
	CSV
	FILLRECORD
	IGNOREHEADER 1
	maxerror 0; 

/*
select distinct account 
from stg_agm_bnr_financials_extract_append fe 
left join {{ source('bods', 'drm_entity') }} dec2 on fe.entity = dec2."name" and level4 = 'GTS_NA' 
where years = 'FY21'
and period = 'Aug'
and scenario = 'Actual_Ledger'
order by 1;
*/

	/*
		select raw_line, raw_field_value, err_reason, * 
		from stl_load_errors
		order by starttime desc
		
		select 	fiscal_month_id, count(*)
		from 	ref_data.agm_bnr_financials_extract abfe 
		group by fiscal_month_id 
		order by 1 desc
	*/
	delete from  ref_data.agm_bnr_financials_extract;
	insert into ref_data.agm_bnr_financials_extract (
				scenario,
				brand,
				customer,
				shipto_geography,
				func,
				entity,
				product,
				fiscal_month_id,
				account,
				amt_local_cur,
				amt_reported
		)
		select 	Scenario,
				Brand,
				Customer,
				"Ship-To Geography",
				"Function",
				Entity,
				Product,
				fiscal_month_id,
				Account,
				cast(CurrencyLocalCur as numeric(25,9)) as amt_local_cur,
				cast(Reported as numeric(25,9)) as amt_reported
		from 	(
				select	Scenario,
						Brand,
						Customer,
						"Ship-To Geography",
						"Function",
						Entity,
						Product,
						CAST( 
							'20' || RIGHT(Years,2) || 
							CASE period
								WHEN 'Jan' THEN '01'
								WHEN 'Feb' THEN '02'
								WHEN 'Mar' THEN '03'
								WHEN 'Apr' THEN '04'
								WHEN 'May' THEN '05'
								WHEN 'Jun' THEN '06'
								WHEN 'Jul' THEN '07'
								WHEN 'Aug' THEN '08'
								WHEN 'Sep' THEN '09'
								WHEN 'Oct' THEN '10'
								WHEN 'Nov' THEN '11'
								WHEN 'Dec' THEN '12'
							END
							AS INT
						) as  fiscal_month_id,
						Account,
						case 
							when CurrencyLocalCur = '' then null
							when CurrencyLocalCur = '#MI' then null
							else CurrencyLocalCur
						end as CurrencyLocalCur,
						case 
							when Reported = '' then null
							when Reported = '#MI' then null
							else Reported
						end as Reported
				from 	stg_agm_bnr_financials_extract
			) as stg
		where stg.fiscal_month_id < 202104
	;
	/* append 2021-04 -> 2021-06 */
	insert into ref_data.agm_bnr_financials_extract (
				scenario,
				brand,
				customer,
				shipto_geography,
				func,
				entity,
				product,
				fiscal_month_id,
				account,
				amt_local_cur,
				amt_reported
		)
		select 	Scenario,
				Brand,
				Customer,
				"Ship-To Geography",
				"Function",
				Entity,
				Product,
				fiscal_month_id,
				Account,
				cast(CurrencyLocalCur as numeric(25,9)) as amt_local_cur,
				cast(Reported as numeric(25,9)) as amt_reported
		from 	(
				select	Scenario,
						Brand,
						Customer,
						"Ship-To Geography",
						"Function",
						Entity,
						Product,
						CAST( 
							'20' || RIGHT(Years,2) || 
							CASE period
								WHEN 'Jan' THEN '01'
								WHEN 'Feb' THEN '02'
								WHEN 'Mar' THEN '03'
								WHEN 'Apr' THEN '04'
								WHEN 'May' THEN '05'
								WHEN 'Jun' THEN '06'
								WHEN 'Jul' THEN '07'
								WHEN 'Aug' THEN '08'
								WHEN 'Sep' THEN '09'
								WHEN 'Oct' THEN '10'
								WHEN 'Nov' THEN '11'
								WHEN 'Dec' THEN '12'
							END
							AS INT
						) as  fiscal_month_id,
						Account,
						CASE 
							when ltrim(rtrim(replace(CurrencyLocalCur,'$',''))) = '-' then 0.0
							when charindex(')', ltrim(rtrim(CurrencyLocalCur))) > 0 then 
								cast(replace(replace(replace(replace(ltrim(rtrim(CurrencyLocalCur)),',','' ),'(',''),')',''),'$','') as decimal(38,8) ) * -1
							else 
								cast(replace(replace(replace(replace(ltrim(rtrim(CurrencyLocalCur)),',','' ),'(',''),')',''),'$','') as decimal(38,8) )
						END as CurrencyLocalCur,
						CASE 
							when ltrim(rtrim(replace(Reported,'$',''))) = '-' then 0.0
							when charindex(')', ltrim(rtrim(Reported))) > 0 then 
								cast(replace(replace(replace(replace(ltrim(rtrim(Reported)),',','' ),'(',''),')',''),'$','') as decimal(38,8) ) * -1
							else 
								cast(replace(replace(replace(replace(ltrim(rtrim(Reported)),',','' ),'(',''),')',''),'$','') as decimal(38,8) )
						END as Reported
				from 	stg_agm_bnr_financials_extract_append
			) as stg
		where stg.fiscal_month_id < 202107
	;
	insert into ref_data.agm_bnr_financials_extract (
				scenario,
				brand,
				customer,
				shipto_geography,
				func,
				entity,
				product,
				fiscal_month_id,
				account,
				amt_local_cur,
				amt_reported
		)
		select 	Scenario,
				Brand,
				Customer,
				"Ship-To Geography",
				"Function",
				Entity,
				Product,
				fiscal_month_id,
				Account,
				cast(CurrencyLocalCur as numeric(25,9)) as amt_local_cur,
				cast(Reported as numeric(25,9)) as amt_reported
		from 	(
				select	Scenario,
						Brand,
						Customer,
						"Ship-To Geography",
						"Function",
						Entity,
						Product,
						CAST( 
							'20' || RIGHT(Years,2) || 
							CASE period
								WHEN 'Jan' THEN '01'
								WHEN 'Feb' THEN '02'
								WHEN 'Mar' THEN '03'
								WHEN 'Apr' THEN '04'
								WHEN 'May' THEN '05'
								WHEN 'Jun' THEN '06'
								WHEN 'Jul' THEN '07'
								WHEN 'Aug' THEN '08'
								WHEN 'Sep' THEN '09'
								WHEN 'Oct' THEN '10'
								WHEN 'Nov' THEN '11'
								WHEN 'Dec' THEN '12'
							END
							AS INT
						) as  fiscal_month_id,
						Account,
						case 
							when CurrencyLocalCur = '' then null
							when CurrencyLocalCur = '#MI' then null
							else CurrencyLocalCur
						end as CurrencyLocalCur,
						case 
							when replace(Reported,',','') = '' then null
							when replace(Reported,',','') = '#MI' then null
							else replace(Reported,',','')
						end as Reported
				from 	stg_agm_bnr_financials_extract_append
			) as stg
		where stg.fiscal_month_id >= 202107
	;

end
$_$
;

CREATE OR REPLACE PROCEDURE ref_data.p_build_reference_calendar()
 LANGUAGE plpgsql
AS $$
BEGIN 

delete from  ref_data.calendar;
copy ref_data.calendar
from 's3://sbd-caspian-sandbox-staging/GTS_UMM/dim_date/date_table.csv' 
iam_role 'arn:aws:iam::555157090578:role/RSABible_Redshift_Role' 
region 'us-east-1'
delimiter ',' 
IGNOREHEADER 1 
maxerror 1000; 
	
end;
$$
;