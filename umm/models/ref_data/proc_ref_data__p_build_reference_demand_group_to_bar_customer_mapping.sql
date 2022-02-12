
CREATE OR REPLACE PROCEDURE ref_data.p_build_reference_demand_group_to_bar_customer_mapping()
 LANGUAGE plpgsql
AS $_$
BEGIN 
	/*
	 * 
	 * 		call ref_data.p_build_reference_demand_group_to_bar_customer_mapping ()
	 * 		select count(*) from ref_data.demand_group_to_bar_customer_mapping;
	 * 		grant execute on procedure ref_data.p_build_reference_demand_group_to_bar_customer_mapping() to group "g-ada-rsabible-sb-ro";
	 */
	
	DROP TABLE IF EXISTS tmp_demand_group_to_bar_customer_mapping
	;
	CREATE TEMPORARY TABLE tmp_demand_group_to_bar_customer_mapping (
		DEMAND_GROUP		varchar(30),
		BAR_CUSTOMER		varchar(30)
	) diststyle all
	;
	/*
	 * 
	 * 	PREPROD: 
	 * 		role: arn:aws:iam::882441036262:role/RSABible_Redshift_Role_PP
	 * 		from:  $1 
	 * 
	 * 	SANDBOX: 
	 * 		role: arn:aws:iam::555157090578:role/RSABible_Redshift_Role
	 * 		from: s3://sbd-caspian-sandbox-staging/GTS_UMM/
	 */
	
	copy tmp_demand_group_to_bar_customer_mapping (
		DEMAND_GROUP,
		BAR_CUSTOMER
	)
	from 's3://sbd-caspian-sandbox-staging/GTS_UMM/demand_group_to_bar_customer_mapping.csv' 
	iam_role 'arn:aws:iam::555157090578:role/RSABible_Redshift_Role' 
	region 'us-east-1'
	delimiter ',' 
	QUOTE '"'
	CSV
	IGNOREHEADER 1 
	maxerror 1000; 
	

	delete from  ref_data.demand_group_to_bar_customer_mapping;
	insert into ref_data.demand_group_to_bar_customer_mapping (
				demand_group,
				bar_customer
		)
		select 	distinct 
				DEMAND_GROUP,
				BAR_CUSTOMER
		from 	tmp_demand_group_to_bar_customer_mapping
		where 	coalesce(ltrim(rtrim(demand_group)),'') != ''
			and	coalesce(ltrim(rtrim(bar_customer)),'') != ''
	;
end;
$_$
;

CREATE OR REPLACE PROCEDURE ref_data.p_build_reference_entity_to_plant_to_division_to_ssbu_mapping()
 LANGUAGE plpgsql
AS $$
BEGIN 
	/*
	 * 
	 * 		call ref_data.p_build_reference_entity_to_plant_to_division_to_ssbu_mapping ()
	 * 		select count(*) from ref_data.entity_to_plant_to_division_to_ssbu_mapping;
	 * 		grant execute on procedure ref_data.p_build_reference_entity_to_plant_to_division_to_ssbu_mapping() to group "g-ada-rsabible-sb-ro";
	 */
	
	DROP TABLE IF EXISTS stg_entity_to_plant_to_division_to_ssbu_mapping
	;
	CREATE TEMPORARY TABLE stg_entity_to_plant_to_division_to_ssbu_mapping (
		PlantVarRegPct 		varchar(30) NULL,
		Raw_Product 		varchar(75) NULL,
		Description 		varchar(100) NULL,
		Region 				varchar(30) NULL,
		"UMM Division"		varchar(30) NULL,
		Entity 				varchar(10) NULL,
		"BA&R Super SBU"	varchar(30) NULL,
		January				varchar(30) NULL,
		February            varchar(30) NULL,
		March               varchar(30) NULL,
		April               varchar(30) NULL,
		May                 varchar(30) NULL,
		June                varchar(30) NULL,
		July                varchar(30) NULL,
		August              varchar(30) NULL,
		September           varchar(30) NULL,
		October             varchar(30) NULL,
		November            varchar(30) NULL,
		December            varchar(30) NULL,
		"Full Year"         varchar(30) NULL
	) diststyle all
	;
	copy stg_entity_to_plant_to_division_to_ssbu_mapping (
		PlantVarRegPct, 	
		Raw_Product, 	
		Description, 	
		Region, 			
		"UMM Division",
		Entity, 			
		"BA&R Super SBU",
		January,			
		February,        
		March,           
		April,           
		May,             
		June,            
		July,            
		August,          
		September,       
		October,         
		November,        
		December,        
		"Full Year"     
	)
	from 's3://sbd-caspian-sandbox-staging/GTS_UMM/entity_to_plant_to_division_mapping/entity_to_plant_to_division_to_sbu_mapping.csv' 
	iam_role 'arn:aws:iam::555157090578:role/RSABible_Redshift_Role' 
	region 'us-east-1'
	delimiter ',' 
	QUOTE '"'
	CSV
	IGNOREHEADER 1 
	maxerror 1000; 
	/*
		select raw_line, raw_field_value, err_reason, * 
		from stl_load_errors
		order by starttime desc
	*/
	delete from  ref_data.entity_to_plant_to_division_to_ssbu_mapping;
	insert into ref_data.entity_to_plant_to_division_to_ssbu_mapping (
				plant_var_reg_pct,
				raw_product,
				description,
				region,
				division,
				entity,
				super_sbu,
				jan,
				feb,
				mar,
				apr,
				may,
				jun,
				jul,
				aug,
				sep,
				oct,
				nov,
				dec
		)
		select 	stg.PlantVarRegPct as plant_var_reg_pct, 
				stg.Raw_Product as raw_product, 
				stg.Description as description, 
				stg.Region as region, 
				stg."UMM Division" as division,
				stg.Entity as entity, 
				stg."BA&R Super SBU" as super_sbu,
				cast(REPLACE(case when stg.January = '' then null else stg.January end,'%','') as numeric(6,2))/100.0 as jan, 
				cast(REPLACE(case when stg.February = '' then null else stg.February end,'%','') as numeric(6,2))/100.0 as feb, 
				cast(REPLACE(case when stg.March = '' then null else stg.March end,'%','') as numeric(6,2))/100.0 as mar, 
				cast(REPLACE(case when stg.April = '' then null else stg.April end,'%','') as numeric(6,2))/100.0 as apr, 
				cast(REPLACE(case when stg.May = '' then null else stg.May end,'%','') as numeric(6,2))/100.0 as may, 
				cast(REPLACE(case when stg.June = '' then null else stg.June end,'%','') as numeric(6,2))/100.0 as jun, 
				cast(REPLACE(case when stg.July = '' then null else stg.July end,'%','') as numeric(6,2))/100.0 as jul, 
				cast(REPLACE(case when stg.August = '' then null else stg.August end,'%','') as numeric(6,2))/100.0 as aug, 
				cast(REPLACE(case when stg.September = '' then null else stg.September end,'%','') as numeric(6,2))/100.0 as sep, 
				cast(REPLACE(case when stg.October = '' then null else stg.October end,'%','') as numeric(6,2))/100.0 as oct, 
				cast(REPLACE(case when stg.November = '' then null else stg.November end,'%','') as numeric(6,2))/100.0 as nov, 
				cast(REPLACE(case when stg.December = '' then null else stg.December end,'%','') as numeric(6,2))/100.0 as dec
		from stg_entity_to_plant_to_division_to_ssbu_mapping as stg
	;
end;
$$
;