
CREATE OR REPLACE PROCEDURE ref_data.p_build_reference_product_commercial_hierarchy()
 LANGUAGE plpgsql
AS $$
BEGIN 
--
	
	
	delete from  ref_data.product_commercial_hierarchy;
	
	drop table if exists stg_product_commercial_hierarchy;
	create temporary table stg_product_commercial_hierarchy ( 
	 gpp_portfolio varchar(50) NOT NULL,
    gts varchar(50) NULL,
    super_bu varchar(50) NULL,
    subcategory varchar(50) NULL,
    category varchar(50) NULL
	);
	
	
	copy stg_product_commercial_hierarchy
	from 's3://sbd-caspian-sandbox-staging/GTS_UMM/commercial_hierarchy/product_commercial_hierarchy_20210416.csv' 
	iam_role 'arn:aws:iam::555157090578:role/RSABible_Redshift_Role' 
	region 'us-east-1'
	delimiter ',' 
	IGNOREHEADER 1 
	maxerror 1000; 
	
	---delete dedups..
	delete from stg_product_commercial_hierarchy
	where gts = 'GTS' 
	and super_bu <> 'HTAS'
	and lower(CONCAT('P', gpp_portfolio))='pxxxxx';

	insert into ref_data.product_commercial_hierarchy (
	gpp_portfolio ,
    gts ,
    super_bu,
    subcategory ,
    category 
	)
	select case when length(gpp_portfolio) = 4 then CONCAT('0', gpp_portfolio)
				when length(gpp_portfolio) = 3 then CONCAT('00', gpp_portfolio)
				else gpp_portfolio
		    end as gpp_portfolio, 
		  	gts,
		  	super_bu,
		  	subcategory ,
		  	category 
	from stg_product_commercial_hierarchy;

end;
$$
;