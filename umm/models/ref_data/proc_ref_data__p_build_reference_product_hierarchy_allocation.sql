
CREATE OR REPLACE PROCEDURE ref_data.p_build_reference_product_hierarchy_allocation()
 LANGUAGE plpgsql
AS $$
BEGIN 
	
	drop table if exists stage_product_hierarchy_allocation_fy20;
	CREATE TEMPORARY TABLE stage_product_hierarchy_allocation_fy20(
	MemberType varchar(max),
	Name varchar(200),
	Superior1 varchar(200),
	Superior2 varchar(200),
	Superior3 varchar(200),
	Description varchar(400),
	PLNLevel varchar(20),
	Generation integer,
	C1 varchar(max),
	C2 varchar(max),
	C3 varchar(max),
	C4 varchar(max),
	C5 varchar(max),
	C6 varchar(max),
	C7 varchar(max),
	C8 varchar(max),
	C9 varchar(max),
	C10 varchar(max),
	C11 varchar(max),
	C12 varchar(max),
	C13 varchar(max),
	C14 varchar(max),
	C15 varchar(max),
	C16 varchar(max),
	C17 varchar(max),
	C18 varchar(max),
	C19 varchar(max),
	C20 varchar(max)
	);
	copy stage_product_hierarchy_allocation_fy20 
	from 's3://sbd-caspian-sandbox-staging/GTS_UMM/product_hierarchy_allocation_mapping/fy2020/ProductHierarchyAllocationMappingfy2020.csv' 
	iam_role 'arn:aws:iam::555157090578:role/RSABible_Redshift_Role' 
	region 'us-east-1'
	delimiter ',' 
	IGNOREHEADER 0
	maxerror 1000;
--Select count(1) 
--from stage_product_hierarchy_allocation_fy20;
	drop table if exists stage_product_hierarchy_allocation_fy21;
    CREATE TEMPORARY TABLE stage_product_hierarchy_allocation_fy21(
	MemberType varchar(max),
	Name varchar(200),
	Superior1 varchar(200),
	Superior2 varchar(200),
	Superior3 varchar(200),
	Description varchar(400),
	PLNLevel varchar(20),
	Generation integer,
	C1 varchar(max),
	C2 varchar(max),
	C3 varchar(max),
	C4 varchar(max),
	C5 varchar(max),
	C6 varchar(max),
	C7 varchar(max),
	C8 varchar(max),
	C9 varchar(max)
	);
	copy stage_product_hierarchy_allocation_fy21 
	from 's3://sbd-caspian-sandbox-staging/GTS_UMM/product_hierarchy_allocation_mapping/fy2021/ProductHierarchyAllocationMappingfy2021.csv' 
	iam_role 'arn:aws:iam::555157090578:role/RSABible_Redshift_Role' 
	region 'us-east-1'
	delimiter ',' 
	IGNOREHEADER 0
	maxerror 1000;
truncate TABLE ref_data.product_hierarchy_allocation_mapping;
insert into ref_data.product_hierarchy_allocation_mapping (MemberType,
	Name ,
	Superior1 ,
	Superior2 ,
	Superior3 ,
	Description ,
	PLNLevel ,
	Generation ,
	start_date , 
	end_date)
Select MemberType,
	Name ,
	Superior1 ,
	Superior2 ,
	Superior3 ,
	Description ,
	PLNLevel ,
	Generation ,
	start_date , 
	end_date
From (
	Select distinct MemberType ,
		Name,
		Superior1 ,
		Superior2 ,
		Superior3 ,
		Description,
		PLNLevel ,
		Generation, 
		'2019' as fiscal_year
	from stage_product_hierarchy_allocation_fy20
	union all
	Select distinct MemberType ,
		Name,
		Superior1 ,
		Superior2 ,
		Superior3 ,
		Description,
		PLNLevel ,
		Generation, 
		'2020' as fiscal_year
	from stage_product_hierarchy_allocation_fy20
	union all 
	Select distinct MemberType ,
		Name,
		Superior1 ,
		Superior2 ,
		Superior3 ,
		Description,
		PLNLevel ,
		Generation, 
		'2021' as fiscal_year
	from stage_product_hierarchy_allocation_fy21
 )hr 
 left join (
	SELECT min(fyr_begin_dte) as start_date, max(fyr_end_dte) as end_date, fyr_id 
	FROM ref_data.calendar c 
	where fyr_id in (2019,2020,2021)
	group by fyr_id
   ) dd on hr.fiscal_year = dd.fyr_id;
	
	
end;
$$
;