
CREATE OR REPLACE PROCEDURE ref_data.p_build_reference_customer_commercial_hierarchy()
 LANGUAGE plpgsql
AS $$
BEGIN 

delete from  ref_data.customer_commercial_hierarchy;
copy ref_data.customer_commercial_hierarchy
from 's3://sbd-caspian-sandbox-staging/GTS_UMM/commercial_hierarchy/sbd_mgmt_reporting_structure_draft_20210413.csv' 
iam_role 'arn:aws:iam::555157090578:role/RSABible_Redshift_Role' 
region 'us-east-1'
delimiter ',' 
IGNOREHEADER 1 
maxerror 1000; 
	
end;
$$
;