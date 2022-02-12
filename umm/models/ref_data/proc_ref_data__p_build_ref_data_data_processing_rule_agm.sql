
CREATE OR REPLACE PROCEDURE ref_data.p_build_ref_data_data_processing_rule_agm()
 LANGUAGE plpgsql
AS $$
BEGIN 
	/*
	 * 
	 * 		call ref_data.p_build_ref_data_data_processing_rule_agm ()
	 * 		select count(*) from ref_data.data_processing_rule_agm;
	 * 		grant execute on procedure ref_data.p_build_ref_data_data_processing_rule_agm() to group "g-ada-rsabible-sb-ro";
	 */
	
	DELETE FROM ref_data.data_processing_rule_agm;
	INSERT INTO ref_data.data_processing_rule_agm (
			data_processing_ruleid,
			bar_acct_category,
			dataprocessing_group,
			dataprocessing_rule_description
		) 
		values 
			(100,'Reported Inventory Adjustment','Reported Inventory Adjustment','Reported Inventory Adjustment'),
			(101,'Reported Warranty Cost','Reported Warranty Cost','Reported Warranty Cost'),
			(102,'Reported Duty / Tariffs','Reported Duty / Tariffs','Reported Duty / Tariffs'),
			(103,'Reported Freight','Reported Freight','Reported Freight'),
			(104,'Reported PPV','Reported PPV','Reported PPV'),
			(105,'Reported Labor / OH','Reported Labor / OH','Reported Labor / OH')
	;
end
$$
;