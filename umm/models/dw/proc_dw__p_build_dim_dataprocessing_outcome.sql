
CREATE OR REPLACE PROCEDURE dw.p_build_dim_dataprocessing_outcome(flag_reload integer)
 LANGUAGE plpgsql
AS $$
--DECLARE variables
-- call dw.p_build_dim_dataprocessing_outcome (2)
-- select * from dw.dim_dataprocessing_outcome
BEGIN  
	
	/* This table does not use identity based surrogate key, so does not need Insert / Update Strategy */
	
	/* check for full reload */
	if(flag_reload = 1)
	then
		delete from dw.dim_dataprocessing_outcome;
	end if;

	
	delete from dw.dim_dataprocessing_outcome;
	insert into dw.dim_dataprocessing_outcome (
				dataprocessing_outcome_key,
				dataprocessing_outcome_id,
				dataprocessing_phase,
				dataprocessing_outcome_desc,
				start_date,
				end_date,
				audit_loadts
		)
		values 
		( 0, 0,'phase 0' ,'Pass through',cast('1900-01-01' as date) , cast('9999-12-31' as date) ,cast(getdate() as timestamp)),  
		( 1, 1,'phase 1' ,'Allocated: SKU',cast('1900-01-01' as date) , cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		( 2, 1,'phase 2' ,'Allocated: One Level Up',cast('1900-01-01' as date) , cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		( 3, 1,'phase 3' ,'Allocated: FOB',cast('1900-01-01' as date) , cast('9999-12-31' as date) ,cast(getdate() as timestamp)),		
		( 4, 1,'phase 4' ,'Allocated: FOB Division',cast('1900-01-01' as date) , cast('9999-12-31' as date) ,cast(getdate() as timestamp)),		
		( 5, 1,'phase 5' ,'Allocated: Royalty',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),		
		( 6, 1,'phase 6' ,'Allocated: RSA',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),	
		( 9, 1,'phase 9' ,'Allocated: Two Levels Down (parent bar_product)',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(11, 1,'phase 11','Allocated: One level up and then 2 level down (parent bar_product)',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(100,2,'phase 100','Unallocated',cast('1900-01-01' as date), cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(101,1,'phase 101','Unallocated: Product_None, Customer_None',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(102,1,'phase 102','Unallocated: Service Customer and Products',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(103,2,'phase 103','Unallocated: ADJ_FOB',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(104,2,'phase 104','Unallocated: ADJ_FOB_NO_CUST',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(105,2,'phase 105','Unallocated: ADJ_RSA',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(20,1,'phase 20','Allocated: ADJ_INV_ADJ',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(27,1,'phase 90','Allocated: ADJ_INV_ADJ_GAP',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(21,1,'phase 21','Allocated: ADJ_WC',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(22,1,'phase 91','Allocated: ADJ_WC_GAP',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(23,1,'phase 24','Allocated: ADJ_PPV',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(24,1,'phase 94','Allocated: ADJ_PPV_GAP',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(25,1,'phase 25','Allocated: ADJ_LABOH_ADJ',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(26,1,'phase 95','Allocated: ADJ_LABOH_ADJ_GAP',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(28,1,'phase 23','Allocated: ADJ_FRGHT',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(29,1,'phase 93','Allocated: ADJ-FRGHT_GAP',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(30,1,'phase 22','Allocated: ADJ_DUTY',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(31,1,'phase 92','Allocated: ADJ_DUTY_GAP',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(32,1,'phase 34','Allocated: ADJ_PPV_UNMATCH',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(33,1,'phase 33','Allocated: ADJ_FRGHT_UNMATCH',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(34,1,'phase 32','Allocated: ADJ_DUTY_UNMATCH',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp));
	
	
	
	
exception
when others then raise info 'exception occur while ingesting data in dim_dataprocessing_outcome';
end;
$$
;