
CREATE OR REPLACE PROCEDURE ref_data.p_build_ref_data_fob_soldto_barcust_map()
 LANGUAGE plpgsql
AS $$
BEGIN 
	
delete from  ref_data.fob_soldto_barcust_mapping;
	
insert into ref_data.fob_soldto_barcust_mapping
values 	('DIR-602B', 'Lowes'),
		('AMAZON', 'Amazon'),
		('DIR-657', 'Amazon'),
		('LOWS', 'Lowes'),
		('DIR-847', 'ICField_NonReg'),
		('DIR-601', 'HomeDepot'),
		('TARGET',  'Target'),
		('DIR-842', 'Hillman'),
		('ACE', 'ACE'),
		('DIR-848', 'ICField_NonReg'),
		('DIR-607', 'Target'),
		('DIR-646', 'ICField_NonReg'),
		('DIR-608', 'Walmart');
				

end
$$
;