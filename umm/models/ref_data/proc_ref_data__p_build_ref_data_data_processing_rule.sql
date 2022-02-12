
CREATE OR REPLACE PROCEDURE ref_data.p_build_ref_data_data_processing_rule()
 LANGUAGE plpgsql
AS $$
BEGIN 
	raise info 'delete from ref_data.data_processing_rule';
	DELETE FROM ref_data.data_processing_rule;
	raise info 'insert into ref_data.data_processing_rule';
	INSERT INTO ref_data.data_processing_rule
	(
	  soldtoflag,
	  skuflag,
	  barcustflag,
	  barproductflag,
	  barbrandflag,
	  bar_acct,
	  data_source, 
	  data_processing_ruleid,
	  dataprocessing_group,
	  dataprocessing_rule_description,
	  dataprocessing_rule_steps
	)
	values 
	('1','1','1','1','1','','n/a',1,'perfect-data','Pass Through',''),
	('1','1','0','1','1','','n/a',2,'cleansed - data : bar_custno','map soldtocust to bar_custno',''),
	('1','1','1','0','1','','n/a',3,'cleansed - data : bar_product','map sku to bar_product',''),
	('1','1','1','1','0','','n/a',4,'cleansed - data : bar_brand','map sku to bar_brand',''),
	('','','','','','','hfm',5,'cleansed - data : hfm sales & cost acct','',''),
	----generic allocation rules 
	('0','0','0','0','1','','n/a',6,'unknown product - unknown customer','General: Missing All',''),
	('0','0','0','1','1','','n/a',7,'partial product - unknown customer','General: Missing SKU, BAR_CUST',''),
	('0','0','1','0','1','','n/a',8,'partial product - partial product','General: Missing SKU, BAR_PROD, SoldTo',''),
	('0','0','1','1','1','','n/a',9,'partial product - partial customer','General: Missing SKU, SoldTo',''),
	('0','1','0','0','1','','n/a',10,'partial product - unknown customer','sku known, soldtocust, bat_custno, bar_product unknown',''),
	('0','1','0','1','1','','n/a',11,'known product - unknwon customer','General: Missing SoldTo, BAR_CUST',''),
	('0','1','1','0','1','','n/a',12,'partial product - partial customer','sku, bar_custno known, soldtocust, bar_product unknown',''),
	('0','1','1','1','1','','n/a',13,'known product - partial customer','General: Missing SoldTo',''),
	('1','0','0','0','1','','n/a',14,'unknown product - partial customer','soldtocust known, unknown sku, bar_product, bar_custno',''),
	('1','0','0','1','1','','n/a',15,'partial product - partial customer','soldtocust, bar_product known, sku, bar_custno unknown',''),
	('1','0','1','0','1','','n/a',16,'unknown product - known customer','General: Missing SKU & BAR_PROD',''),
	('1','0','1','1','1','','n/a',17,'partial product - known customer','General: Missing SKU',''),
	('1','1','0','0','1','','n/a',18,'partial product - partial customer','soldtocust, sku known, bar_custno, bar_product unknown',''),
	('1','1','0','1','1','','n/a',19,'known product - partial customer','General: Missing BAR_CUST',''),
	('1','1','1','0','1','','n/a',20,'partial product - known customer','soldtocust,bar_custno,sku known, unknown bar_product',''),
	------account exceptions rules
	('','','','','','A40111','n/a',21,'acct exception - fob_invoicesale','Account: FOB Invoice Sales',''),
	('','','','','','A40910','n/a',22,'acct exception - royalty_revenue','Account: Royalties',''),
	('','','','','','A40115','n/a',23,'acct exception - rsa_and_price_adjustments','Account: RSA',''),
	--('','','','','','A43112',24,'acct exception - rebates','',''),
	--('','','','','','A43116',25,'acct exception - coop_advertising','',''),
	('','','','','','A60111','n/a',26,'acct exception - standard_material_cost_fob','Account: FOB Std Cost',''),
	('','','','','','','n/a',27,'cleansing - Product_None / Customer_None','Data: CUST & PROD NONE',''),
	('','','','','','','n/a',28,'cleansing - OTH / PSD_Oth','Data: SERVICE','')
	--('','','','','','A60710',27,'acct exception - cos_freight_outbound','','')
	;

	INSERT INTO ref_data.data_processing_rule
		(
		  data_processing_ruleid,
		  dataprocessing_group,
		  dataprocessing_rule_description
		)
	SELECT 	data_processing_ruleid,
			dataprocessing_group,
			dataprocessing_rule_description
	FROM 	ref_data.data_processing_rule_agm 
	;
	drop table if exists stage_ref_allocation_rule;
	
	
	create temporary table stage_ref_allocation_rule
	diststyle all
	as 
	Select *,
		 case when dataprocessing_group like 'cleansed%' and  soldtoflag in  ('1') and barcustflag = '0' and SKUFlag in ('1','0') and barproductFlag in ('1','0') then '2-' || md5(concat(SoldToFlag,barcustflag)) 
				when dataprocessing_group like 'cleansed%' and skuflag ='1' and barproductflag = '0' and soldtoflag in ('1','0') and barcustflag in ('1','0') then '3-' || md5(concat(skuflag,barproductflag))
				when dataprocessing_group like 'cleansed%' and skuflag = '1' and barbrandflag = '0' and soldtoflag in ('1','0') and barcustflag in ('1','0')  then '4-' || md5(concat(skuflag,barbrandflag))
				when dataprocessing_group like 'cleansed%' and data_source = 'hfm' then  md5(data_source)
			else md5(concat(concat(concat(SoldToFlag,barcustflag),SKUFlag),barproductFlag)) end as  dataprocessing_hash       
	From (
	Select data_processing_ruleid,
		  soldtoflag,
		  skuflag,
		  barcustflag,
		  barproductflag,
		  barbrandflag,
		  bar_acct,
		  data_source,
		  dataprocessing_group,
		  dataprocessing_rule_description,
		  dataprocessing_rule_steps
	from ref_data.data_processing_rule
	) a;	

	update ref_data.data_processing_rule  
	set dataprocessing_hash = s.dataprocessing_hash
	from stage_ref_allocation_rule s 
	where data_processing_rule.data_processing_ruleid = s.data_processing_ruleid;

end
$$
;