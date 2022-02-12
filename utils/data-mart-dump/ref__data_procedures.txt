
CREATE OR REPLACE PROCEDURE ref_data.p_build_hfmfxrates_current()
 LANGUAGE plpgsql
AS $$
BEGIN 
	/*
	 * 
	 * 		call ref_data.p_build_hfmfxrates_current()
 * 			select * from ref_data.hfmfxrates_current;
	 * 
	 */
DELETE FROM ref_data.hfmfxrates_current;

----this is one time load and needs to be chnaged
INSERT INTO ref_data.hfmfxrates_current (fiscal_month_begin_date,bar_year, bar_period, fiscal_month_id,fxrate, from_currtype, to_currtype)
with hfm_rates as 
(
SELECT distinct --id,
			"year" as bar_year, 
			lower("period") as bar_period,
			"amt" as fxrate,
			case when lower(custom1)= 'cny' then 'rmb' else lower(custom1) end as from_currtype,
			lower(custom2) as to_currtype
FROM bods.hfm_vw_hfm_actual_trans_current hvhatc 
where custom2 = 'USD'
and "year" >='2018'
and rectype = 'Actual'
and bar_acct = 'PLRATE'
),hfm_rates_current as (
Select fiscal_month_begin_date,fyr_id as bar_year, cr.bar_period,c.fiscal_month_id,fxrate,from_currtype,to_currtype
from hfm_rates cr
inner join (select fyr_id, lower(SUBSTRING(fmth_name,1,3)) as bar_period, 
			   fmth_id as fiscal_month_id,
			   min(cast(fmth_begin_dte as date)) as fiscal_month_begin_date
		  from ref_data.calendar 
		  group by fyr_id, lower(SUBSTRING(fmth_name,1,3)), fmth_id
		  ) c on cast(cr.bar_year as integer) = c.fyr_id 
		and cr.bar_period = c.bar_period
)
select fiscal_month_begin_date,bar_year,bar_period,fiscal_month_id,fxrate,from_currtype,to_currtype
from hfm_rates_current
union 
select c.fiscal_month_begin_date,
	  c.bar_year,
	  c.bar_period,
	  c.fiscal_month_id,
	  cr.fxrate,
	  cr.from_currtype,
	  cr.to_currtype
from (
	select min(cast(fmth_begin_dte as date)) as fiscal_month_begin_date, fyr_id as bar_year, lower(SUBSTRING(fmth_name,1,3)) as bar_period,
	       fmth_id as fiscal_month_id
	from ref_data.calendar c
	where cast(dy_dte as date) >= cast(date_trunc('month', CURRENT_DATE) - INTERVAL '2 month' as date)
	and cast(dy_dte as date) <= cast(date_trunc('month', CURRENT_DATE) + INTERVAL '1 month' as date)
	and not exists (select 1 from hfm_rates_current cr where  cast(cr.bar_year as integer) = c.fyr_id and cr.bar_period = lower(SUBSTRING(fmth_name,1,3))) 
	group by fyr_id , lower(SUBSTRING(fmth_name,1,3)) ,fmth_id
	) c, 
	( select fiscal_month_begin_date,bar_year,bar_period,fiscal_month_id,fxrate,from_currtype,to_currtype
	  from hfm_rates_current
	  where fiscal_month_id in (select max(fiscal_month_id) from hfm_rates_current)
	)cr  
;
--order by 4;
	
end
$$
;

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

CREATE OR REPLACE PROCEDURE ref_data.p_build_ref_data_pnl_acct()
 LANGUAGE plpgsql
AS $$
BEGIN 
	
delete from ref_data.pnl_acct;

INSERT INTO ref_data.pnl_acct (bar_acct,bar_acct_desc,acct_type) VALUES
	 ('A40110','sales_invoiced','sales'),
	 ('A40111','fob_invoice_sale','sales'),
	 ('A40115','rsa_and_price_adjustments', 'cost'),
	 ('A40116','sales_freight_income','sales'),
	 ('A40120','destroy_in_field','sales');
INSERT INTO ref_data.pnl_acct (bar_acct,bar_acct_desc,acct_type) VALUES
	 ('A40210','product_sales_export','sales'),
	 ('A40310','rental_sales','sales'),
	 ('A40410','billable_service_revenue', ''),
	 ('A40510','contract_service_revenue',''),
	 ('A40610','install_revenue','sales');
INSERT INTO ref_data.pnl_acct (bar_acct,bar_acct_desc,acct_type) VALUES
	 ('A40710','franchise_revenue', 'sales'),
	 ('A40910','royalty_revenue','sales'),
	 ('A41110','returns_domestic','cost'),
	 ('A41210','returns_export','cost');
INSERT INTO ref_data.pnl_acct (bar_acct,bar_acct_desc,acct_type) VALUES
	 ('A42110','freight_domestic',''),
	 ('A42210','freight_export',''),
	 ('A43110','discounts_allow_domestic','cost'),
	 ('A43111','fillrate_fine','cost'),
	 ('A43112','rebates', 'cost');
INSERT INTO ref_data.pnl_acct (bar_acct,bar_acct_desc,acct_type) VALUES
	 ('A43115','cashdiscount_domestic', 'cost'),
	 ('A43116','coop_advertising', 'cost'),
	 ('A43117','sales_adjustments_other',''),
	 ('A43120','customer_considerations', 'cost'),
	 ('A43130','fob_deductions',''),
	 ('A43210','discounts_allow_export', 'cost'),
	 ('A43215','cash_discount_total', 'cost');
INSERT INTO ref_data.pnl_acct (bar_acct,bar_acct_desc,acct_type) VALUES
	 ('A60110','standard_material_cost_domestic', 'cost'),
	 ('A60111','standard_material_cost_fob', 'cost'),
	 ('A60112','standard_material_cost_serv_install',''),
	 ('A60113','standard_material_cost_serv_install_3p',''),
	 ('A60114','merchandising_cos', 'cost');
INSERT INTO ref_data.pnl_acct (bar_acct,bar_acct_desc,acct_type) VALUES
	 ('A60115','targeted_funds_cos',''),
	 ('A60116','free_goods_cos', 'cost'),
	 ('A60210','standard_material_cost_export', 'cost'),
	 ('A60310','rental_cos',''),
	 ('A60410','cos_service', 'A60410'),
	 ('A60510','cos_monitoring','');
INSERT INTO ref_data.pnl_acct (bar_acct,bar_acct_desc,acct_type) VALUES
	 ('A60610','cos_installations', 'cost'),
	 ('A60612','std_labor_cos_serv_install',''),
	 ('A60710','cos_freight_outbound', 'cost'),
	 ('A61110','cos_trd_domestic_labor', 'cost'),
	 ('A61210','cos_trd_export_labor','');
INSERT INTO ref_data.pnl_acct (bar_acct,bar_acct_desc,acct_type) VALUES
	 ('A62210','std_oh_cos_export',''),
	 ('A62612','std_oh_cos_serv_install', 'cost'),
	 ('A62613','std_oh_cos_serv_install_3p','');
end
$$
;

CREATE OR REPLACE PROCEDURE ref_data.p_build_ref_data_pnl_acct_agm()
 LANGUAGE plpgsql
AS $$
/*
 * 		call ref_data.p_build_ref_data_pnl_acct_agm()
 * 
 */
BEGIN 
	
delete from ref_data.pnl_acct_agm;
INSERT INTO ref_data.pnl_acct_agm (acct_category,bar_acct,bar_acct_desc,acct_type,multiplication_factor) VALUES
	 ('Reported Inventory Adjustment','A84100','Material Scrap and Usag',NULL,-1),
	 ('Reported Inventory Adjustment','A84112','Substitution Varianc',NULL,-1),
	 ('Reported Inventory Adjustment','A82810','Scrap and Waste Recover',NULL,-1),
	 ('Reported Inventory Adjustment','A76130','Obsolescence',NULL,-1),
	 ('Reported Inventory Adjustment','A76140','Merchadising Provision',NULL,-1),
	 ('Reported Inventory Adjustment','A76110','Surplus Defici',NULL,-1),
	 ('Reported Inventory Adjustment','A76210','Book to Physical Provisio',NULL,-1),
	 ('Reported Inventory Adjustment','A76211','Book to Physical Actua',NULL,-1),
	 ('Reported Inventory Adjustment','A76213','Conversions/Rework/Scra',NULL,-1),
	 ('Reported Inventory Adjustment','A76214','Missing / Damaged Good',NULL,-1);
INSERT INTO ref_data.pnl_acct_agm (acct_category,bar_acct,bar_acct_desc,acct_type,multiplication_factor) VALUES
	 ('Reported Warranty Cost','A82499','Warranty Cost',NULL,-1),
	 ('Reported Warranty Cost','A82411','Warranty Exp-No App Defec',NULL,-1),
	 ('Reported Warranty Cost','A82412','Warranty Std Cost-Ret Scra',NULL,-1),
	 ('Reported Warranty Cost','A82413','Warranty Std Cost-Recon Credi',NULL,-1),
	 ('Reported Warranty Cost','A82414','Warranty Part',NULL,-1),
	 ('Reported Warranty Cost','A82415','Warranty Recover',NULL,-1),
	 ('Reported Warranty Cost','A82416','Warranty Freigh',NULL,-1),
	 ('Reported Warranty Cost','A82417','Warranty Recharge',NULL,-1),
	 ('Reported Warranty Cost','A82418','Warranty Repair-Svc Cente',NULL,-1),
	 ('Reported Warranty Cost','A82419','Warranty Recon Cos',NULL,-1);
INSERT INTO ref_data.pnl_acct_agm (acct_category,bar_acct,bar_acct_desc,acct_type,multiplication_factor) VALUES
	 ('Reported Duty / Tariffs','A82125','Duty Absorptio',NULL,-1),
	 ('Reported Duty / Tariffs','A82123','Transport Inbound - Dut',NULL,-1),
	 ('Reported Duty / Tariffs','A82126','S301 Tariff',NULL,-1),
	 ('Reported Freight','A82124','Freight Absorptio',NULL,-1),
	 ('Reported Freight','A82120','Transport Inboun',NULL,-1),
	 ('Reported Freight','A82122','Premium Freigh',NULL,-1),
	 ('Reported Freight','A82130','Interfacility Freigh',NULL,-1),
	 ('Reported Freight','A82121','MRO Paid Freigh',NULL,-1),
	 ('Reported Freight','A82140','Currency Translation In',NULL,-1),
	 ('Reported PPV','A77100','Purch Price Varianc',NULL,-1);
INSERT INTO ref_data.pnl_acct_agm (acct_category,bar_acct,bar_acct_desc,acct_type,multiplication_factor) VALUES
	 ('Reported PPV','A77680','Sourcing Va',NULL,-1),
	 ('Reported PPV','A83740','COS-Servic',NULL,-1),
	 ('Reported PPV','A83510','Cash Disc On Purchase',NULL,-1),
	 ('Reported PPV','A77110','PPV Exchange Syste',NULL,-1),
	 ('Reported PPV','A77115','PPV Exchange Manua',NULL,-1),
	 ('Reported PPV','A83720','PPV Hedge Contract',NULL,-1),
	 ('Reported PPV','A84111','Process Yiel',NULL,-1),
	 ('Reported Labor / OH','A76320','unknown',NULL,-1),
	 ('Reported Labor / OH','A76999','unknown',NULL,-1),
	 ('Reported Labor / OH','A84290','Shop Direct Labor Prod Credi',NULL,1),
	 ('Reported Labor / OH','A84395','Mfg Svc Liquidation Adjustmen',NULL,1),
	 ('Reported Labor / OH','A77900','Mfg Svc Services/Install Abs Offse',NULL,1);
INSERT INTO ref_data.pnl_acct_agm (acct_category,bar_acct,bar_acct_desc,acct_type,multiplication_factor) VALUES
	 ('Reported Labor / OH','A84390','Mfg Svc Manuf Prod Cre',NULL,1),
	 ('Reported Labor / OH','A85950','Mfg Svc Allocation Offse',NULL,-1),
	 ('Reported Labor / OH','A85951','Mfg Svc Plant Var Allocation Offse',NULL,-1),
	 ('Reported Labor / OH','A85952','Mfg Svc Plant Var Allocation Offset-GE',NULL,-1),
	 ('Reported Labor / OH','A85953','OCOS Allocation Offset',NULL,-1),
	 ('Reported Labor / OH','A85800','Corp COGS Alloc I',NULL,-1),
	 ('Reported Labor / OH','A85900','Corp COGS Alloc Ou',NULL,-1),
	 ('Reported Labor / OH','A84210','Shop Direct Labor Payroll Cos',NULL,-1),
	 ('Reported Labor / OH','A85100D','Mfg Svc Hrly Wages Direc',NULL,-1),
	 ('Reported Labor / OH','A85130D','Mfg Svc Hrly Unappl. Direc',NULL,-1);
INSERT INTO ref_data.pnl_acct_agm (acct_category,bar_acct,bar_acct_desc,acct_type,multiplication_factor) VALUES
	 ('Reported Labor / OH','A85110D','Mfg Svc Salaried Wages Direc',NULL,-1),
	 ('Reported Labor / OH','A85139D','Mfg Svc Special Comp Direc',NULL,-1),
	 ('Reported Labor / OH','A85191D','Mfg Svc Temp Labor Direc',NULL,-1),
	 ('Reported Labor / OH','A85100I','Mfg Svc Hrly Wages Indirec',NULL,-1),
	 ('Reported Labor / OH','A85110I','Mfg Svc Salaried Wages Indirec',NULL,-1),
	 ('Reported Labor / OH','A85139I','Mfg Svc Special Comp Indirec',NULL,-1),
	 ('Reported Labor / OH','A85130I','Mfg Svc Hrly Unappl. Indirec',NULL,-1),
	 ('Reported Labor / OH','A85191I','Mfg Svc Temp Labor Indirec',NULL,-1),
	 ('Reported Labor / OH','A85125','Mfg Svc Hrly OT Premiu',NULL,-1),
	 ('Reported Labor / OH','A85120D','Mfg Svc Salaried OT Direc',NULL,-1);
INSERT INTO ref_data.pnl_acct_agm (acct_category,bar_acct,bar_acct_desc,acct_type,multiplication_factor) VALUES
	 ('Reported Labor / OH','A85140D','Mfg Svc Benefits Direc',NULL,-1),
	 ('Reported Labor / OH','A85141D','Mfg Svc Retirement Plan Direc',NULL,-1),
	 ('Reported Labor / OH','A85142D','Mfg Svc Defined Benefit Pension Service Cost Direc',NULL,-1),
	 ('Reported Labor / OH','A85145D','Mfg Svc Medical Direc',NULL,-1),
	 ('Reported Labor / OH','A85146D','Mfg Svc Workers Comp Direc',NULL,-1),
	 ('Reported Labor / OH','A85150D','Mfg Svc Payroll Taxes Direc',NULL,-1),
	 ('Reported Labor / OH','A85159D','Mfg Svc Other Benefits Direc',NULL,-1),
	 ('Reported Labor / OH','A84291','Labor FX Trans Direct Variance',NULL,-1),
	 ('Reported Labor / OH','A85155','Mfg Svc Labor FX Translationa',NULL,-1),
	 ('Reported Labor / OH','A85300','Mfg Svc Supplies & Matl Ot',NULL,-1);
INSERT INTO ref_data.pnl_acct_agm (acct_category,bar_acct,bar_acct_desc,acct_type,multiplication_factor) VALUES
	 ('Reported Labor / OH','A85301','Mfg Svc Plant Supplie',NULL,-1),
	 ('Reported Labor / OH','A85305','Mfg Svc Packaging Ex',NULL,-1),
	 ('Reported Labor / OH','A85314','Mfg Svc Exp Toolin',NULL,-1),
	 ('Reported Labor / OH','A85220','Mfg Svc Repairs & Rearrangemen',NULL,-1),
	 ('Reported Labor / OH','A85200','Mfg Svc Other Utilitie',NULL,-1),
	 ('Reported Labor / OH','A85230','Mfg Svc Hardware and Software Maintenanc',NULL,-1),
	 ('Reported Labor / OH','A85201','Mfg Svc Electricit',NULL,-1),
	 ('Reported Labor / OH','A85202','Mfg Svc Oi',NULL,-1),
	 ('Reported Labor / OH','A85203','Mfg Svc Ga',NULL,-1),
	 ('Reported Labor / OH','A85204','Mfg Svc Wate',NULL,-1);
INSERT INTO ref_data.pnl_acct_agm (acct_category,bar_acct,bar_acct_desc,acct_type,multiplication_factor) VALUES
	 ('Reported Labor / OH','A82399','Service Cost Genera',NULL,-1),
	 ('Reported Labor / OH','A82299','OCOS - Othe',NULL,-1),
	 ('Reported Labor / OH','A82112','Recyclin',NULL,-1),
	 ('Reported Labor / OH','A82117','Product Return Cost',NULL,-1),
	 ('Reported Labor / OH','A82311','Kitting/Labelling Variance',NULL,-1),
	 ('Reported Labor / OH','A82312','Dekitting Variance',NULL,-1),
	 ('Reported Labor / OH','A82313','Rework/Reco',NULL,-1),
	 ('Reported Labor / OH','A82210','Pre-Manufacturing Cost',NULL,-1),
	 ('Reported Labor / OH','A85310','Mfg Svc - Prod Eng Matl/Prototyp',NULL,-1),
	 ('Reported Labor / OH','A85313','Mfg Svc Research Matl & Sv',NULL,-1);
INSERT INTO ref_data.pnl_acct_agm (acct_category,bar_acct,bar_acct_desc,acct_type,multiplication_factor) VALUES
	 ('Reported Labor / OH','A85178','Mfg Svc Waste Remova',NULL,-1),
	 ('Reported Labor / OH','A83660','Other Conversion FX Translationa',NULL,-1),
	 ('Reported Labor / OH','A85100B','Mfg Svc Hrly Wages Bas',NULL,-1),
	 ('Reported Labor / OH','A85110B','Mfg Svc Salaried Wages Bas',NULL,-1),
	 ('Reported Labor / OH','A85120B','Mfg Svc Salaried OT Bas',NULL,-1),
	 ('Reported Labor / OH','A85130B','Mfg Svc Hrly Unappl. Bas',NULL,-1),
	 ('Reported Labor / OH','A85139B','Mfg Svc Special Comp Bas',NULL,-1),
	 ('Reported Labor / OH','A85191B','Mfg Svc Temp Labor Bas',NULL,-1),
	 ('Reported Labor / OH','A85121','Mfg Svc - Sales People Incentive',NULL,-1),
	 ('Reported Labor / OH','A85140B','Mfg Svc Benefits Bas',NULL,-1);
INSERT INTO ref_data.pnl_acct_agm (acct_category,bar_acct,bar_acct_desc,acct_type,multiplication_factor) VALUES
	 ('Reported Labor / OH','A85141B','Mfg Svc Retirement Plan Bas',NULL,-1),
	 ('Reported Labor / OH','A85142B','Mfg Svc Defined Benefit Pension Service Cost Bas',NULL,-1),
	 ('Reported Labor / OH','A85145B','Mfg Svc Medical Bas',NULL,-1),
	 ('Reported Labor / OH','A85146B','Mfg Svc Workers Comp Bas',NULL,-1),
	 ('Reported Labor / OH','A85150B','Mfg Svc Payroll Taxes Bas',NULL,-1),
	 ('Reported Labor / OH','A85159B','Mfg Svc Other Benefits Bas',NULL,-1),
	 ('Reported Labor / OH','A85163','Mfg Svc T&',NULL,-1),
	 ('Reported Labor / OH','A85164','Mfg Svc Rel',NULL,-1),
	 ('Reported Labor / OH','A85170','Mfg Svc Meetings & Con',NULL,-1),
	 ('Reported Labor / OH','A85172','Mfg Svc Education & Trainin',NULL,-1);
INSERT INTO ref_data.pnl_acct_agm (acct_category,bar_acct,bar_acct_desc,acct_type,multiplication_factor) VALUES
	 ('Reported Labor / OH','A85183','Mfg Svc Recruitin',NULL,-1),
	 ('Reported Labor / OH','A85186','Mfg Svc Consulting Fee',NULL,-1),
	 ('Reported Labor / OH','A85192','Mfg Svc Other Svc & Fee',NULL,-1),
	 ('Reported Labor / OH','A85197','Mfg Professional Fees MSP Provider',NULL,-1),
	 ('Reported Labor / OH','A85410','Mfg Svc Depr Ex',NULL,-1),
	 ('Reported Labor / OH','A85415','Mfg Svc PC/CAD Depr Ex',NULL,-1),
	 ('Reported Labor / OH','A85412','Mfg Svc Software Amor',NULL,-1),
	 ('Reported Labor / OH','A85400','Mfg Svc Taxes & Insuranc',NULL,-1),
	 ('Reported Labor / OH','A85405','Mfg Svc Prod Liabl Insur Ex',NULL,-1),
	 ('Reported Labor / OH','A85426','Mfg Svc ST Lease/Rent-Fact Equi',NULL,-1);
INSERT INTO ref_data.pnl_acct_agm (acct_category,bar_acct,bar_acct_desc,acct_type,multiplication_factor) VALUES
	 ('Reported Labor / OH','A85428','Mfg Svc ST Lease/Rent <1y',NULL,-1),
	 ('Reported Labor / OH','A85420','Mfg Svc Ren',NULL,-1),
	 ('Reported Labor / OH','A85445','Mfg Svc Equip Lease >1y',NULL,-1),
	 ('Reported Labor / OH','A83730','ESOP-CO',NULL,-1),
	 ('Reported Labor / OH','A85182','MFG Flee',NULL,-1),
	 ('Reported Labor / OH','A85210','Mfg Svc Voice Com',NULL,-1),
	 ('Reported Labor / OH','A85211','Mfg Svc Data Com',NULL,-1),
	 ('Reported Labor / OH','A82999','Base Costs Othe',NULL,-1),
	 ('Reported Labor / OH','A83110','Amort of Patent',NULL,-1),
	 ('Reported Labor / OH','A83120','Amort of License',NULL,-1);
INSERT INTO ref_data.pnl_acct_agm (acct_category,bar_acct,bar_acct_desc,acct_type,multiplication_factor) VALUES
	 ('Reported Labor / OH','A83999','Other Manufacturing Ex',NULL,-1),
	 ('Reported Labor / OH','A85160','Mfg Svc Office Supplie',NULL,-1),
	 ('Reported Labor / OH','A85181','Mfg Svc Donation',NULL,-1),
	 ('Reported Labor / OH','A85199','Mfg Svc Misc Othe',NULL,-1),
	 ('Reported Labor / OH','A85331','Mfg Svc Promo Matl & Dis',NULL,-1),
	 ('Reported Labor / OH','A85358','Mfg Svc CoO',NULL,-1),
	 ('Reported Labor / OH','A85490','Mfg Svc COS Redist Ex',NULL,-1),
	 ('Reported Labor / OH','A82591','Base OH FX Translationa',NULL,-1);
end
$$
;

CREATE OR REPLACE PROCEDURE ref_data.p_build_ref_data_ptg_accruals_agm(fmthid integer)
 LANGUAGE plpgsql
AS $$
BEGIN 
	
	
delete from ref_data.ptg_accruals where fiscal_month_id = fmthid;
insert into ref_data.ptg_accruals (gl_acct ,amt,amt_usd,currkey,fiscal_month_id,posting_week_enddate,audit_loadts)
Select CAST(acct AS varchar(50)) AS gl_acct, 
	  sum(CAST(amt AS NUMERIC(19,8))) AS amt, 
	  sum(case when hc.from_currtype is null then CAST(amt AS NUMERIC(19,8))
	  		 else cast(fxrate as numeric(19,8))*CAST(amt AS NUMERIC(19,8)) end) as amt_usd, 
	  cast(currkey as varchar(10)) as currkey,
	  fmthid as fiscal_month_id,
	  dd.wk_end_dte as posting_week_enddate,
	  getdate() as audit_loadts
from bods.c11_0ec_pca3_current s
left join ref_data.calendar dd on cast((case when s.postdate = '' then null else postdate end) as date) = cast(dd.dy_dte as date)
left join ref_data.hfmfxrates_current hc on dd.fmth_id = hc.fiscal_month_id and lower(s.currkey) = lower(hc.from_currtype)
where 1=1
and dd.fmth_id = fmthid
and s.costctr in ('1005000000')
and acct in ('0005757004','0005555531')
group by CAST(acct AS varchar(50)) ,cast(currkey as varchar(10)), dd.wk_end_dte;
end
$$
;

CREATE OR REPLACE PROCEDURE ref_data.p_build_ref_data_volume_conv_sku()
 LANGUAGE plpgsql
AS $$
BEGIN 
	
delete from ref_data.volume_conv_sku; 
insert into ref_data.volume_conv_sku
Select 	CAST('08130N-PWR' AS varchar(30)) as SKU, cast(12 as numeric(19,2)) as ConversionRate union all 
Select 	CAST('0502SD-PWR'  AS varchar(30)) as SKU, cast(12 as numeric(19,2)) as ConversionRate union all 
Select 	CAST('DFM12720'  AS varchar(30))   as SKU,	cast(100  as numeric(19,2)) as ConversionRate union all 
Select 	CAST('DFM12722'  AS varchar(30))  as SKU,	cast(100  as numeric(19,2)) as ConversionRate union all 
Select 	CAST('DFM12728'  AS varchar(30))  as SKU, cast(100  as numeric(19,2)) as ConversionRate union all 
Select 	CAST('DFM12726'  AS varchar(30))  as SKU,	cast(100  as numeric(19,2)) as ConversionRate union all 
Select 	CAST('DFM12724' AS varchar(30))  as SKU,	cast(100  as numeric(19,2)) as ConversionRate union all 
Select 	CAST('ECC720-I'  AS varchar(30))  as SKU,	cast(4000  as numeric(19,2)) as ConversionRate;
end
$$
;

CREATE OR REPLACE PROCEDURE ref_data.p_build_ref_entity()
 LANGUAGE plpgsql
AS $$
/*
 * 		call ref_data.p_build_ref_entity();
 * 		select count(*) from ref_data.entity;
 * 		grant execute on procedure ref_data.p_build_ref_entity() to group "g-ada-rsabible-sb-ro";
 * 
 */
BEGIN 
	
	delete from ref_data.entity;
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2050', 'Accessories - Commercial', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0363', 'Puebla', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2487', 'HTF US FOB_Macau', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2094', 'Ft Mill VAS', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0305', 'Mac HQ - Columbus', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0325', 'InnerSpace', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2093', 'Ft. Mill Manufacturing (US)', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2474', 'Kenilworth', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2058', 'Rialto DC', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2700CE', 'Newell US Central Europe Drop Ship', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0339', 'ZAG US', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0597', 'Mech Tools Asia CDN FOB - Macau', 'GTS_NA', 'GTS_CA', 'GTS_CA_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0242', 'MRO - WWPT Canada', 'GTS_NA', 'GTS_CA', 'GTS_CA_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2700CAD', 'Newell US Canadian Drop Ship', 'GTS_NA', 'GTS_CA', 'GTS_CA_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E6201', 'Powers US', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E6202', 'Powers Canada', 'GTS_NA', 'GTS_CA', 'GTS_CA_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2083', 'Power Tools Adjustment Co', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2072', 'Reynosa Consumer Manufacturing', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2013PTA', 'North Jackson Manufacturing PTA', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0353', 'NA Verticals PlantVar', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0237', 'Stanley Israel Migdal Haemek', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2098', 'MTD Tupelo MFG', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2051', 'North America Distribution', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0166', 'Elco Base', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2746', 'East Longmeadow', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0306', 'Mechanic NHT', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0308', 'Mac Canada', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2057', 'Fort Mill DC', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0595', 'Mech Tools Asia CDN FOB - Chiro', 'GTS_NA', 'GTS_CA', 'GTS_CA_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2107', 'Hermosillo Mfg Plant (Mexico Legal)', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0093', 'Fastening Clinton Plant', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2091', 'NA Power Tools', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2018', 'North America Professional - Adjustment', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2700', 'Irwin Industrial Tool Company', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2743', 'Newell Bandsaw Canada', 'GTS_NA', 'GTS_CA', 'GTS_CA_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0091', 'Fastening East Greenwich', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'GTS_US_Adj', 'GTS US Adjust', 'GTS_NA', 'GTS_US', 'GTS_US_Adj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2096', 'Monterrey Mfg Plant (Mexico Legal)', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2476', 'Stanley Israel Ramat Gavriel', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0521', 'Lista - Holliston', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2478', 'Sedalia DC', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'GTS_US_MFG_OFFSET', 'GTS US Manufacturing Offset', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2020', 'Hampstead', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0023', 'Louisville', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'GTS_CA_Alloc', 'GTS Canada Allocation', 'GTS_NA', 'GTS_CA', 'GTS_CA_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0160', 'Fastening Divisional HQ', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0304', 'Tools US Div Management', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0545', 'Tools US Assortment Packing', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0120', 'Virax US', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2468', 'Kannapolis', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2031', 'Reynosa Professional Manufacturing', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0244', 'WWPT Canada MRO Transfer', 'GTS_NA', 'GTS_CA', 'GTS_CA_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2700MX', 'Newell US Mexico Drop Ship', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2108', 'Hermosillo Mfg Plant (US Legal)', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0598', 'ZAG Israel CDN FOB', 'GTS_NA', 'GTS_CA', 'GTS_CA_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2035', 'BDK US Inc.', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0121', 'Wendeng', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'GTS_NA_Vert_Adj', 'GTS NA Verticals Adj - DO NOT USE', 'GTS_NA', 'GTS_US', 'GTS_NA_Vert_Adj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'GTS_US_Alloc', 'GTS US Allocation', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'GTS_NA_Vert_Alloc', 'GTS NA Verticals Alloc - DO NOT USE', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2036', 'BDC', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2477', 'Right Co II LLC GTS', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0320', 'Proto US', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0241', 'MRO - WWPT US', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0243', 'WWPT US MRO Transfer', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2071', 'GPA - North America Power Tools', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0162', 'Stanley Bostitch Inc', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0006', 'Fastening Pass Thru Company', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2491', 'HTF US FOB_ GH', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2017', 'Charlotte Packaging', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0276', 'Abmast US', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2060', 'Craftsman USD Entity', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2473', 'Waterloo Holdings Inc.', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0314', 'Mac Georgetown', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0522', 'Lista - Burlington', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0523', 'Lista - Ontario Limited', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0326', 'Vidmar', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0626', 'Vidmar US - export sales', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2702', 'Newell US HQ', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2710', 'Newell Bandsaw US', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2700LAG', 'Newell US LAG Drop Ship', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2700AP', 'Newell US APAC Drop Ship', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2095', 'MTD Nogales', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2013', 'North Jackson Manufacturing PTG', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2097', 'Monterrey Mfg Plant (US Legal)', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2472', 'Sedalia', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2032', 'Reynosa Prof Mfg (Mexico Legal)', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0238', 'Stanley Israel Carmiel', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0337', 'The Stanley Works Israel Ltd.', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0379', 'Mechanic Dallas', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2023', 'Mission Manufacturing', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0547', 'Cheraw', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0551', 'New Britain Tape Plant', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0371', 'Mech Xiaolan WFOE (China)', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2006', 'Shelbyville Accessories', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0098', 'Greenfield', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2744', 'Gorham', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2007', 'Tampa Accessories', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2021', 'South Jackson Manufacturing', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2061', 'Dallas DC', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2059', 'North Jackson DC', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2475', 'IDL Licensing', 'GTS_NA', 'GTS_US', 'GTS_US_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'GTS_CA_Adj', 'GTS Canada Adjust', 'GTS_NA', 'GTS_CA', 'GTS_CA_Adj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2111', 'Power Tools Canada', 'GTS_NA', 'GTS_CA', 'GTS_CA_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2110', 'Canadian Adjustment', 'GTS_NA', 'GTS_CA', 'GTS_CA_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0321', 'Mechanic Canada Adj', 'GTS_NA', 'GTS_CA', 'GTS_CA_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0231', 'Tools Canada', 'GTS_NA', 'GTS_CA', 'GTS_CA_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0366', 'Tools Canada Adjustment Co', 'GTS_NA', 'GTS_CA', 'GTS_CA_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0234', 'Mechanics Consumer Canada', 'GTS_NA', 'GTS_CA', 'GTS_CA_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0170', 'Fastening Canada', 'GTS_NA', 'GTS_CA', 'GTS_CA_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2703', 'Newell CA HQ', 'GTS_NA', 'GTS_CA', 'GTS_CA_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2720', 'Newell Industries Canada Inc. A', 'GTS_NA', 'GTS_CA', 'GTS_CA_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2721', 'Newell Industries Canada Inc. B', 'GTS_NA', 'GTS_CA', 'GTS_CA_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0245', 'St. Hyacinthe', 'GTS_NA', 'GTS_CA', 'GTS_CA_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2488', 'GH Canada', 'GTS_NA', 'GTS_CA', 'GTS_CA_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E2460', 'GPA Canada', 'GTS_NA', 'GTS_CA', 'GTS_CA_WOAdj' );
	INSERT INTO ref_data.entity ( name, description, level4, level5, level6 ) VALUES ( 'E0596', 'Mech Tools Asia CDN FOB - Global Holdings', 'GTS_NA', 'GTS_CA', 'GTS_CA_WOAdj' );

end
$$
;

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
left join bods.drm_entity_current dec2 on fe.entity = dec2."name" and level4 = 'GTS_NA' 
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

CREATE OR REPLACE PROCEDURE ref_data.p_build_reference_parent_product_hierarchy_allocation()
 LANGUAGE plpgsql
AS $$
BEGIN 
    
    /*
        call ref_data.p_build_reference_parent_product_hierarchy_allocation();
        select * from ref_data.parent_product_hierarchy_allocation_mapping limit 10;
        select count(*) from ref_data.parent_product_hierarchy_allocation_mapping;
     */
	DELETE FROM ref_data.parent_product_hierarchy_allocation_mapping;
	
INSERT INTO ref_data.parent_product_hierarchy_allocation_mapping
(member_type,"name",superior1,superior2,superior3,start_date,end_date) 
VALUES
--	 ('Parent','CONSTR_HAND_TOOLS','CONSTR_HT','HTAS_SBU','HTAS','2018-12-30','2019-12-28'),
--	 ('Parent','CONSTR_HAND_TOOLS','CONSTR_HT','HTAS_SBU','HTAS','2019-12-29','2021-01-02'),
--	 ('Parent','CONSTR_HAND_TOOLS','CONSTR_HT','HTAS_SBU','HTAS','2021-01-03','2022-01-01'),
--	 ('Parent','TRADESMAN','CPT','PTE','PTG','2018-12-30','2019-12-28'),
--	 ('Parent','TRADESMAN','CPT','PTE','PTG','2019-12-29','2021-01-02'),
--	 ('Parent','TRADESMAN','CPT','PTE','PTG','2021-01-03','2022-01-01');
('Parent','ACCY_HAND_POWER','ANF_DIV','ANF','PTE','2018-12-30','2019-12-28'),
('Parent','ACCY_HAND_POWER','ANF_DIV','ANF','PTE','2019-12-29','2021-01-02'),
('Parent','ACCY_HAND_POWER','ANF_DIV','ANF','PTE','2021-01-03','2022-01-01'),
('Parent','AE_OTH','AUTO_ELEC','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','AE_OTH','AUTO_ELEC','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','AE_OTH','AUTO_ELEC','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','AE_RECON','AUTO_ELEC','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','AE_RECON','AUTO_ELEC','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','AE_RECON','AUTO_ELEC','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','ANCH_FLEX_TECH','ANF_DIV','ANF','PTE','2018-12-30','2019-12-28'),
('Parent','ANCH_FLEX_TECH','ANF_DIV','ANF','PTE','2019-12-29','2021-01-02'),
('Parent','ANCH_FLEX_TECH','ANF_DIV','ANF','PTE','2021-01-03','2022-01-01'),
('Parent','ANF','PTE','PTG','GTS_Product','2018-12-30','2019-12-28'),
('Parent','ANF','PTE','PTG','GTS_Product','2019-12-29','2021-01-02'),
('Parent','ANF','PTE','PTG','GTS_Product','2021-01-03','2022-01-01'),
('Parent','ANF_DIV','ANF','PTE','PTG','2018-12-30','2019-12-28'),
('Parent','ANF_DIV','ANF','PTE','PTG','2019-12-29','2021-01-02'),
('Parent','ANF_DIV','ANF','PTE','PTG','2021-01-03','2022-01-01'),
('Parent','AUTO_ELEC','CPT','PTE','PTG','2018-12-30','2019-12-28'),
('Parent','AUTO_ELEC','CPT','PTE','PTG','2019-12-29','2021-01-02'),
('Parent','AUTO_ELEC','CPT','PTE','PTG','2021-01-03','2022-01-01'),
('Parent','AUTOPT','PRODUCT_DoNotUse','GTS_Product','Total_Product','2018-12-30','2019-12-28'),
('Parent','AUTOPT','PRODUCT_DoNotUse','GTS_Product','Total_Product','2019-12-29','2021-01-02'),
('Parent','AUTOPT','PRODUCT_DoNotUse','GTS_Product','Total_Product','2021-01-03','2022-01-01'),
('Parent','AUTOPT_AIRTOOLS','DFES_AUTOPT_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','AUTOPT_AIRTOOLS','DFES_AUTOPT_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','AUTOPT_AIRTOOLS','DFES_AUTOPT_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','AUTOPT_CORDED_PT','DFES_AUTOPT_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','AUTOPT_CORDED_PT','DFES_AUTOPT_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','AUTOPT_CORDED_PT','DFES_AUTOPT_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','AUTOPT_CORDLESS_PT','DFES_AUTOPT_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','AUTOPT_CORDLESS_PT','DFES_AUTOPT_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','AUTOPT_CORDLESS_PT','DFES_AUTOPT_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','AUTOPT_DIV','PPT','PTE','PTG','2018-12-30','2019-12-28'),
('Parent','AUTOPT_DIV','PPT','PTE','PTG','2019-12-29','2021-01-02'),
('Parent','AUTOPT_DIV','PPT','PTE','PTG','2021-01-03','2022-01-01'),
('Parent','AUTOPT_PTA','DFES_AUTOPT_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','AUTOPT_PTA','DFES_AUTOPT_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','AUTOPT_PTA','DFES_AUTOPT_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','AUTOPT_TOOLS','DFES_AUTOPT_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','AUTOPT_TOOLS','DFES_AUTOPT_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','AUTOPT_TOOLS','DFES_AUTOPT_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','Bandsaw_CATEGORY','PTA_IND','IND','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','Bandsaw_CATEGORY','PTA_IND','IND','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','Bandsaw_CATEGORY','PTA_IND','IND','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','BENCH_STATIONARY','WOOD_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','BENCH_STATIONARY','WOOD_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','BENCH_STATIONARY','WOOD_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','CDL_DRYWALL','METAL_CONCRETE_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','CDL_DRYWALL','METAL_CONCRETE_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','CDL_DRYWALL','METAL_CONCRETE_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','CHEM_FAST','ANF_DIV','ANF','PTE','2018-12-30','2019-12-28'),
('Parent','CHEM_FAST','ANF_DIV','ANF','PTE','2019-12-29','2021-01-02'),
('Parent','CHEM_FAST','ANF_DIV','ANF','PTE','2021-01-03','2022-01-01'),
('Parent','CONC_OTH','METAL_CONCRETE_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','CONC_OTH','METAL_CONCRETE_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','CONC_OTH','METAL_CONCRETE_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','CONS_TOOLS','CPT','PTE','PTG','2018-12-30','2019-12-28'),
('Parent','CONS_TOOLS','CPT','PTE','PTG','2019-12-29','2021-01-02'),
('Parent','CONS_TOOLS','CPT','PTE','PTG','2021-01-03','2022-01-01'),
('Parent','CONSTR','PRODUCT_DoNotUse','GTS_Product','Total_Product','2018-12-30','2019-12-28'),
('Parent','CONSTR','PRODUCT_DoNotUse','GTS_Product','Total_Product','2019-12-29','2021-01-02'),
('Parent','CONSTR','PRODUCT_DoNotUse','GTS_Product','Total_Product','2021-01-03','2022-01-01'),
('Parent','CONSTR_BUILDING','CONSTR_HAND_TOOLS','CONSTR_HT','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','CONSTR_BUILDING','CONSTR_HAND_TOOLS','CONSTR_HT','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','CONSTR_BUILDING','CONSTR_HAND_TOOLS','CONSTR_HT','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','CONSTR_CUTTING','CONSTR_HAND_TOOLS','CONSTR_HT','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','CONSTR_CUTTING','CONSTR_HAND_TOOLS','CONSTR_HT','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','CONSTR_CUTTING','CONSTR_HAND_TOOLS','CONSTR_HT','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','CONSTR_ELEC_TOOLS','CONSTR_HAND_TOOLS','CONSTR_HT','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','CONSTR_ELEC_TOOLS','CONSTR_HAND_TOOLS','CONSTR_HT','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','CONSTR_ELEC_TOOLS','CONSTR_HAND_TOOLS','CONSTR_HT','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','CONSTR_HAND_TOOLS','CONSTR_HT','HTAS_SBU','HTAS','2018-12-30','2019-12-28'),
('Parent','CONSTR_HAND_TOOLS','CONSTR_HT','HTAS_SBU','HTAS','2019-12-29','2021-01-02'),
('Parent','CONSTR_HAND_TOOLS','CONSTR_HT','HTAS_SBU','HTAS','2021-01-03','2022-01-01'),
('Parent','CONSTR_HT','HTAS_SBU','HTAS','GTS_Product','2018-12-30','2019-12-28'),
('Parent','CONSTR_HT','HTAS_SBU','HTAS','GTS_Product','2019-12-29','2021-01-02'),
('Parent','CONSTR_HT','HTAS_SBU','HTAS','GTS_Product','2021-01-03','2022-01-01'),
('Parent','CONSTR_MEAS_LAYOUT','CONSTR_HAND_TOOLS','CONSTR_HT','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','CONSTR_MEAS_LAYOUT','CONSTR_HAND_TOOLS','CONSTR_HT','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','CONSTR_MEAS_LAYOUT','CONSTR_HAND_TOOLS','CONSTR_HT','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','CONSTR_METAL_STR','IND_AUTO_TOOLS','HTAS_SBU','HTAS','2018-12-30','2019-12-28'),
('Parent','CONSTR_METAL_STR','IND_AUTO_TOOLS','HTAS_SBU','HTAS','2019-12-29','2021-01-02'),
('Parent','CONSTR_METAL_STR','IND_AUTO_TOOLS','HTAS_SBU','HTAS','2021-01-03','2022-01-01'),
('Parent','CONSTR_STORAGE','PRODUCT_DoNotUse','GTS_Product','Total_Product','2018-12-30','2019-12-28'),
('Parent','CONSTR_STORAGE','PRODUCT_DoNotUse','GTS_Product','Total_Product','2019-12-29','2021-01-02'),
('Parent','CONSTR_STORAGE','PRODUCT_DoNotUse','GTS_Product','Total_Product','2021-01-03','2022-01-01'),
('Parent','CONSTR_TOOLBOXES','CONSTR_HT','HTAS_SBU','HTAS','2018-12-30','2019-12-28'),
('Parent','CONSTR_TOOLBOXES','CONSTR_HT','HTAS_SBU','HTAS','2019-12-29','2021-01-02'),
('Parent','CONSTR_TOOLBOXES','CONSTR_HT','HTAS_SBU','HTAS','2021-01-03','2022-01-01'),
('Parent','CONSTRUCTION_SAWS','WOOD_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','CONSTRUCTION_SAWS','WOOD_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','CONSTRUCTION_SAWS','WOOD_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','CORD_OTH','DFES_AUTOPT_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','CORD_OTH','DFES_AUTOPT_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','CORD_OTH','DFES_AUTOPT_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','CORD_RECON','DFES_AUTOPT_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','CORD_RECON','DFES_AUTOPT_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','CORD_RECON','DFES_AUTOPT_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','CORDED_EXP','DFES_AUTOPT_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','CORDED_EXP','DFES_AUTOPT_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','CORDED_EXP','DFES_AUTOPT_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','CPT','PTE','PTG','GTS_Product','2018-12-30','2019-12-28'),
('Parent','CPT','PTE','PTG','GTS_Product','2019-12-29','2021-01-02'),
('Parent','CPT','PTE','PTG','GTS_Product','2021-01-03','2022-01-01'),
('Parent','CRD_DRYWALL','METAL_CONCRETE_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','CRD_DRYWALL','METAL_CONCRETE_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','CRD_DRYWALL','METAL_CONCRETE_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','CRD_GRINDING','METAL_CONCRETE_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','CRD_GRINDING','METAL_CONCRETE_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','CRD_GRINDING','METAL_CONCRETE_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','CRD_METALWORKING','METAL_CONCRETE_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','CRD_METALWORKING','METAL_CONCRETE_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','CRD_METALWORKING','METAL_CONCRETE_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','CT_AE_ENERGY','CONS_TOOLS','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','CT_AE_ENERGY','CONS_TOOLS','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','CT_AE_ENERGY','CONS_TOOLS','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','CT_BATT','CONS_TOOLS','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','CT_BATT','CONS_TOOLS','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','CT_BATT','CONS_TOOLS','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','CT_BNCHTOP','CONS_TOOLS','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','CT_BNCHTOP','CONS_TOOLS','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','CT_BNCHTOP','CONS_TOOLS','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','CT_CORDED_COMBO','CONS_TOOLS','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','CT_CORDED_COMBO','CONS_TOOLS','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','CT_CORDED_COMBO','CONS_TOOLS','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','CT_CORDED_DRILLS','CONS_TOOLS','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','CT_CORDED_DRILLS','CONS_TOOLS','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','CT_CORDED_DRILLS','CONS_TOOLS','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','CT_CORDED_GRINDER','CONS_TOOLS','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','CT_CORDED_GRINDER','CONS_TOOLS','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','CT_CORDED_GRINDER','CONS_TOOLS','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','CT_CORDED_OTH','CONS_TOOLS','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','CT_CORDED_OTH','CONS_TOOLS','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','CT_CORDED_OTH','CONS_TOOLS','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','CT_CORDED_SAND','CONS_TOOLS','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','CT_CORDED_SAND','CONS_TOOLS','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','CT_CORDED_SAND','CONS_TOOLS','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','CT_CORDED_SAWS','CONS_TOOLS','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','CT_CORDED_SAWS','CONS_TOOLS','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','CT_CORDED_SAWS','CONS_TOOLS','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','CT_CORDLESS_COMBO','CONS_TOOLS','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','CT_CORDLESS_COMBO','CONS_TOOLS','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','CT_CORDLESS_COMBO','CONS_TOOLS','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','CT_CORDLESS_DRILLS','CONS_TOOLS','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','CT_CORDLESS_DRILLS','CONS_TOOLS','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','CT_CORDLESS_DRILLS','CONS_TOOLS','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','CT_CORDLESS_EXPAN','CONS_TOOLS','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','CT_CORDLESS_EXPAN','CONS_TOOLS','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','CT_CORDLESS_EXPAN','CONS_TOOLS','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','CT_CORDLESS_OTH','CONS_TOOLS','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','CT_CORDLESS_OTH','CONS_TOOLS','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','CT_CORDLESS_OTH','CONS_TOOLS','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','CT_CORDLESS_SAND','CONS_TOOLS','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','CT_CORDLESS_SAND','CONS_TOOLS','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','CT_CORDLESS_SAND','CONS_TOOLS','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','CT_CORDLESS_SAWS','CONS_TOOLS','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','CT_CORDLESS_SAWS','CONS_TOOLS','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','CT_CORDLESS_SAWS','CONS_TOOLS','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','CT_CORDLESS_SCREW','CONS_TOOLS','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','CT_CORDLESS_SCREW','CONS_TOOLS','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','CT_CORDLESS_SCREW','CONS_TOOLS','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','CT_MISC','CONS_TOOLS','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','CT_MISC','CONS_TOOLS','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','CT_MISC','CONS_TOOLS','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','CT_MULTI_ACC','CONS_TOOLS','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','CT_MULTI_ACC','CONS_TOOLS','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','CT_MULTI_ACC','CONS_TOOLS','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','CT_OSC','CONS_TOOLS','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','CT_OSC','CONS_TOOLS','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','CT_OSC','CONS_TOOLS','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','CT_OTH','CONS_TOOLS','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','CT_OTH','CONS_TOOLS','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','CT_OTH','CONS_TOOLS','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','CT_PROJECT_KIT','CONS_TOOLS','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','CT_PROJECT_KIT','CONS_TOOLS','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','CT_PROJECT_KIT','CONS_TOOLS','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','CT_RECON','CONS_TOOLS','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','CT_RECON','CONS_TOOLS','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','CT_RECON','CONS_TOOLS','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','CT_VPX','CONS_TOOLS','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','CT_VPX','CONS_TOOLS','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','CT_VPX','CONS_TOOLS','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','CTB_MISC_STORAGE','CONSTR_TOOLBOXES','CONSTR_HT','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','CTB_MISC_STORAGE','CONSTR_TOOLBOXES','CONSTR_HT','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','CTB_MISC_STORAGE','CONSTR_TOOLBOXES','CONSTR_HT','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','CTB_PORT_STORAGE','CONSTR_TOOLBOXES','CONSTR_HT','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','CTB_PORT_STORAGE','CONSTR_TOOLBOXES','CONSTR_HT','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','CTB_PORT_STORAGE','CONSTR_TOOLBOXES','CONSTR_HT','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','CTB_RECON','CONSTR_TOOLBOXES','CONSTR_HT','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','CTB_RECON','CONSTR_TOOLBOXES','CONSTR_HT','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','CTB_RECON','CONSTR_TOOLBOXES','CONSTR_HT','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','CTB_SOFT_STORAGE','CONSTR_TOOLBOXES','CONSTR_HT','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','CTB_SOFT_STORAGE','CONSTR_TOOLBOXES','CONSTR_HT','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','CTB_SOFT_STORAGE','CONSTR_TOOLBOXES','CONSTR_HT','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','CTB_SYST_STORAGE','CONSTR_TOOLBOXES','CONSTR_HT','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','CTB_SYST_STORAGE','CONSTR_TOOLBOXES','CONSTR_HT','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','CTB_SYST_STORAGE','CONSTR_TOOLBOXES','CONSTR_HT','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','CTB_TOOLBOX','CONSTR_TOOLBOXES','CONSTR_HT','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','CTB_TOOLBOX','CONSTR_TOOLBOXES','CONSTR_HT','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','CTB_TOOLBOX','CONSTR_TOOLBOXES','CONSTR_HT','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','CTB_WORK_FACIL','CONSTR_TOOLBOXES','CONSTR_HT','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','CTB_WORK_FACIL','CONSTR_TOOLBOXES','CONSTR_HT','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','CTB_WORK_FACIL','CONSTR_TOOLBOXES','CONSTR_HT','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','DFES_AUTOPT_DIV','PPT','PTE','PTG','2018-12-30','2019-12-28'),
('Parent','DFES_AUTOPT_DIV','PPT','PTE','PTG','2019-12-29','2021-01-02'),
('Parent','DFES_AUTOPT_DIV','PPT','PTE','PTG','2021-01-03','2022-01-01'),
('Parent','DRILLING_FASTENING','DFES_AUTOPT_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','DRILLING_FASTENING','DFES_AUTOPT_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','DRILLING_FASTENING','DFES_AUTOPT_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','FAS','PTE','PTG','GTS_Product','2018-12-30','2019-12-28'),
('Parent','FAS','PTE','PTG','GTS_Product','2019-12-29','2021-01-02'),
('Parent','FAS','PTE','PTG','GTS_Product','2021-01-03','2022-01-01'),
('Parent','FAST_COMP','FAST_DIV','FAS','PTE','2018-12-30','2019-12-28'),
('Parent','FAST_COMP','FAST_DIV','FAS','PTE','2019-12-29','2021-01-02'),
('Parent','FAST_COMP','FAST_DIV','FAS','PTE','2021-01-03','2022-01-01'),
('Parent','FAST_CORDLESS_NS','FAST_DIV','FAS','PTE','2018-12-30','2019-12-28'),
('Parent','FAST_CORDLESS_NS','FAST_DIV','FAS','PTE','2019-12-29','2021-01-02'),
('Parent','FAST_CORDLESS_NS','FAST_DIV','FAS','PTE','2021-01-03','2022-01-01'),
('Parent','FAST_DIV','FAS','PTE','PTG','2018-12-30','2019-12-28'),
('Parent','FAST_DIV','FAS','PTE','PTG','2019-12-29','2021-01-02'),
('Parent','FAST_DIV','FAS','PTE','PTG','2021-01-03','2022-01-01'),
('Parent','FAST_FAST','FAST_DIV','FAS','PTE','2018-12-30','2019-12-28'),
('Parent','FAST_FAST','FAST_DIV','FAS','PTE','2019-12-29','2021-01-02'),
('Parent','FAST_FAST','FAST_DIV','FAS','PTE','2021-01-03','2022-01-01'),
('Parent','FAST_IND','FAST_DIV','FAS','PTE','2018-12-30','2019-12-28'),
('Parent','FAST_IND','FAST_DIV','FAS','PTE','2019-12-29','2021-01-02'),
('Parent','FAST_IND','FAST_DIV','FAS','PTE','2021-01-03','2022-01-01'),
('Parent','FAST_MERCH_ADV','FAST_DIV','FAS','PTE','2018-12-30','2019-12-28'),
('Parent','FAST_MERCH_ADV','FAST_DIV','FAS','PTE','2019-12-29','2021-01-02'),
('Parent','FAST_MERCH_ADV','FAST_DIV','FAS','PTE','2021-01-03','2022-01-01'),
('Parent','FAST_NAIL_STP','FAST_DIV','FAS','PTE','2018-12-30','2019-12-28'),
('Parent','FAST_NAIL_STP','FAST_DIV','FAS','PTE','2019-12-29','2021-01-02'),
('Parent','FAST_NAIL_STP','FAST_DIV','FAS','PTE','2021-01-03','2022-01-01'),
('Parent','FAST_OFFICE','FAST_DIV','FAS','PTE','2018-12-30','2019-12-28'),
('Parent','FAST_OFFICE','FAST_DIV','FAS','PTE','2019-12-29','2021-01-02'),
('Parent','FAST_OFFICE','FAST_DIV','FAS','PTE','2021-01-03','2022-01-01'),
('Parent','FAST_POW_RECON','ANF_DIV','ANF','PTE','2018-12-30','2019-12-28'),
('Parent','FAST_POW_RECON','ANF_DIV','ANF','PTE','2019-12-29','2021-01-02'),
('Parent','FAST_POW_RECON','ANF_DIV','ANF','PTE','2021-01-03','2022-01-01'),
('Parent','FAST_TOOL','FAST_DIV','FAS','PTE','2018-12-30','2019-12-28'),
('Parent','FAST_TOOL','FAST_DIV','FAS','PTE','2019-12-29','2021-01-02'),
('Parent','FAST_TOOL','FAST_DIV','FAS','PTE','2021-01-03','2022-01-01'),
('Parent','FORCED_ENTRY','ANF_DIV','ANF','PTE','2018-12-30','2019-12-28'),
('Parent','FORCED_ENTRY','ANF_DIV','ANF','PTE','2019-12-29','2021-01-02'),
('Parent','FORCED_ENTRY','ANF_DIV','ANF','PTE','2021-01-03','2022-01-01'),
('Parent','GRINDING','METAL_CONCRETE_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','GRINDING','METAL_CONCRETE_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','GRINDING','METAL_CONCRETE_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','GTS_Product','Total_Product','Product','','2018-12-30','2019-12-28'),
('Parent','GTS_Product','Total_Product','Product','','2019-12-29','2021-01-02'),
('Parent','GTS_Product','Total_Product','Product','','2021-01-03','2022-01-01'),
('Parent','HD_MECH_FAST_EXT_THR','ANF_DIV','ANF','PTE','2018-12-30','2019-12-28'),
('Parent','HD_MECH_FAST_EXT_THR','ANF_DIV','ANF','PTE','2019-12-29','2021-01-02'),
('Parent','HD_MECH_FAST_EXT_THR','ANF_DIV','ANF','PTE','2021-01-03','2022-01-01'),
('Parent','HD_MECH_FAST_INT_THR','ANF_DIV','ANF','PTE','2018-12-30','2019-12-28'),
('Parent','HD_MECH_FAST_INT_THR','ANF_DIV','ANF','PTE','2019-12-29','2021-01-02'),
('Parent','HD_MECH_FAST_INT_THR','ANF_DIV','ANF','PTE','2021-01-03','2022-01-01'),
('Parent','HOME','PTE','PTG','GTS_Product','2018-12-30','2019-12-28'),
('Parent','HOME','PTE','PTG','GTS_Product','2019-12-29','2021-01-02'),
('Parent','HOME','PTE','PTG','GTS_Product','2021-01-03','2022-01-01'),
('Parent','HOME_PROD','HOME','PTE','PTG','2018-12-30','2019-12-28'),
('Parent','HOME_PROD','HOME','PTE','PTG','2019-12-29','2021-01-02'),
('Parent','HOME_PROD','HOME','PTE','PTG','2021-01-03','2022-01-01'),
('Parent','HP_CORDED','HOME_PROD','HOME','PTE','2018-12-30','2019-12-28'),
('Parent','HP_CORDED','HOME_PROD','HOME','PTE','2019-12-29','2021-01-02'),
('Parent','HP_CORDED','HOME_PROD','HOME','PTE','2021-01-03','2022-01-01'),
('Parent','HP_CORDLESS','HOME_PROD','HOME','PTE','2018-12-30','2019-12-28'),
('Parent','HP_CORDLESS','HOME_PROD','HOME','PTE','2019-12-29','2021-01-02'),
('Parent','HP_CORDLESS','HOME_PROD','HOME','PTE','2021-01-03','2022-01-01'),
('Parent','HP_MISC','HOME_PROD','HOME','PTE','2018-12-30','2019-12-28'),
('Parent','HP_MISC','HOME_PROD','HOME','PTE','2019-12-29','2021-01-02'),
('Parent','HP_MISC','HOME_PROD','HOME','PTE','2021-01-03','2022-01-01'),
('Parent','HP_PAINTING','HOME_PROD','HOME','PTE','2018-12-30','2019-12-28'),
('Parent','HP_PAINTING','HOME_PROD','HOME','PTE','2019-12-29','2021-01-02'),
('Parent','HP_PAINTING','HOME_PROD','HOME','PTE','2021-01-03','2022-01-01'),
('Parent','HP_RECON','HOME_PROD','HOME','PTE','2018-12-30','2019-12-28'),
('Parent','HP_RECON','HOME_PROD','HOME','PTE','2019-12-29','2021-01-02'),
('Parent','HP_RECON','HOME_PROD','HOME','PTE','2021-01-03','2022-01-01'),
('Parent','HP_SDA','HOME_PROD','HOME','PTE','2018-12-30','2019-12-28'),
('Parent','HP_SDA','HOME_PROD','HOME','PTE','2019-12-29','2021-01-02'),
('Parent','HP_SDA','HOME_PROD','HOME','PTE','2021-01-03','2022-01-01'),
('Parent','HP_SPEC','HOME_PROD','HOME','PTE','2018-12-30','2019-12-28'),
('Parent','HP_SPEC','HOME_PROD','HOME','PTE','2019-12-29','2021-01-02'),
('Parent','HP_SPEC','HOME_PROD','HOME','PTE','2021-01-03','2022-01-01'),
('Parent','HP_STEAM','HOME_PROD','HOME','PTE','2018-12-30','2019-12-28'),
('Parent','HP_STEAM','HOME_PROD','HOME','PTE','2019-12-29','2021-01-02'),
('Parent','HP_STEAM','HOME_PROD','HOME','PTE','2021-01-03','2022-01-01'),
('Parent','HTAS','GTS_Product','Total_Product','Product','2018-12-30','2019-12-28'),
('Parent','HTAS','GTS_Product','Total_Product','Product','2019-12-29','2021-01-02'),
('Parent','HTAS','GTS_Product','Total_Product','Product','2021-01-03','2022-01-01'),
('Parent','HTAS_SBU','HTAS','GTS_Product','Total_Product','2018-12-30','2019-12-28'),
('Parent','HTAS_SBU','HTAS','GTS_Product','Total_Product','2019-12-29','2021-01-02'),
('Parent','HTAS_SBU','HTAS','GTS_Product','Total_Product','2021-01-03','2022-01-01'),
('Parent','IA_AUTO_SOL','IND_AUTO_TOOLS','HTAS_SBU','HTAS','2018-12-30','2019-12-28'),
('Parent','IA_AUTO_SOL','IND_AUTO_TOOLS','HTAS_SBU','HTAS','2019-12-29','2021-01-02'),
('Parent','IA_AUTO_SOL','IND_AUTO_TOOLS','HTAS_SBU','HTAS','2021-01-03','2022-01-01'),
('Parent','IA_AUTO_STOR','IA_AUTO_SOL','IND_AUTO_TOOLS','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','IA_AUTO_STOR','IA_AUTO_SOL','IND_AUTO_TOOLS','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','IA_AUTO_STOR','IA_AUTO_SOL','IND_AUTO_TOOLS','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','IA_BRAND_COMM','IA_TOOLS','IND_AUTO_TOOLS','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','IA_BRAND_COMM','IA_TOOLS','IND_AUTO_TOOLS','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','IA_BRAND_COMM','IA_TOOLS','IND_AUTO_TOOLS','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','IA_ENG_STOR','IA_AUTO_SOL','IND_AUTO_TOOLS','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','IA_ENG_STOR','IA_AUTO_SOL','IND_AUTO_TOOLS','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','IA_ENG_STOR','IA_AUTO_SOL','IND_AUTO_TOOLS','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','IA_HARDLINES','IA_TOOLS','IND_AUTO_TOOLS','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','IA_HARDLINES','IA_TOOLS','IND_AUTO_TOOLS','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','IA_HARDLINES','IA_TOOLS','IND_AUTO_TOOLS','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','IA_SD_PLIERS','IA_TOOLS','IND_AUTO_TOOLS','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','IA_SD_PLIERS','IA_TOOLS','IND_AUTO_TOOLS','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','IA_SD_PLIERS','IA_TOOLS','IND_AUTO_TOOLS','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','IA_SPEC_TOOLS','IA_TOOLS','IND_AUTO_TOOLS','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','IA_SPEC_TOOLS','IA_TOOLS','IND_AUTO_TOOLS','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','IA_SPEC_TOOLS','IA_TOOLS','IND_AUTO_TOOLS','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','IA_STOR_SOL','IND_AUTO_TOOLS','HTAS_SBU','HTAS','2018-12-30','2019-12-28'),
('Parent','IA_STOR_SOL','IND_AUTO_TOOLS','HTAS_SBU','HTAS','2019-12-29','2021-01-02'),
('Parent','IA_STOR_SOL','IND_AUTO_TOOLS','HTAS_SBU','HTAS','2021-01-03','2022-01-01'),
('Parent','IA_TOOL_RECON','IA_TOOLS','IND_AUTO_TOOLS','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','IA_TOOL_RECON','IA_TOOLS','IND_AUTO_TOOLS','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','IA_TOOL_RECON','IA_TOOLS','IND_AUTO_TOOLS','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','IA_TOOLS','IND_AUTO_TOOLS','HTAS_SBU','HTAS','2018-12-30','2019-12-28'),
('Parent','IA_TOOLS','IND_AUTO_TOOLS','HTAS_SBU','HTAS','2019-12-29','2021-01-02'),
('Parent','IA_TOOLS','IND_AUTO_TOOLS','HTAS_SBU','HTAS','2021-01-03','2022-01-01'),
('Parent','IND','HTAS_SBU','HTAS','GTS_Product','2018-12-30','2019-12-28'),
('Parent','IND','HTAS_SBU','HTAS','GTS_Product','2019-12-29','2021-01-02'),
('Parent','IND','HTAS_SBU','HTAS','GTS_Product','2021-01-03','2022-01-01'),
('Parent','IND_AUTO','PRODUCT_DoNotUse','GTS_Product','Total_Product','2018-12-30','2019-12-28'),
('Parent','IND_AUTO','PRODUCT_DoNotUse','GTS_Product','Total_Product','2019-12-29','2021-01-02'),
('Parent','IND_AUTO','PRODUCT_DoNotUse','GTS_Product','Total_Product','2021-01-03','2022-01-01'),
('Parent','IND_AUTO_ENG_STOR','PRODUCT_DoNotUse','GTS_Product','Total_Product','2018-12-30','2019-12-28'),
('Parent','IND_AUTO_ENG_STOR','PRODUCT_DoNotUse','GTS_Product','Total_Product','2019-12-29','2021-01-02'),
('Parent','IND_AUTO_ENG_STOR','PRODUCT_DoNotUse','GTS_Product','Total_Product','2021-01-03','2022-01-01'),
('Parent','IND_AUTO_TOOLS','HTAS_SBU','HTAS','GTS_Product','2018-12-30','2019-12-28'),
('Parent','IND_AUTO_TOOLS','HTAS_SBU','HTAS','GTS_Product','2019-12-29','2021-01-02'),
('Parent','IND_AUTO_TOOLS','HTAS_SBU','HTAS','GTS_Product','2021-01-03','2022-01-01'),
('Parent','IND_EQUIP','OTH_PRD_SVC_SBU','OTH_PRD_SVC','OTH_TOTAL','2018-12-30','2019-12-28'),
('Parent','IND_EQUIP','OTH_PRD_SVC_SBU','OTH_PRD_SVC','OTH_TOTAL','2019-12-29','2021-01-02'),
('Parent','IND_EQUIP','OTH_PRD_SVC_SBU','OTH_PRD_SVC','OTH_TOTAL','2021-01-03','2022-01-01'),
('Parent','IND_EQUIP_GROUP','IND_EQUIP','OTH_PRD_SVC_SBU','OTH_PRD_SVC','2018-12-30','2019-12-28'),
('Parent','IND_EQUIP_GROUP','IND_EQUIP','OTH_PRD_SVC_SBU','OTH_PRD_SVC','2019-12-29','2021-01-02'),
('Parent','IND_EQUIP_GROUP','IND_EQUIP','OTH_PRD_SVC_SBU','OTH_PRD_SVC','2021-01-03','2022-01-01'),
('Parent','IND_EQUIP_OTH','IND_EQUIP','OTH_PRD_SVC_SBU','OTH_PRD_SVC','2018-12-30','2019-12-28'),
('Parent','IND_EQUIP_OTH','IND_EQUIP','OTH_PRD_SVC_SBU','OTH_PRD_SVC','2019-12-29','2021-01-02'),
('Parent','IND_EQUIP_OTH','IND_EQUIP','OTH_PRD_SVC_SBU','OTH_PRD_SVC','2021-01-03','2022-01-01'),
('Parent','MED_LIGHT_MECH_FAST','ANF_DIV','ANF','PTE','2018-12-30','2019-12-28'),
('Parent','MED_LIGHT_MECH_FAST','ANF_DIV','ANF','PTE','2019-12-29','2021-01-02'),
('Parent','MED_LIGHT_MECH_FAST','ANF_DIV','ANF','PTE','2021-01-03','2022-01-01'),
('Parent','METAL_CONCRETE_DIV','PPT','PTE','PTG','2018-12-30','2019-12-28'),
('Parent','METAL_CONCRETE_DIV','PPT','PTE','PTG','2019-12-29','2021-01-02'),
('Parent','METAL_CONCRETE_DIV','PPT','PTE','PTG','2021-01-03','2022-01-01'),
('Parent','METAL_WORKING','METAL_CONCRETE_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','METAL_WORKING','METAL_CONCRETE_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','METAL_WORKING','METAL_CONCRETE_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','Newell_CATEGORY','Newell_DIV','Newell_SUB','Newell_Products','2018-12-30','2019-12-28'),
('Parent','Newell_CATEGORY','Newell_DIV','Newell_SUB','Newell_Products','2019-12-29','2021-01-02'),
('Parent','Newell_CATEGORY','Newell_DIV','Newell_SUB','Newell_Products','2021-01-03','2022-01-01'),
('Parent','Newell_DIV','Newell_SUB','Newell_Products','HTAS','2018-12-30','2019-12-28'),
('Parent','Newell_DIV','Newell_SUB','Newell_Products','HTAS','2019-12-29','2021-01-02'),
('Parent','Newell_DIV','Newell_SUB','Newell_Products','HTAS','2021-01-03','2022-01-01'),
('Parent','Newell_Products','HTAS','GTS_Product','Total_Product','2018-12-30','2019-12-28'),
('Parent','Newell_Products','HTAS','GTS_Product','Total_Product','2019-12-29','2021-01-02'),
('Parent','Newell_Products','HTAS','GTS_Product','Total_Product','2021-01-03','2022-01-01'),
('Parent','Newell_SUB','Newell_Products','HTAS','GTS_Product','2018-12-30','2019-12-28'),
('Parent','Newell_SUB','Newell_Products','HTAS','GTS_Product','2019-12-29','2021-01-02'),
('Parent','Newell_SUB','Newell_Products','HTAS','GTS_Product','2021-01-03','2022-01-01'),
('Parent','OTH_FACT_STORES','OTH_PRD_SVC_SBU','OTH_PRD_SVC','OTH_TOTAL','2018-12-30','2019-12-28'),
('Parent','OTH_FACT_STORES','OTH_PRD_SVC_SBU','OTH_PRD_SVC','OTH_TOTAL','2019-12-29','2021-01-02'),
('Parent','OTH_FACT_STORES','OTH_PRD_SVC_SBU','OTH_PRD_SVC','OTH_TOTAL','2021-01-03','2022-01-01'),
('Parent','Oth_Mixed_Products','GTS_Product','Total_Product','Product','2018-12-30','2019-12-28'),
('Parent','Oth_Mixed_Products','GTS_Product','Total_Product','Product','2019-12-29','2021-01-02'),
('Parent','Oth_Mixed_Products','GTS_Product','Total_Product','Product','2021-01-03','2022-01-01'),
('Parent','OTH_PRD_SVC','OTH_TOTAL','GTS_Product','Total_Product','2018-12-30','2019-12-28'),
('Parent','OTH_PRD_SVC','OTH_TOTAL','GTS_Product','Total_Product','2019-12-29','2021-01-02'),
('Parent','OTH_PRD_SVC','OTH_TOTAL','GTS_Product','Total_Product','2021-01-03','2022-01-01'),
('Parent','OTH_PRD_SVC_SBU','OTH_PRD_SVC','OTH_TOTAL','GTS_Product','2018-12-30','2019-12-28'),
('Parent','OTH_PRD_SVC_SBU','OTH_PRD_SVC','OTH_TOTAL','GTS_Product','2019-12-29','2021-01-02'),
('Parent','OTH_PRD_SVC_SBU','OTH_PRD_SVC','OTH_TOTAL','GTS_Product','2021-01-03','2022-01-01'),
('Parent','OTH_PS_ACC','OTH_SERVICE_TOT','OTH_PRD_SVC_SBU','OTH_PRD_SVC','2018-12-30','2019-12-28'),
('Parent','OTH_PS_ACC','OTH_SERVICE_TOT','OTH_PRD_SVC_SBU','OTH_PRD_SVC','2019-12-29','2021-01-02'),
('Parent','OTH_PS_ACC','OTH_SERVICE_TOT','OTH_PRD_SVC_SBU','OTH_PRD_SVC','2021-01-03','2022-01-01'),
('Parent','OTH_PS_PARTS','OTH_SERVICE_TOT','OTH_PRD_SVC_SBU','OTH_PRD_SVC','2018-12-30','2019-12-28'),
('Parent','OTH_PS_PARTS','OTH_SERVICE_TOT','OTH_PRD_SVC_SBU','OTH_PRD_SVC','2019-12-29','2021-01-02'),
('Parent','OTH_PS_PARTS','OTH_SERVICE_TOT','OTH_PRD_SVC_SBU','OTH_PRD_SVC','2021-01-03','2022-01-01'),
('Parent','OTH_PS_REBUILT','OTH_FACT_STORES','OTH_PRD_SVC_SBU','OTH_PRD_SVC','2018-12-30','2019-12-28'),
('Parent','OTH_PS_REBUILT','OTH_FACT_STORES','OTH_PRD_SVC_SBU','OTH_PRD_SVC','2019-12-29','2021-01-02'),
('Parent','OTH_PS_REBUILT','OTH_FACT_STORES','OTH_PRD_SVC_SBU','OTH_PRD_SVC','2021-01-03','2022-01-01'),
('Parent','OTH_SERVICE_TOT','OTH_PRD_SVC_SBU','OTH_PRD_SVC','OTH_TOTAL','2018-12-30','2019-12-28'),
('Parent','OTH_SERVICE_TOT','OTH_PRD_SVC_SBU','OTH_PRD_SVC','OTH_TOTAL','2019-12-29','2021-01-02'),
('Parent','OTH_SERVICE_TOT','OTH_PRD_SVC_SBU','OTH_PRD_SVC','OTH_TOTAL','2021-01-03','2022-01-01'),
('Parent','OTH_TOTAL','GTS_Product','Total_Product','Product','2018-12-30','2019-12-28'),
('Parent','OTH_TOTAL','GTS_Product','Total_Product','Product','2019-12-29','2021-01-02'),
('Parent','OTH_TOTAL','GTS_Product','Total_Product','Product','2021-01-03','2022-01-01'),
('Parent','OUT','PTG','GTS_Product','Total_Product','2018-12-30','2019-12-28'),
('Parent','OUT','PTG','GTS_Product','Total_Product','2019-12-29','2021-01-02'),
('Parent','OUT','PTG','GTS_Product','Total_Product','2021-01-03','2022-01-01'),
('Parent','OUT_ACC','OUTDOOR','OUTD_SBU','OUT','2018-12-30','2019-12-28'),
('Parent','OUT_ACC','OUTDOOR','OUTD_SBU','OUT','2019-12-29','2021-01-02'),
('Parent','OUT_ACC','OUTDOOR','OUTD_SBU','OUT','2021-01-03','2022-01-01'),
('Parent','OUT_CORDED','OUTDOOR','OUTD_SBU','OUT','2018-12-30','2019-12-28'),
('Parent','OUT_CORDED','OUTDOOR','OUTD_SBU','OUT','2019-12-29','2021-01-02'),
('Parent','OUT_CORDED','OUTDOOR','OUTD_SBU','OUT','2021-01-03','2022-01-01'),
('Parent','OUT_CORDLESS','OUTDOOR','OUTD_SBU','OUT','2018-12-30','2019-12-28'),
('Parent','OUT_CORDLESS','OUTDOOR','OUTD_SBU','OUT','2019-12-29','2021-01-02'),
('Parent','OUT_CORDLESS','OUTDOOR','OUTD_SBU','OUT','2021-01-03','2022-01-01'),
('Parent','OUT_GAS','OUTDOOR','OUTD_SBU','OUT','2018-12-30','2019-12-28'),
('Parent','OUT_GAS','OUTDOOR','OUTD_SBU','OUT','2019-12-29','2021-01-02'),
('Parent','OUT_GAS','OUTDOOR','OUTD_SBU','OUT','2021-01-03','2022-01-01'),
('Parent','OUT_MISC','OUTDOOR','OUTD_SBU','OUT','2018-12-30','2019-12-28'),
('Parent','OUT_MISC','OUTDOOR','OUTD_SBU','OUT','2019-12-29','2021-01-02'),
('Parent','OUT_MISC','OUTDOOR','OUTD_SBU','OUT','2021-01-03','2022-01-01'),
('Parent','OUT_NON_POWERED','OUTDOOR','OUTD_SBU','OUT','2018-12-30','2019-12-28'),
('Parent','OUT_NON_POWERED','OUTDOOR','OUTD_SBU','OUT','2019-12-29','2021-01-02'),
('Parent','OUT_NON_POWERED','OUTDOOR','OUTD_SBU','OUT','2021-01-03','2022-01-01'),
('Parent','OUT_OTH','OUTDOOR','OUTD_SBU','OUT','2018-12-30','2019-12-28'),
('Parent','OUT_OTH','OUTDOOR','OUTD_SBU','OUT','2019-12-29','2021-01-02'),
('Parent','OUT_OTH','OUTDOOR','OUTD_SBU','OUT','2021-01-03','2022-01-01'),
('Parent','OUT_RECON','OUTDOOR','OUTD_SBU','OUT','2018-12-30','2019-12-28'),
('Parent','OUT_RECON','OUTDOOR','OUTD_SBU','OUT','2019-12-29','2021-01-02'),
('Parent','OUT_RECON','OUTDOOR','OUTD_SBU','OUT','2021-01-03','2022-01-01'),
('Parent','OUTD_SBU','OUT','PTG','GTS_Product','2018-12-30','2019-12-28'),
('Parent','OUTD_SBU','OUT','PTG','GTS_Product','2019-12-29','2021-01-02'),
('Parent','OUTD_SBU','OUT','PTG','GTS_Product','2021-01-03','2022-01-01'),
('Parent','OUTDOOR','OUTD_SBU','OUT','PTG','2018-12-30','2019-12-28'),
('Parent','OUTDOOR','OUTD_SBU','OUT','PTG','2019-12-29','2021-01-02'),
('Parent','OUTDOOR','OUTD_SBU','OUT','PTG','2021-01-03','2022-01-01'),
('Parent','PORTABLE_WOODWORKING','WOOD_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','PORTABLE_WOODWORKING','WOOD_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','PORTABLE_WOODWORKING','WOOD_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','PPT','PTE','PTG','GTS_Product','2018-12-30','2019-12-28'),
('Parent','PPT','PTE','PTG','GTS_Product','2019-12-29','2021-01-02'),
('Parent','PPT','PTE','PTG','GTS_Product','2021-01-03','2022-01-01'),
('Parent','PPT_CORDED_DIV','PPT','PTE','PTG','2018-12-30','2019-12-28'),
('Parent','PPT_CORDED_DIV','PPT','PTE','PTG','2019-12-29','2021-01-02'),
('Parent','PPT_CORDED_DIV','PPT','PTE','PTG','2021-01-03','2022-01-01'),
('Parent','PPT_CORDLESS_24V','DFES_AUTOPT_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','PPT_CORDLESS_24V','DFES_AUTOPT_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','PPT_CORDLESS_24V','DFES_AUTOPT_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','PPT_CORDLESS_BATT','DFES_AUTOPT_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','PPT_CORDLESS_BATT','DFES_AUTOPT_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','PPT_CORDLESS_BATT','DFES_AUTOPT_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','PPT_CORDLESS_COMBO','DFES_AUTOPT_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','PPT_CORDLESS_COMBO','DFES_AUTOPT_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','PPT_CORDLESS_COMBO','DFES_AUTOPT_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','PPT_CORDLESS_DIV','PPT','PTE','PTG','2018-12-30','2019-12-28'),
('Parent','PPT_CORDLESS_DIV','PPT','PTE','PTG','2019-12-29','2021-01-02'),
('Parent','PPT_CORDLESS_DIV','PPT','PTE','PTG','2021-01-03','2022-01-01'),
('Parent','PPT_CORDLESS_DRILLFAST','DFES_AUTOPT_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','PPT_CORDLESS_DRILLFAST','DFES_AUTOPT_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','PPT_CORDLESS_DRILLFAST','DFES_AUTOPT_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','PPT_CORDLESS_GRINDER','METAL_CONCRETE_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','PPT_CORDLESS_GRINDER','METAL_CONCRETE_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','PPT_CORDLESS_GRINDER','METAL_CONCRETE_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','PPT_CORDLESS_MASONRY','METAL_CONCRETE_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','PPT_CORDLESS_MASONRY','METAL_CONCRETE_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','PPT_CORDLESS_MASONRY','METAL_CONCRETE_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','PPT_CORDLESS_MW','METAL_CONCRETE_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','PPT_CORDLESS_MW','METAL_CONCRETE_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','PPT_CORDLESS_MW','METAL_CONCRETE_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','PPT_CORDLESS_NS','PPT_CORDLESS_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','PPT_CORDLESS_NS','PPT_CORDLESS_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','PPT_CORDLESS_NS','PPT_CORDLESS_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','PPT_CORDLESS_OTH','DFES_AUTOPT_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','PPT_CORDLESS_OTH','DFES_AUTOPT_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','PPT_CORDLESS_OTH','DFES_AUTOPT_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','PPT_CORDLESS_RECON','DFES_AUTOPT_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','PPT_CORDLESS_RECON','DFES_AUTOPT_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','PPT_CORDLESS_RECON','DFES_AUTOPT_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','PPT_CORDLESS_ROTARY','METAL_CONCRETE_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','PPT_CORDLESS_ROTARY','METAL_CONCRETE_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','PPT_CORDLESS_ROTARY','METAL_CONCRETE_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','PPT_CORDLESS_SAWS','WOOD_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','PPT_CORDLESS_SAWS','WOOD_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','PPT_CORDLESS_SAWS','WOOD_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','PPT_CORDLESS_VAC','METAL_CONCRETE_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','PPT_CORDLESS_VAC','METAL_CONCRETE_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','PPT_CORDLESS_VAC','METAL_CONCRETE_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','PPT_GARAGE_DOOR','METAL_CONCRETE_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','PPT_GARAGE_DOOR','METAL_CONCRETE_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','PPT_GARAGE_DOOR','METAL_CONCRETE_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','Product','','','','2018-12-30','2019-12-28'),
('Parent','Product','','','','2019-12-29','2021-01-02'),
('Parent','Product','','','','2021-01-03','2022-01-01'),
('Parent','PRODUCT_DoNotUse','GTS_Product','Total_Product','Product','2018-12-30','2019-12-28'),
('Parent','PRODUCT_DoNotUse','GTS_Product','Total_Product','Product','2019-12-29','2021-01-02'),
('Parent','PRODUCT_DoNotUse','GTS_Product','Total_Product','Product','2021-01-03','2022-01-01'),
('Parent','PTA_Bandsaw','IND','HTAS_SBU','HTAS','2018-12-30','2019-12-28'),
('Parent','PTA_Bandsaw','IND','HTAS_SBU','HTAS','2019-12-29','2021-01-02'),
('Parent','PTA_Bandsaw','IND','HTAS_SBU','HTAS','2021-01-03','2022-01-01'),
('Parent','PTA_COMM','IND','HTAS_SBU','HTAS','2018-12-30','2019-12-28'),
('Parent','PTA_COMM','IND','HTAS_SBU','HTAS','2019-12-29','2021-01-02'),
('Parent','PTA_COMM','IND','HTAS_SBU','HTAS','2021-01-03','2022-01-01'),
('Parent','PTA_CONS','CONSTR_HT','HTAS_SBU','HTAS','2018-12-30','2019-12-28'),
('Parent','PTA_CONS','CONSTR_HT','HTAS_SBU','HTAS','2019-12-29','2021-01-02'),
('Parent','PTA_CONS','CONSTR_HT','HTAS_SBU','HTAS','2021-01-03','2022-01-01'),
('Parent','PTA_CONS_ABR','PTA_COMM','IND','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','PTA_CONS_ABR','PTA_COMM','IND','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','PTA_CONS_ABR','PTA_COMM','IND','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','PTA_CONS_DIAMONDS','PTA_COMM','IND','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','PTA_CONS_DIAMONDS','PTA_COMM','IND','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','PTA_CONS_DIAMONDS','PTA_COMM','IND','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','PTA_CONS_FAST','PTA_CONS','CONSTR_HT','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','PTA_CONS_FAST','PTA_CONS','CONSTR_HT','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','PTA_CONS_FAST','PTA_CONS','CONSTR_HT','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','PTA_CONS_LINE','PTA_CONS','CONSTR_HT','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','PTA_CONS_LINE','PTA_CONS','CONSTR_HT','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','PTA_CONS_LINE','PTA_CONS','CONSTR_HT','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','PTA_CONS_MASON','PTA_COMM','IND','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','PTA_CONS_MASON','PTA_COMM','IND','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','PTA_CONS_MASON','PTA_COMM','IND','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','PTA_CONS_METAL','PTA_COMM','IND','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','PTA_CONS_METAL','PTA_COMM','IND','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','PTA_CONS_METAL','PTA_COMM','IND','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','PTA_CONS_MISC','PTA_CONS','CONSTR_HT','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','PTA_CONS_MISC','PTA_CONS','CONSTR_HT','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','PTA_CONS_MISC','PTA_CONS','CONSTR_HT','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','PTA_CONS_OTH','PTA_CONS','CONSTR_HT','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','PTA_CONS_OTH','PTA_CONS','CONSTR_HT','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','PTA_CONS_OTH','PTA_CONS','CONSTR_HT','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','PTA_CONS_RECON','PTA_CONS','CONSTR_HT','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','PTA_CONS_RECON','PTA_CONS','CONSTR_HT','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','PTA_CONS_RECON','PTA_CONS','CONSTR_HT','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','PTA_CONS_ROUTER','PTA_CONS','CONSTR_HT','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','PTA_CONS_ROUTER','PTA_CONS','CONSTR_HT','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','PTA_CONS_ROUTER','PTA_CONS','CONSTR_HT','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','PTA_CONS_SAW','PTA_CONS','CONSTR_HT','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','PTA_CONS_SAW','PTA_CONS','CONSTR_HT','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','PTA_CONS_SAW','PTA_CONS','CONSTR_HT','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','PTA_CONS_WOOD','PTA_CONS','CONSTR_HT','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','PTA_CONS_WOOD','PTA_CONS','CONSTR_HT','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','PTA_CONS_WOOD','PTA_CONS','CONSTR_HT','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','PTA_IND','IND','HTAS_SBU','HTAS','2018-12-30','2019-12-28'),
('Parent','PTA_IND','IND','HTAS_SBU','HTAS','2019-12-29','2021-01-02'),
('Parent','PTA_IND','IND','HTAS_SBU','HTAS','2021-01-03','2022-01-01'),
('Parent','PTA_IND_ABR','PTA_COMM','IND','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','PTA_IND_ABR','PTA_COMM','IND','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','PTA_IND_ABR','PTA_COMM','IND','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','PTA_IND_DIAMONDS','PTA_COMM','IND','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','PTA_IND_DIAMONDS','PTA_COMM','IND','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','PTA_IND_DIAMONDS','PTA_COMM','IND','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','PTA_IND_FAST','PTA_CONS','CONSTR_HT','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','PTA_IND_FAST','PTA_CONS','CONSTR_HT','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','PTA_IND_FAST','PTA_CONS','CONSTR_HT','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','PTA_IND_FS','PTA_CONS','CONSTR_HT','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','PTA_IND_FS','PTA_CONS','CONSTR_HT','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','PTA_IND_FS','PTA_CONS','CONSTR_HT','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','PTA_IND_LINEAR','PTA_CONS','CONSTR_HT','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','PTA_IND_LINEAR','PTA_CONS','CONSTR_HT','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','PTA_IND_LINEAR','PTA_CONS','CONSTR_HT','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','PTA_IND_MASON','PTA_COMM','IND','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','PTA_IND_MASON','PTA_COMM','IND','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','PTA_IND_MASON','PTA_COMM','IND','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','PTA_IND_METAL','PTA_COMM','IND','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','PTA_IND_METAL','PTA_COMM','IND','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','PTA_IND_METAL','PTA_COMM','IND','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','PTA_IND_MISC','PTA_CONS','CONSTR_HT','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','PTA_IND_MISC','PTA_CONS','CONSTR_HT','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','PTA_IND_MISC','PTA_CONS','CONSTR_HT','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','PTA_IND_OTH','PTA_CONS','CONSTR_HT','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','PTA_IND_OTH','PTA_CONS','CONSTR_HT','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','PTA_IND_OTH','PTA_CONS','CONSTR_HT','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','PTA_IND_RECON','PTA_CONS','CONSTR_HT','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','PTA_IND_RECON','PTA_CONS','CONSTR_HT','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','PTA_IND_RECON','PTA_CONS','CONSTR_HT','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','PTA_IND_SAW','PTA_CONS','CONSTR_HT','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','PTA_IND_SAW','PTA_CONS','CONSTR_HT','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','PTA_IND_SAW','PTA_CONS','CONSTR_HT','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','PTA_IND_STEP','PTA_COMM','IND','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','PTA_IND_STEP','PTA_COMM','IND','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','PTA_IND_STEP','PTA_COMM','IND','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','PTA_IND_TAP','IA_TOOLS','IND_AUTO_TOOLS','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','PTA_IND_TAP','IA_TOOLS','IND_AUTO_TOOLS','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','PTA_IND_TAP','IA_TOOLS','IND_AUTO_TOOLS','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','PTA_IND_WOOD','PTA_CONS','CONSTR_HT','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','PTA_IND_WOOD','PTA_CONS','CONSTR_HT','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','PTA_IND_WOOD','PTA_CONS','CONSTR_HT','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','PTE','PTG','GTS_Product','Total_Product','2018-12-30','2019-12-28'),
('Parent','PTE','PTG','GTS_Product','Total_Product','2019-12-29','2021-01-02'),
('Parent','PTE','PTG','GTS_Product','Total_Product','2021-01-03','2022-01-01'),
('Parent','PTG','GTS_Product','Total_Product','Product','2018-12-30','2019-12-28'),
('Parent','PTG','GTS_Product','Total_Product','Product','2019-12-29','2021-01-02'),
('Parent','PTG','GTS_Product','Total_Product','Product','2021-01-03','2022-01-01'),
('Parent','ROD_REBAR_ADH_INSERT','ANF_DIV','ANF','PTE','2018-12-30','2019-12-28'),
('Parent','ROD_REBAR_ADH_INSERT','ANF_DIV','ANF','PTE','2019-12-29','2021-01-02'),
('Parent','ROD_REBAR_ADH_INSERT','ANF_DIV','ANF','PTE','2021-01-03','2022-01-01'),
('Parent','ROTARY_HAMMERS','METAL_CONCRETE_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','ROTARY_HAMMERS','METAL_CONCRETE_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','ROTARY_HAMMERS','METAL_CONCRETE_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','RTL_METAL_STOR','CONSTR_METAL_STR','IND_AUTO_TOOLS','HTAS_SBU','2018-12-30','2019-12-28'),
('Parent','RTL_METAL_STOR','CONSTR_METAL_STR','IND_AUTO_TOOLS','HTAS_SBU','2019-12-29','2021-01-02'),
('Parent','RTL_METAL_STOR','CONSTR_METAL_STR','IND_AUTO_TOOLS','HTAS_SBU','2021-01-03','2022-01-01'),
('Parent','S3','HTAS','GTS_Product','Total_Product','2018-12-30','2019-12-28'),
('Parent','S3','HTAS','GTS_Product','Total_Product','2019-12-29','2021-01-02'),
('Parent','S3','HTAS','GTS_Product','Total_Product','2021-01-03','2022-01-01'),
('Parent','S3_CATEGORY','S3_DIV','S3_SUB','S3','2018-12-30','2019-12-28'),
('Parent','S3_CATEGORY','S3_DIV','S3_SUB','S3','2019-12-29','2021-01-02'),
('Parent','S3_CATEGORY','S3_DIV','S3_SUB','S3','2021-01-03','2022-01-01'),
('Parent','S3_DIV','S3_SUB','S3','HTAS','2018-12-30','2019-12-28'),
('Parent','S3_DIV','S3_SUB','S3','HTAS','2019-12-29','2021-01-02'),
('Parent','S3_DIV','S3_SUB','S3','HTAS','2021-01-03','2022-01-01'),
('Parent','S3_SUB','S3','HTAS','GTS_Product','2018-12-30','2019-12-28'),
('Parent','S3_SUB','S3','HTAS','GTS_Product','2019-12-29','2021-01-02'),
('Parent','S3_SUB','S3','HTAS','GTS_Product','2021-01-03','2022-01-01'),
('Parent','SCREWS_NUTS_BOLTS_MISC','ANF_DIV','ANF','PTE','2018-12-30','2019-12-28'),
('Parent','SCREWS_NUTS_BOLTS_MISC','ANF_DIV','ANF','PTE','2019-12-29','2021-01-02'),
('Parent','SCREWS_NUTS_BOLTS_MISC','ANF_DIV','ANF','PTE','2021-01-03','2022-01-01'),
('Parent','Svcs','GTS_Product','Total_Product','Product','2018-12-30','2019-12-28'),
('Parent','Svcs','GTS_Product','Total_Product','Product','2019-12-29','2021-01-02'),
('Parent','Svcs','GTS_Product','Total_Product','Product','2021-01-03','2022-01-01'),
('Parent','TD_AUTOMOTIVE','TRADESMAN','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','TD_AUTOMOTIVE','TRADESMAN','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','TD_AUTOMOTIVE','TRADESMAN','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','TD_BATT','TRADESMAN','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','TD_BATT','TRADESMAN','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','TD_BATT','TRADESMAN','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','TD_BENCH','TRADESMAN','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','TD_BENCH','TRADESMAN','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','TD_BENCH','TRADESMAN','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','TD_CONSTR_SAWS','TRADESMAN','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','TD_CONSTR_SAWS','TRADESMAN','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','TD_CONSTR_SAWS','TRADESMAN','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','TD_CORDED_DRILLS','TRADESMAN','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','TD_CORDED_DRILLS','TRADESMAN','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','TD_CORDED_DRILLS','TRADESMAN','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','TD_CORDED_FAST','TRADESMAN','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','TD_CORDED_FAST','TRADESMAN','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','TD_CORDED_FAST','TRADESMAN','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','TD_CORDED_METAL','TRADESMAN','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','TD_CORDED_METAL','TRADESMAN','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','TD_CORDED_METAL','TRADESMAN','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','TD_CORDED_OTH','TRADESMAN','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','TD_CORDED_OTH','TRADESMAN','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','TD_CORDED_OTH','TRADESMAN','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','TD_CORDED_SAWS','TRADESMAN','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','TD_CORDED_SAWS','TRADESMAN','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','TD_CORDED_SAWS','TRADESMAN','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','TD_CORDLESS_COMBO','TRADESMAN','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','TD_CORDLESS_COMBO','TRADESMAN','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','TD_CORDLESS_COMBO','TRADESMAN','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','TD_CORDLESS_DRILLS','TRADESMAN','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','TD_CORDLESS_DRILLS','TRADESMAN','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','TD_CORDLESS_DRILLS','TRADESMAN','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','TD_CORDLESS_EXP','TRADESMAN','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','TD_CORDLESS_EXP','TRADESMAN','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','TD_CORDLESS_EXP','TRADESMAN','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','TD_CORDLESS_METAL','TRADESMAN','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','TD_CORDLESS_METAL','TRADESMAN','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','TD_CORDLESS_METAL','TRADESMAN','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','TD_CORDLESS_NS','TRADESMAN','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','TD_CORDLESS_NS','TRADESMAN','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','TD_CORDLESS_NS','TRADESMAN','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','TD_CORDLESS_SAWS','TRADESMAN','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','TD_CORDLESS_SAWS','TRADESMAN','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','TD_CORDLESS_SAWS','TRADESMAN','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','TD_CORDLESS_VAC','TRADESMAN','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','TD_CORDLESS_VAC','TRADESMAN','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','TD_CORDLESS_VAC','TRADESMAN','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','TD_METAL','TRADESMAN','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','TD_METAL','TRADESMAN','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','TD_METAL','TRADESMAN','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','TD_MISC','TRADESMAN','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','TD_MISC','TRADESMAN','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','TD_MISC','TRADESMAN','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','TD_PORTABLE_WW','TRADESMAN','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','TD_PORTABLE_WW','TRADESMAN','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','TD_PORTABLE_WW','TRADESMAN','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','TD_RECON','TRADESMAN','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','TD_RECON','TRADESMAN','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','TD_RECON','TRADESMAN','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','TD_ROTARY','TRADESMAN','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','TD_ROTARY','TRADESMAN','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','TD_ROTARY','TRADESMAN','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','TD_TILE','TRADESMAN','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','TD_TILE','TRADESMAN','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','TD_TILE','TRADESMAN','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','TD_VAC_DUST','TRADESMAN','CPT','PTE','2018-12-30','2019-12-28'),
('Parent','TD_VAC_DUST','TRADESMAN','CPT','PTE','2019-12-29','2021-01-02'),
('Parent','TD_VAC_DUST','TRADESMAN','CPT','PTE','2021-01-03','2022-01-01'),
('Parent','TILE_MASONRY','METAL_CONCRETE_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','TILE_MASONRY','METAL_CONCRETE_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','TILE_MASONRY','METAL_CONCRETE_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','Total_Product','','','','2018-12-30','2019-12-28'),
('Parent','Total_Product','','','','2019-12-29','2021-01-02'),
('Parent','Total_Product','','','','2021-01-03','2022-01-01'),
('Parent','TRADESMAN','CPT','PTE','PTG','2018-12-30','2019-12-28'),
('Parent','TRADESMAN','CPT','PTE','PTG','2019-12-29','2021-01-02'),
('Parent','TRADESMAN','CPT','PTE','PTG','2021-01-03','2022-01-01'),
('Parent','VACUUMS','METAL_CONCRETE_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','VACUUMS','METAL_CONCRETE_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','VACUUMS','METAL_CONCRETE_DIV','PPT','PTE','2021-01-03','2022-01-01'),
('Parent','WOOD_DIV','PPT','PTE','PTG','2018-12-30','2019-12-28'),
('Parent','WOOD_DIV','PPT','PTE','PTG','2019-12-29','2021-01-02'),
('Parent','WOOD_DIV','PPT','PTE','PTG','2021-01-03','2022-01-01'),
('Parent','WOODWORKING_OTHER','WOOD_DIV','PPT','PTE','2018-12-30','2019-12-28'),
('Parent','WOODWORKING_OTHER','WOOD_DIV','PPT','PTE','2019-12-29','2021-01-02'),
('Parent','WOODWORKING_OTHER','WOOD_DIV','PPT','PTE','2021-01-03','2022-01-01');

	
end;
$$
;

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

CREATE OR REPLACE PROCEDURE ref_data.p_build_reference_rsa_bible()
 LANGUAGE plpgsql
AS $_$
BEGIN 
	/*
	 * 
	 * 		call ref_data.p_build_reference_rsa_bible ()
	 * 		select source_system, count(*) from ref_data.rsa_bible group by source_system;
	 */
	
	DROP TABLE IF EXISTS rsa_bible_us
	;
	CREATE TEMPORARY TABLE rsa_bible_us (
		region 				varchar(30),
		demand_group		varchar(30),
		customer			varchar(50),
		division 			varchar(30),
		brand				varchar(30),
		sku					varchar(50),
		yr					int,
		month_num			int,
		amt					varchar(30), 	
		pcr	 				varchar(300),
		mgsv  				varchar(30)
	) diststyle all
	;
	
	insert into rsa_bible_us (
				region,
				demand_group,
				customer,
				division,
				brand,
				sku,
				yr,
				month_num,
				amt,
				pcr,
				mgsv
		)
		select 	rc.region,
				rc.demandgroup,
				rc.customer,
				rc.division,
				rc.brand,
				rc.sku,
				cast(rc.year as integer) as year,
				cast(rc.period as integer) as period,
				rc.rsa_amt,
				rc.pcr,
				rc.mgsv
		from 	sftpgtsi.rsabible_current rc 
		where 	lower(rc.region) = 'us'
	;

	DROP TABLE IF EXISTS rsa_bible_cad
	;
	CREATE TEMPORARY TABLE rsa_bible_cad (
		region 				varchar(30),
		demand_group		varchar(30),
		customer			varchar(50),
		division 			varchar(30),
		brand				varchar(30),
		sku					varchar(50),
		yr					int,
		month_num			int,
		amt					varchar(30), 	
		pcr	 				varchar(300),
		mgsv  				varchar(30)
	) diststyle all
	;
	
	insert into rsa_bible_cad (
				region,
				demand_group,
				customer,
				division,
				brand,
				sku,
				yr,
				month_num,
				amt,
				pcr,
				mgsv
		)
		select 	rc.region,
				rc.demandgroup,
				rc.customer,
				rc.division,
				rc.brand,
				rc.sku,
				cast(rc.year as integer) as year,
				cast(rc.period as integer) as period,
				rc.rsa_amt,
				rc.pcr,
				rc.mgsv
		from 	sftpgtsi.rsabible_current rc 
		where 	lower(rc.region) = 'cad'
	;
	delete from  ref_data.rsa_bible;
	insert into ref_data.rsa_bible (
				source_system,
				demand_group,
				division,
				brand,
				sku,
				fiscal_month_id,
				amt,
				amt_str,
				pcr,
				mgsv
		)
		select 	'rsa_bible_cad' as source_system,
				ltrim(rtrim(demand_group)) as demand_group,
				case 
					when ltrim(rtrim(division)) = '' then null 
					else right('0' || ltrim(rtrim(division)), 2)
				end as division,
				ltrim(rtrim(brand)) as brand,
				ltrim(rtrim(sku)) as sku,
				((yr * 100) + month_num) as fiscal_month_id,
				CASE 
					when ltrim(rtrim(replace(amt,'$',''))) = '-' then 0.0
					when ltrim(rtrim(replace(amt,'$',''))) = '' then null
					when charindex(')', ltrim(rtrim(amt))) > 0 then 
						cast(replace(replace(replace(replace(ltrim(rtrim(amt)),',','' ),'(',''),')',''),'$','') as decimal(38,8) ) * -1
					else 
						cast(replace(replace(replace(replace(ltrim(rtrim(amt)),',','' ),'(',''),')',''),'$','') as decimal(38,8) )
				END as amt,
				ltrim(rtrim(amt)) as amt_str,
				ltrim(rtrim(pcr)) as pcr,
				ltrim(rtrim(mgsv)) as mgsv
		from 	rsa_bible_cad
		where 	((yr * 100) + month_num) is not null
		union all
		select 	'rsa_bible_us' as source_system,
				ltrim(rtrim(demand_group)) as demand_group,
				case 
					when ltrim(rtrim(division)) = '' then null 
					else right('0' || ltrim(rtrim(division)), 2)
				end as division,
				ltrim(rtrim(brand)) as brand,
				ltrim(rtrim(sku)) as sku,
				((yr * 100) + month_num) as fiscal_month_id,
				CASE 
					when ltrim(rtrim(replace(amt,'$',''))) = '-' then 0.0
					when ltrim(rtrim(replace(amt,'$',''))) = '' then null
					when charindex(')', ltrim(rtrim(amt))) > 0 then 
						cast(replace(replace(replace(replace(ltrim(rtrim(amt)),',','' ),'(',''),')',''),'$','') as decimal(38,8) ) * -1
					else 
						cast(replace(replace(replace(replace(ltrim(rtrim(amt)),',','' ),'(',''),')',''),'$','') as decimal(38,8) )
				END as amt,
				ltrim(rtrim(amt)) as amt_str,
				ltrim(rtrim(pcr)) as pcr,
				ltrim(rtrim(mgsv)) as mgsv
		from 	rsa_bible_us
		where 	((yr * 100) + month_num) is not null
	;
end  
$_$
;

CREATE OR REPLACE PROCEDURE ref_data.p_build_sku_barbrand_mapping()
 LANGUAGE plpgsql
AS $$
BEGIN 
	/*  create mapping table for material -> bar_brand 
	 * 	based on historical transactions
	 */
	drop table if exists stage_sku_barbrand_mapping
	;
	create temporary table stage_sku_barbrand_mapping
	diststyle all
	as 
		with 
			cte_base as (	
				select	distinct 
						cast(material as varchar(30)) as material,
						cast(bar_brand as varchar(14)) as bar_brand,
						cast((case when s.postdate = '' then null else postdate end) as date)  as postdate
				from 	bods.c11_0ec_pca3_current s
				inner join ref_data.entity rbh on s.bar_entity = rbh.name
						---only accounts thats contributes to sgm pnl structure
						inner join (
							select 	distinct bar_acct 
							from 	ref_data.pnl_acct
						) acct 
							on 	lower(s.bar_acct) = lower(acct.bar_acct) 
				where 	s.bar_acct is not null 
					and s.bar_entity is not null 
					and s.bar_acct <> ''
					and rbh.level4 = 'GTS_NA'
					--and s.bar_bu in ('GTS')
--					and s.bar_acct not in ('IGNORE')
--					and s.bar_currtype in ('USD' ,'CAD')
					and case when cast(material as varchar(30)) = '' then null else  cast(material as varchar(30)) end is not null 
					and cast((case when s.postdate = '' then null else postdate end) as date)  >= cast('2018-12-30' as date)  
		
					-- TESTING
					--and material = 'CMAS261290'
			)
			,cte_base_next as (
				select 	base.material,
						base.bar_brand,
						base.postdate,
						lead(base.bar_brand) over(partition by base.material order by base.postdate) as bar_brand_next
				from 	cte_base base
			)
			,cte_base_historical as (
				select 	nxt.material,
						nxt.bar_brand,
						nxt.postdate,
						
						nxt.bar_brand_next,
						lead(nxt.postdate) over (partition by nxt.material order by nxt.postdate) as postdate_next,
						row_number() over (partition by nxt.material order by nxt.postdate) rnk
				from 	cte_base_next nxt
				where 	nxt.bar_brand != nxt.bar_brand_next or 
						nxt.bar_brand_next is null
			)
		select 	hist.material,
				hist.bar_brand,				
		--		hist.postdate,
		--		hist.bar_brand_next,
		--		hist.rnk,
				case
					when hist.rnk = 1 then cast('1900-01-01' as date) 
					else cast(hist.postdate as date) 
				end as start_date,
				
				case
					when hist.bar_brand_next is null then cast('9999-12-31' as date) 
					else cast(dateadd(day,-1,hist.postdate_next) as date)
				end as end_date,
				
				case when hist.bar_brand_next is null then 1 else 0 end as current_flag,
					
				getdate() as audit_loadts
					
		from 	cte_base_historical as hist
	;
--	select * from stage_sku_barbrand_mapping
--	where material = 'CMAS261290'
	drop table if exists stage_sku_barbrand_mapping_gpp; 
	create temporary table stage_sku_barbrand_mapping_gpp
	diststyle all
	as  
	SELECT  distinct cast(cmac.matnr as varchar(30)) as material,
				  brnd.wgbez as brand,
				  cast('1900-01-01' as date) start_date, 
				  cast('12-31-9999' as date) end_date, 
				  1 as current_flag,
				  getdate() as audit_loadts
	FROM 	sapc11.mara_current cmac 
	left join sapc11.t023t_current brnd on cmac.matkl = brnd.matkl 
	WHERE 	'P' + SPLIT_PART(cmac.wrkst, '-', 4) is not null
	--and left(brnd.matkl,1) = 'T'
	and brnd.spras = 'E'
	and brnd.wgbez is not null;

	delete from ref_data.sku_barbrand_mapping
	;
	---insert materials from gpp hierarchy, and left overs from c11 transactions
	insert into ref_data.sku_barbrand_mapping (
				material,
				bar_brand,
				start_date,
				end_date,
				current_flag,
				audit_loadts
		)
		select 	material,
				brand,
				start_date,
				end_date,
				current_flag,
				getdate() as audit_loadts
		from 	stage_sku_barbrand_mapping_gpp 
	;
	
	---Add leftovers now 
		insert into ref_data.sku_barbrand_mapping (
				material,
				bar_brand,
				start_date,
				end_date,
				current_flag,
				audit_loadts
		)
		select 	tran_based.material,
				tran_based.bar_brand,
				tran_based.start_date,
				tran_based.end_date,
				tran_based.current_flag,
				getdate() as audit_loadts
		from 	stage_sku_barbrand_mapping  tran_based 
		left join stage_sku_barbrand_mapping_gpp gpp_based on tran_based.material = gpp_based.material 
		where gpp_based.material is null
	;
	exception
		when others then raise info 'exception occur while ingesting data in ref_data.barcust_soldto_mapping';
end
$$
;

CREATE OR REPLACE PROCEDURE ref_data.p_build_sku_barbrand_mapping_sgm(fmthid integer)
 LANGUAGE plpgsql
AS $$
BEGIN 
	/*
	 * 		drop procedure ref_data.p_build_sku_barbrand_mapping_sgm(fmthid integer)
	 * 		delete from ref_data.sku_barbrand_mapping_sgm;
	 * 		call ref_data.p_build_sku_barbrand_mapping_sgm(202101)
	 * 		select count(*) from ref_data.sku_barbrand_mapping_sgm;
	 * 		select distinct ss_fiscal_month_id from ref_data.sku_barbrand_mapping_sgm;
	 * 		grant execute on procedure ref_data.p_build_sku_barbrand_mapping_sgm(fmthid integer) to group "g-ada-rsabible-sb-ro";
	 */
	
	/*
	 *		This procedure creates a custom mapping: sku -> bar_brand 
	 *		
	 *		The final table is a snapshot by fiscal month where each material
	 *		is mapped to a single bar_brand based on the highest invoice sales 
	 *		transactions (A40110) from the beginning of time up to the current
	 *		fiscal period.
	 *
	 * 		Final Table(s): 
	 *			ref_data.sku_barbrand_mapping_sgm
	 *
	 */
	
	/*  create mapping table for material -> brand
	 */
	drop table if exists stage_sku_barbrand_mapping_sgm
	;
	create temporary table stage_sku_barbrand_mapping_sgm
	diststyle all
	as 
	with
		cte_base as (	
			SELECT 	fmthid as ss_fiscal_month_id,
					dp.material,
					dp.product_brand AS bar_brand,
					sum(f.amt_usd) as total_amt_usd
			FROM 	dw.dim_product AS dp
					INNER JOIN dw.fact_pnl_commercial_stacked AS f
						ON 	f.product_key = dp.product_key
			WHERE 	lower(dp.material) not in ('unknown') and 
					f.bar_acct = 'A40110' and 
					f.fiscal_month_id <= fmthid
			group by dp.material,
					dp.product_brand
		),
		cte_rank as (
			select 	base.ss_fiscal_month_id,
					base.material,
					base.bar_brand,
					base.total_amt_usd,
					rank() over(
						partition by base.material
						order by base.total_amt_usd desc, base.bar_brand
					) as rnk
			from 	cte_base as base
		)
		select 	rnk.ss_fiscal_month_id,
				rnk.material,
				rnk.bar_brand
		from 	cte_rank as rnk
		where 	rnk.rnk = 1
	;
	delete 
	from 	ref_data.sku_barbrand_mapping_sgm
	where 	ss_fiscal_month_id = fmthid
	;
	insert into ref_data.sku_barbrand_mapping_sgm (
				ss_fiscal_month_id,
				material,
				bar_brand,
				audit_loadts
		)
		select 	stg.ss_fiscal_month_id,
				stg.material,
				stg.bar_brand,
				getdate() as audit_loadts
		from 	stage_sku_barbrand_mapping_sgm stg
	;
	
	exception
		when others then raise info 'exception occur while ingesting data in ref_data.p_build_sku_barbrand_mapping_sgm';
end
$$
;

CREATE OR REPLACE PROCEDURE ref_data.p_build_sku_barproduct_mapping()
 LANGUAGE plpgsql
AS $$
BEGIN 
	
	/*  create mapping table for material -> gpp_portfolio 
	 * 	based on [bods.c11_0material_attr_current]
	 */
	drop table if exists stage_sku_barproduct_mapping
	;
	create temporary table stage_sku_barproduct_mapping
	diststyle all
	as 
	SELECT 	cast(cmac.matnr as varchar(30)) as material,
--			cmac.wrkst as gpp_code,
			'P' + SPLIT_PART(cmac.wrkst, '-', 4) as bar_product
	FROM 	bods.c11_0material_attr_current cmac 
	;
--	select 	* 
--	from 	stage_sku_barproduct_mapping
--	where material = 'CMAS261290'
--	;
	delete from ref_data.sku_barproduct_mapping
	;
	insert into ref_data.sku_barproduct_mapping (
				material,
				bar_product,
				start_date,
				end_date,
				current_flag,
				audit_loadts
		)
		select 	material,
				bar_product,
				cast('1900-01-01' as date) as start_date,
				cast('9999-12-31' as date) as end_date,
				1 current_flag,
				getdate() as audit_loadts
		from 	stage_sku_barproduct_mapping
	;
--	select 	* 
--	from 	ref_data.sku_barproduct_mapping
--	where material = 'CMAS261290'
--	;
	
	exception
		when others then raise info 'exception occur while ingesting data in ref_data.p_build_sku_barproduct_mapping';
end
$$
;

CREATE OR REPLACE PROCEDURE ref_data.p_build_sku_barproduct_mapping_c11_bods()
 LANGUAGE plpgsql
AS $$
BEGIN 
	
	/*  create mapping table for material -> bar_product from c11 raw boads data
	 * 	based on [bods.c11_0material_attr_current]
	 */
	drop table if exists stage_sku_barproduct_mapping
	;
	create temporary table stage_sku_barproduct_mapping
	diststyle all
	as 
	with cte_base as (	
	select cast(material as varchar(30)) as  material, 
		 cast(bar_product as varchar(30)) as  bar_product, 
		 cast((case when s.postdate = '' then null else postdate end) as date)  as postdate , 
		 sum(bar_amt) as dollartotal
	from bods.c11_0ec_pca3_current s
	inner join ref_data.entity rbh on s.bar_entity = rbh.name
	---only accounts thats contributes to sgm pnl structure
	inner join (select 	distinct bar_acct from 	ref_data.pnl_acct) acct on lower(s.bar_acct) = lower(acct.bar_acct) 
	where s.bar_acct is not null 
		and s.bar_entity is not null 
		and s.bar_acct <> ''
		and rbh.level4 = 'GTS_NA'	
		and material not in ('')
		and left(bar_product, 1) in ('P')
		and length(bar_product) = 6
		and bar_product not in ('P60999')
		--and bar_year >= 2019
		group by  cast(material as varchar(30)) , 
				cast(bar_product as varchar(30)) , 
				cast((case when s.postdate = '' then null else postdate end) as date)
		),cte_base_next as (
					select 	base.material,
							base.bar_product,
							base.postdate,
							lead(base.bar_product) over(partition by base.material order by base.postdate) as bar_product_next
					from 	cte_base base
				)
				,cte_base_historical as (
					select 	nxt.material,
							nxt.bar_product,
							nxt.postdate,
							nxt.bar_product_next,
							lead(nxt.postdate) over (partition by nxt.material order by nxt.postdate) as postdate_next,
							row_number() over (partition by nxt.material order by nxt.postdate) rnk
					from 	cte_base_next nxt
					where 	nxt.bar_product != nxt.bar_product_next or 
							nxt.bar_product_next is null
				)select 	hist.material,
						hist.bar_product,				
					case
						when hist.rnk = 1 then cast('1900-01-01' as date) 
						else cast(hist.postdate as date) 
					end as start_date,
					case
						when hist.bar_product_next is null then cast('9999-12-31' as date) 
						else cast(dateadd(day,-1,hist.postdate_next) as date)
					end as end_date,
					case when hist.bar_product_next is null then 1 else 0 end as current_flag,
					getdate() as audit_loadts
			from 	cte_base_historical as hist
		;
	
	delete from ref_data.sku_barproduct_mapping_c11_bods
	;
	insert into ref_data.sku_barproduct_mapping_c11_bods (
				material,
				bar_product,
				start_date,
				end_date,
				current_flag,
				audit_loadts
		)
		select 	material,
				bar_product,
				start_date,
				end_date,
				current_flag,
				getdate() as audit_loadts
		from 	stage_sku_barproduct_mapping
	;
	
--	select 	* 
--	from 	ref_data.sku_barproduct_mapping
--	where material = 'CMAS261290'
--	;
---0 overalaps	
--select count(1), material 
--from ref_data.sku_barproduct_mapping
--group by material 
--having count(1) >1;
	exception
		when others then raise info 'exception occur while ingesting data in ref_data.sku_barproduct_mapping_bods';
end
$$
;

CREATE OR REPLACE PROCEDURE ref_data.p_build_sku_barproduct_mapping_lawson_bods()
 LANGUAGE plpgsql
AS $$
BEGIN 
	
	/*  create mapping table for material -> bar_product from c11 raw boads data
	 * 	based on [bods.c11_0material_attr_current]
	 */
	drop table if exists stage_sku_barproduct_mapping
	;
	create temporary table stage_sku_barproduct_mapping
	diststyle all
	as 
	with cte_base as (	
	select case when cast(prod_cd as varchar(30)) = '' then null else cast(prod_cd as varchar(30)) end as  material, 
		 case when cast(s.bar_product  as varchar(22)) = '' then null else cast(s.bar_product  as varchar(22)) end as  bar_product, 
		 cast((case when s.post_dte = '' then null else s.post_dte end) as date)  as postdate , 
		 sum(bar_amt) as dollartotal
	from bods.lawson_mac_pl_trans_current  s
	inner join ref_data.entity rbh on s.bar_entity = rbh.name
	---only accounts thats contributes to sgm pnl structure
	inner join (select 	distinct bar_acct from 	ref_data.pnl_acct) acct on lower(s.bar_acct) = lower(acct.bar_acct) 
	where s.bar_acct is not null 
		and s.bar_entity is not null 
		and s.bar_acct <> ''
		and rbh.level4 = 'GTS_NA'	
		and material not in ('')
		and left(bar_product, 1) in ('P')
		and length(bar_product) = 6
		and bar_product not in ('P60999')
		--and bar_year >= 2019
		group by  case when cast(prod_cd as varchar(30)) = '' then null else cast(prod_cd as varchar(30)) end , 
				case when cast(s.bar_product  as varchar(22)) = '' then null else cast(s.bar_product  as varchar(22)) end, 
				cast((case when s.post_dte = '' then null else s.post_dte end) as date) 
		),cte_base_next as (
					select 	base.material,
							base.bar_product,
							base.postdate,
							lead(base.bar_product) over(partition by base.material order by base.postdate) as bar_product_next
					from 	cte_base base
				)
				,cte_base_historical as (
					select 	nxt.material,
							nxt.bar_product,
							nxt.postdate,
							nxt.bar_product_next,
							lead(nxt.postdate) over (partition by nxt.material order by nxt.postdate) as postdate_next,
							row_number() over (partition by nxt.material order by nxt.postdate) rnk
					from 	cte_base_next nxt
					where 	nxt.bar_product != nxt.bar_product_next or 
							nxt.bar_product_next is null
				)select 	hist.material,
						hist.bar_product,				
					case
						when hist.rnk = 1 then cast('1900-01-01' as date) 
						else cast(hist.postdate as date) 
					end as start_date,
					case
						when hist.bar_product_next is null then cast('9999-12-31' as date) 
						else cast(dateadd(day,-1,hist.postdate_next) as date)
					end as end_date,
					case when hist.bar_product_next is null then 1 else 0 end as current_flag,
					getdate() as audit_loadts
			from 	cte_base_historical as hist
		;
	
	delete from ref_data.sku_barproduct_mapping_lawson_bods
	;
	insert into ref_data.sku_barproduct_mapping_lawson_bods (
				material,
				bar_product,
				start_date,
				end_date,
				current_flag,
				audit_loadts
		)
		select 	material,
				bar_product,
				start_date,
				end_date,
				current_flag,
				getdate() as audit_loadts
		from 	stage_sku_barproduct_mapping
	;
	
--	select 	* 
--	from 	ref_data.sku_barproduct_mapping_lawson_bods
--	where material = '2112-704'
--	;
---0 overalaps	
--select count(1), material 
--from ref_data.sku_barproduct_mapping_lawson_bods
--group by material 
--having count(1) >1;
	exception
		when others then raise info 'exception occur while ingesting data in ref_data.sku_barproduct_mapping_bods';
end
$$
;

CREATE OR REPLACE PROCEDURE ref_data.p_build_sku_barproduct_mapping_p10_bods()
 LANGUAGE plpgsql
AS $$
BEGIN 
	
	/*  create mapping table for material -> bar_product from c11 raw boads data
	 * 	based on [bods.c11_0material_attr_current]
	 */
	drop table if exists stage_sku_barproduct_mapping
	;
	create temporary table stage_sku_barproduct_mapping
	diststyle all
	as 
	with cte_base as (	
	select case when cast(prod_cd as varchar(30)) = '' then null else cast(prod_cd as varchar(30)) end as  material, 
		 case when cast(s.bar_product  as varchar(22)) = '' then null else cast(s.bar_product  as varchar(22)) end as  bar_product, 
		 cast(s.cpudt as date)  as postdate , 
		 sum(bar_amt) as dollartotal
	from bods.p10_0ec_pca_3_trans_current s
	inner join ref_data.entity rbh on s.bar_entity = rbh.name
	---only accounts thats contributes to sgm pnl structure
	inner join (select 	distinct bar_acct from 	ref_data.pnl_acct) acct on lower(s.bar_acct) = lower(acct.bar_acct) 
	where s.bar_acct is not null 
		and s.bar_entity is not null 
		and s.bar_acct <> ''
		and rbh.level4 = 'GTS_NA'	
		and material not in ('')
		and left(bar_product, 1) in ('P')
		and length(bar_product) = 6
		and bar_product not in ('P60999')
		--and bar_year >= 2019
		group by  case when cast(prod_cd as varchar(30)) = '' then null else cast(prod_cd as varchar(30)) end , 
				case when cast(s.bar_product  as varchar(22)) = '' then null else cast(s.bar_product  as varchar(22)) end, 
				cast(s.cpudt as date)
		),cte_base_next as (
					select 	base.material,
							base.bar_product,
							base.postdate,
							lead(base.bar_product) over(partition by base.material order by base.postdate) as bar_product_next
					from 	cte_base base
				)
				,cte_base_historical as (
					select 	nxt.material,
							nxt.bar_product,
							nxt.postdate,
							nxt.bar_product_next,
							lead(nxt.postdate) over (partition by nxt.material order by nxt.postdate) as postdate_next,
							row_number() over (partition by nxt.material order by nxt.postdate) rnk
					from 	cte_base_next nxt
					where 	nxt.bar_product != nxt.bar_product_next or 
							nxt.bar_product_next is null
				)select 	hist.material,
						hist.bar_product,				
					case
						when hist.rnk = 1 then cast('1900-01-01' as date) 
						else cast(hist.postdate as date) 
					end as start_date,
					case
						when hist.bar_product_next is null then cast('9999-12-31' as date) 
						else cast(dateadd(day,-1,hist.postdate_next) as date)
					end as end_date,
					case when hist.bar_product_next is null then 1 else 0 end as current_flag,
					getdate() as audit_loadts
			from 	cte_base_historical as hist
		;
	
	delete from ref_data.sku_barproduct_mapping_p10_bods
	;
	insert into ref_data.sku_barproduct_mapping_p10_bods (
				material,
				bar_product,
				start_date,
				end_date,
				current_flag,
				audit_loadts
		)
		select 	material,
				bar_product,
				start_date,
				end_date,
				current_flag,
				getdate() as audit_loadts
		from 	stage_sku_barproduct_mapping
	;
	
--	select 	* 
--	from 	ref_data.sku_barproduct_mapping_p10_bods
--	where material = '3-203-156-41'
--	;
---0 overalaps	
--select count(1), material 
--from ref_data.sku_barproduct_mapping_p10_bods
--group by material 
--having count(1) >1;
	exception
		when others then raise info 'exception occur while ingesting data in ref_data.sku_barproduct_mapping_bods';
end
$$
;

CREATE OR REPLACE PROCEDURE ref_data.p_build_sku_brand_mapping_masterdata()
 LANGUAGE plpgsql
AS $$
BEGIN 
    /*
        ref_data.p_build_sku_brand_mapping_masterdata();
        select * from finance_stage.core_tran_delta_agg where source_system = 'E0194' limit 10;
        select count(*) from finance_stage.core_tran_delta_agg where source_system = 'E0194' and fiscal_month_id = 202012;
     */
    
    drop table if exists stage_sku_brand_mapping
    ;
    create temporary table stage_sku_brand_mapping as 
        SELECT  distinct 
                cast(cmac.matnr as varchar(30)) as material,
                cmac.matkl as brand_code,
                brnd.wgbez as brand_map
        FROM    sapc11.mara_current cmac 
                left join sapc11.t023t_current brnd 
                    on  cmac.matkl = brnd.matkl 
                    and brnd.spras = 'E'
                    and brnd.wgbez is not null
    ;
    delete from ref_data.sku_brand_mapping_masterdata;
    insert into ref_data.sku_brand_mapping_masterdata (
                material,
                brand_code,
                brand_map
        )
        select  material,
                brand_code,
                brand_map
        FROM    stage_sku_brand_mapping
    ;
	exception
		when others then raise info 'exception occur while ingesting data in ref_data.p_build_sku_brand_mapping_masterdata';
end
$$
;

CREATE OR REPLACE PROCEDURE ref_data.p_build_sku_gpp_mapping()
 LANGUAGE plpgsql
AS $$
BEGIN 
	
	/*  create mapping table for material -> gpp_portfolio/division
	 */
	drop table if exists stage_sku_gpp_mapping
	;
	create temporary table stage_sku_gpp_mapping
	diststyle all
	as 
	SELECT 	cast(cmac.matnr as varchar(30)) as material,
			cmac.wrkst as gpp_code,
			'P' + SPLIT_PART(cmac.wrkst, '-', 4) as gpp_portfolio,
			SPLIT_PART(cmac.wrkst, '-', 2) as gpp_division
	FROM 	sapc11.mara_current cmac 
	WHERE 	'P' + SPLIT_PART(cmac.wrkst, '-', 4) is not null
	;
	delete from ref_data.sku_gpp_mapping
	;
	insert into ref_data.sku_gpp_mapping (
				material,
				gpp_portfolio,
				gpp_division,
				start_date,
				end_date,
				current_flag,
				audit_loadts
		)
		select 	material,
				gpp_portfolio,
				gpp_division,
				cast('1900-01-01' as date) as start_date,
				cast('9999-12-31' as date) as end_date,
				1 current_flag,
				getdate() as audit_loadts
		from 	stage_sku_gpp_mapping
	;
	
	exception
		when others then raise info 'exception occur while ingesting data in ref_data.p_build_sku_barproduct_mapping';
end
$$
;

CREATE OR REPLACE PROCEDURE ref_data.p_build_sku_gpp_mapping_sgm(fmthid integer)
 LANGUAGE plpgsql
AS $$
BEGIN 
	/*
	 * 		drop procedure ref_data.p_build_sku_gpp_mapping_sgm(fmthid integer)
	 * 		delete from ref_data.sku_gpp_mapping_sgm;
	 * 		call ref_data.p_build_sku_gpp_mapping_sgm(202101)
	 * 		select count(*) from ref_data.sku_gpp_mapping_sgm;
	 * 		select distinct ss_fiscal_month_id from ref_data.sku_gpp_mapping_sgm;
	 * 		grant execute on procedure ref_data.p_build_sku_gpp_mapping_sgm(fmthid integer) to group "g-ada-rsabible-sb-ro";
	 */
	
	/*
	 *		This procedure creates a custom mapping: sku -> bar_product (aka gpp portfolio) 
	 *		
	 *		The final table is a snapshot by fiscal month where each material
	 *		is mapped to a single bar_product based on the highest invoice sales 
	 *		transactions (A40110) from the beginning of time up to the current
	 *		fiscal period.
	 *
	 * 		Final Table(s): 
	 *			ref_data.sku_gpp_mapping_sgm
	 *
	 */
	
	/*  create mapping table for material -> gpp_portfolio/division
	 */
	drop table if exists stage_sku_gpp_mapping_sgm
	;
	create temporary table stage_sku_gpp_mapping_sgm
	diststyle all
	as 
	with
		cte_base as (	
			SELECT 	fmthid as ss_fiscal_month_id,
					dp.material,
					dp.bar_product,
					sum(f.amt_usd) as total_amt_usd
			FROM 	dw.dim_product AS dp
					INNER JOIN dw.fact_pnl_commercial_stacked AS f
						ON 	f.product_key = dp.product_key
			WHERE 	lower(dp.material) not in ('unknown') and 
					f.bar_acct = 'A40110' and 
					f.fiscal_month_id <= fmthid
			group by dp.material,
					dp.bar_product
		),
		cte_rank as (
			select 	base.ss_fiscal_month_id,
					base.material,
					base.bar_product,
					base.total_amt_usd,
					rank() over(
						partition by base.material
						order by base.total_amt_usd desc, base.bar_product
					) as rnk
			from 	cte_base as base
		)
		select 	rnk.ss_fiscal_month_id,
				rnk.material,
				rnk.bar_product
		from 	cte_rank as rnk
		where 	rnk.rnk = 1
	;
	/* Create Master Mapping Table for SKU -> GPP Portfolio */
	drop table if exists _master_mapping_sku_to_portfolio
	;
	create temporary table _master_mapping_sku_to_portfolio as 
	
		select 	distinct material, gpp_portfolio
		from 	ref_data.sku_gpp_mapping as map_md
		union all
		select 	distinct material, gpp_portfolio
		from 	(
					select 	distinct 
							map_filler.material, 
							map_filler.bar_product as gpp_portfolio
					from 	stage_sku_gpp_mapping_sgm as map_filler
							left outer join ref_data.sku_gpp_mapping mm1
								on 	lower(mm1.material) = lower(map_filler.material)
					where 	mm1.material is null
				)
	;
	delete 
	from 	ref_data.sku_gpp_mapping_sgm
	where 	ss_fiscal_month_id = fmthid
	;
	insert into ref_data.sku_gpp_mapping_sgm (
				ss_fiscal_month_id,
				material,
				gpp_portfolio,
				audit_loadts
		)
		select 	fmthid as ss_fiscal_month_id,
				stg.material,
				stg.gpp_portfolio,
				getdate() as audit_loadts
		from 	_master_mapping_sku_to_portfolio stg
	;
	
	exception
		when others then raise info 'exception occur while ingesting data in ref_data.p_build_sku_gpp_mapping_sgm';
end
$$
;

CREATE OR REPLACE PROCEDURE ref_data.p_build_soldto_barcust_mapping()
 LANGUAGE plpgsql
AS $$
--DECLARE variables
BEGIN  
	
	drop table if exists stage_soldtocust_barcust_mapping;
	
	create temporary table stage_soldtocust_barcust_mapping
	diststyle all
	as 
		with 
			cte_base as (	
				select	distinct 
						cast(lower(soldtocust) as varchar(22)) as soldtocust,
						cast(lower(bar_custno) as varchar(30)) as bar_custno,
						cast((case when s.postdate = '' then null else postdate end) as date)  as postdate
				from 	bods.c11_0ec_pca3_current s
				inner join ref_data.entity rbh on s.bar_entity = rbh.name
						---only accounts thats contributes to sgm pnl structure
						inner join (
							select 	distinct bar_acct 
							from 	ref_data.pnl_acct
						) acct 
							on 	lower(s.bar_acct) = lower(acct.bar_acct) 
				where s.bar_acct is not null 
					and s.bar_entity is not null 
					and s.bar_acct <> ''
					and rbh.level4 = 'GTS_NA'
--					and s.bar_bu in ('GTS')
--					and s.bar_acct not in ('IGNORE')
--					and s.bar_currtype in ('USD' ,'CAD')
					and case when cast(soldtocust as varchar(30)) = '' then null else  cast(soldtocust as varchar(30)) end is not null 
					and case when cast(bar_custno as varchar(20)) = '' then null else cast(bar_custno as varchar(20)) end is not null
					and cast((case when s.postdate = '' then null else postdate end) as date)  >= cast('2018-12-30' as date) 
					--and cast('2021-12-01' as date) 
		
					-- TESTING
					--and material = 'CMAS261290'
			)
			,cte_base_next as (
				select 	base.soldtocust,
						base.bar_custno,
						base.postdate,
						lead(base.bar_custno) over(partition by base.soldtocust order by base.postdate) as bar_custno_next
				from 	cte_base base
			)
			,cte_base_historical as (
				select 	nxt.soldtocust,
						nxt.bar_custno,
						nxt.postdate,
						
						nxt.bar_custno_next,
						lead(nxt.postdate) over (partition by nxt.soldtocust order by nxt.postdate) as postdate_next,
						row_number() over (partition by nxt.soldtocust order by nxt.postdate) rnk
				from 	cte_base_next nxt
				where 	nxt.bar_custno != nxt.bar_custno_next or 
						nxt.bar_custno_next is null
			)
		select 	hist.soldtocust,
				hist.bar_custno,				
				case
					when hist.rnk = 1 then cast('1900-01-01' as date) 
					else cast(hist.postdate as date) 
				end as start_date,
				
				case
					when hist.bar_custno_next is null then cast('9999-12-31' as date) 
					else cast(dateadd(day,-1,hist.postdate_next) as date)
				end as end_date,
				
				case when hist.bar_custno_next is null then 1 else 0 end as current_flag,
					
				getdate() as audit_loadts
					
		from 	cte_base_historical as hist
	;

--Select soldtocust, count(1)
--from stage_soldtocust_barcust_mapping
--where current_flag =1 
--group by soldtocust 
--having count(1) >1 ;
----
--Select *
--from stage_soldtocust_barcust_mapping
--Where soldtocust ='0000008120';
	raise info 'insert into ref_data.soldto_barcust_mapping';
	truncate table ref_data.soldto_barcust_mapping;
	insert into ref_data.soldto_barcust_mapping (
				soldtocust,
				bar_custno,
				start_date,
				end_date,
				current_flag,
				audit_loadts
		)
		select 	soldtocust,
				bar_custno,
				start_date,
				end_date,
				current_flag,
				getdate() as audit_loadts
		from 	stage_soldtocust_barcust_mapping;
exception
when others then raise exception 'exception occur while ingesting in ref_data.soldto_barcust_mapping';
end;
$$
;

CREATE OR REPLACE PROCEDURE ref_data.p_build_soldto_shipto_barcust_mapping()
 LANGUAGE plpgsql
AS $$
--DECLARE variables
BEGIN  
	
	drop table if exists stage_soldtocust_shiptocust_barcust_mapping;
	create temporary table stage_soldtocust_shiptocust_barcust_mapping
	diststyle all
	as 
		with 
			cte_base as (	
				select	distinct 
						cast(lower(soldtocust) as varchar(22)) as soldtocust,
						cast(lower(shiptocust) as varchar(22)) as shiptocust,
						cast(lower(bar_custno) as varchar(30)) as bar_custno,
						cast((case when s.postdate = '' then null else postdate end) as date)  as postdate
				from 	bods.c11_0ec_pca3_current s
				inner join ref_data.entity rbh on s.bar_entity = rbh.name
						---only accounts thats contributes to sgm pnl structure
						inner join (
							select 	distinct bar_acct 
							from 	ref_data.pnl_acct
						) acct 
							on 	lower(s.bar_acct) = lower(acct.bar_acct) 
				where s.bar_acct is not null 
					and s.bar_entity is not null 
					and s.bar_acct <> ''
					and rbh.level4 = 'GTS_NA'
--					and s.bar_bu in ('GTS')
--					and s.bar_acct not in ('IGNORE')
--					and s.bar_currtype in ('USD' ,'CAD')
					and case when cast(soldtocust as varchar(50)) = '' then null else  cast(soldtocust as varchar(50)) end is not null 
					and case when cast(shiptocust as varchar(50)) = '' then null else  cast(shiptocust as varchar(50)) end is not null 
					and case when cast(bar_custno as varchar(50)) = '' then null else cast(bar_custno as varchar(50)) end is not null
					and cast((case when s.postdate = '' then null else postdate end) as date)  >= cast('2018-12-30' as date) 
					--and cast('2021-12-01' as date) 
		
					-- TESTING
					--and material = 'CMAS261290'
			)
			,cte_base_next as (
				select 	base.soldtocust,
						base.shiptocust,
						base.bar_custno,
						base.postdate,
						lead(base.bar_custno) over(partition by base.soldtocust, base.shiptocust order by base.postdate) as bar_custno_next
				from 	cte_base base
			)
			,cte_base_historical as (
				select 	nxt.soldtocust,
						nxt.shiptocust,
						nxt.bar_custno,
						nxt.postdate,
						
						nxt.bar_custno_next,
						lead(nxt.postdate) over (partition by nxt.soldtocust, nxt.shiptocust order by nxt.postdate) as postdate_next,
						row_number() over (partition by nxt.soldtocust order by nxt.postdate) rnk
				from 	cte_base_next nxt
				where 	nxt.bar_custno != nxt.bar_custno_next or 
						nxt.bar_custno_next is null
			)
		select 	hist.soldtocust,
				hist.shiptocust,
				hist.bar_custno,				
				case
					when hist.rnk = 1 then cast('1900-01-01' as date) 
					else cast(hist.postdate as date) 
				end as start_date,
				
				case
					when hist.bar_custno_next is null then cast('9999-12-31' as date) 
					else cast(dateadd(day,-1,hist.postdate_next) as date)
				end as end_date,
				
				case when hist.bar_custno_next is null then 1 else 0 end as current_flag,
					
				getdate() as audit_loadts
					
		from 	cte_base_historical as hist
	;

--Select soldtocust, count(1)
--from stage_soldtocust_barcust_mapping
--where current_flag =1 
--group by soldtocust 
--having count(1) >1 ;
----
--Select *
--from stage_soldtocust_shiptocust_barcust_mapping
--Where soldtocust ='0001077198';
--Select *
--from stage_soldtocust_shiptocust_barcust_mapping
--where post
	raise info 'insert into ref_data.soldto_shipto_barcust_mapping';
	truncate table ref_data.soldto_shipto_barcust_mapping;
	insert into ref_data.soldto_shipto_barcust_mapping (
				soldtocust,
				shiptocust,
				bar_custno,
				start_date,
				end_date,
				current_flag,
				audit_loadts
		)
		select 	soldtocust,
				shiptocust,
				bar_custno,
				start_date,
				end_date,
				current_flag,
				getdate() as audit_loadts
		from 	stage_soldtocust_shiptocust_barcust_mapping;
exception
when others then raise exception 'exception occur while ingesting in ref_data.soldto_shipto_barcust_mapping';
end;
$$
;

CREATE OR REPLACE PROCEDURE ref_data.umm_closedate_config()
 LANGUAGE plpgsql
AS $$
--call dw.p_build_dim_business_unit (1)
BEGIN
	
	--comment
	delete  from ref_data.umm_closedate_config; 

	drop table if exists stage_umm_closedate_config; 
	create temporary table stage_umm_closedate_config
	diststyle all
	as 
	Select row_number() over (order by fmth_id) as rownumber,
		  fmth_id as fiscal_month_id,
		  min(dy_dte) as fiscal_close_date, 
		  max(dy_dte) as fiscal_month_enddate,
		  dateadd(day,7,min(dy_dte)) as fiscal_wklyjob_start_date,
		  dateadd(day,5,min(dy_dte)) as finance_close_date 
	from ref_data.calendar 
	where fmth_id >= 201901
	group by fmth_id;

	insert into ref_data.umm_closedate_config
	select * from stage_umm_closedate_config;
	EXCEPTION
		when others then raise info 'exception occur while ingesting data in ref_data.umm_closedate_config';
END

$$
;
