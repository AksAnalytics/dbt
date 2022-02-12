
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