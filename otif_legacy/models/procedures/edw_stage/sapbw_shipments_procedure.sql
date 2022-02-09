CREATE OR REPLACE PROCEDURE edw_stage.sapbw_shipments()
	LANGUAGE plpgsql
AS $$
	
	

begin

insert into edw.otif.edw_shipments
    (
source_schema,
order_number ,
bol_number ,
scac ,
completion_dt ,
early_delivery_dt ,
planned_dt ,
staged_dt ,
loaded_dt ,
documented_dt ,
plt_last_staged_dt ,
rpk_first_picked_dt ,
rpk_last_sealed_dt ,
carrier_name ,
 sap_delivery,
     sap_delivery_type,
     sap_plant,
     sap_route,
     sap_createdondate,
     sap_createdontime,
     sap_plandate,
     sap_pickingdate,
     sap_loadingdate,
     sap_deliverydate,
     sap_transpdate,
     sap_actgoodsissue,
     line_number,
     etl_crte_user,
     etl_crte_ts

     )

 SELECT      'WMS_BW' as source_schema,
            256||trim( leading '0' from DELIV_NUMB)  as order_number,
            ZI_USBOL as bol_number,
            ZI_TRANMD as scac,
            ZI_CONFDT||':'||ZI_CONFTM as completion_dt,
            ZI_ESDLDT as early_delivery_dt,
            Z_PLANDT||':'||Z_PLANTM as planned_dt,
            Z_LASTGDT||':'||z_lastgtm as staged_dt,
            Z_LOADEDT||':'||Z_LOADETM as loaded_dt,
            Z_DOCUMDT||':'||Z_DOCUMTM as documented_dt,
            Z_LASTGDT||':'||z_lastgtm as plt_last_staged_dt,
            Z_1STPICD||':'||Z_1STPICT as rpk_first_picked_dt,
            Z_LASTGDT||':'||Z_LASTGTM  as rpk_last_sealed_dt,
            ZI_CARPNT as carrier_name,
            lip.vbeln as sap_delivery,
            lip.lfart as sap_delivery_type,
            lip.werks  as sap_plant,
            lip.route  as sap_route,
            lip.erdat  as sap_createdondate,
            lip.erzet as sap_createdontime,
            lip.wadat as sap_plandate,
            lip.kodat as sap_pickingdate,
            lip.lddat as sap_loadingdate,
            lip.lfdat as sap_deliverydate,
            lip.tddat as sap_transpdate,
            lip.wadat_ist as sap_actgoodsissue,
            lis.posnr as line_number,		
            'ETL_USER' as etl_crte_user,
            current_timestamp etl_crte_ts      
	 from  temp.z_slo004 zs 
        left join  sapc11.likp_current lip
           on zs.DELIV_NUMB = lip.vbeln
        left join sapc11.lips_current lis
          on zs.DELIV_NUMB = lis.vbeln
       where order_number not in ( select order_number from otif.edw_shipments);
end;


$$
;