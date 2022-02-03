{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS bods.c11_0ec_pca3_current_t1
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   
	,eventdts VARCHAR(65535)   
	,rec_src VARCHAR(65535)   
	,row_sqn BIGINT   
	,hash_full_record VARCHAR(65535)   
	,id BIGINT   
	,"year" VARCHAR(65535)   
	,period VARCHAR(65535)   
	,cocode VARCHAR(65535)   
	,busarea VARCHAR(65535)   
	,costctr VARCHAR(65535)   
	,acct VARCHAR(65535)   
	,int_entitytype VARCHAR(65535)   
	,int_functype VARCHAR(65535)   
	,docct VARCHAR(65535)   
	,docno VARCHAR(65535)   
	,docline VARCHAR(65535)   
	,curtype VARCHAR(65535)   
	,postdate VARCHAR(65535)   
	,salesgrp VARCHAR(65535)   
	,salesoff VARCHAR(65535)   
	,material VARCHAR(65535)   
	,payer VARCHAR(65535)   
	,shiptocust VARCHAR(65535)   
	,soldtocust VARCHAR(65535)   
	,currkey VARCHAR(65535)   
	,amt NUMERIC(38,10)   
	,bar_acct VARCHAR(65535)   
	,bar_function VARCHAR(65535)   
	,bar_entity VARCHAR(65535)   
	,bar_shipto VARCHAR(65535)   
	,bar_product VARCHAR(65535)   
	,bar_brand VARCHAR(65535)   
	,bar_custno VARCHAR(65535)   
	,bar_scenario VARCHAR(65535)   
	,bar_year VARCHAR(65535)   
	,bar_period VARCHAR(65535)   
	,bar_currtype VARCHAR(65535)   
	,bar_amt NUMERIC(38,10)   
	,bar_bu VARCHAR(65535)   
	,quanunit VARCHAR(65535)   
	,quantity NUMERIC(38,10)   
	,werks VARCHAR(65535)   
	,rprctr VARCHAR(65535)   
	,brand_code VARCHAR(65535)   
	,bwtar VARCHAR(65535)   
	,class_code VARCHAR(65535)   
	,credit NUMERIC(38,10)   
	,debit NUMERIC(38,10)   
	,distribution_channel VARCHAR(65535)   
	,hierarchy VARCHAR(65535)   
	,poper NUMERIC(38,10)   
	,product VARCHAR(65535)   
	,refactiv VARCHAR(65535)   
	,refdocct VARCHAR(65535)   
	,refdocline VARCHAR(65535)   
	,refdocln VARCHAR(65535)   
	,refdocnr VARCHAR(65535)   
	,refdocnum VARCHAR(65535)   
	,refryear VARCHAR(65535)   
	,rtcur VARCHAR(65535)   
	,salesgrp_lkp VARCHAR(65535)   
	,salesoff_lkp VARCHAR(65535)   
	,salesorg VARCHAR(65535)   
	,zzdmdgroup VARCHAR(65535)   
	,zzhier VARCHAR(65535)   
	,zzitmcat VARCHAR(65535)   
	,zzorigsorg VARCHAR(65535)   
	,zzpayer VARCHAR(65535)   
	,zzshipto VARCHAR(65535)   
	,zzsoldto VARCHAR(65535)   
	,src_id VARCHAR(65535)   
	,rvers VARCHAR(65535)   
	,rhoart NUMERIC(38,10)   
	,rfarea VARCHAR(65535)   
	,kokrs VARCHAR(65535)   
	,hrkft VARCHAR(65535)   
	,rassc VARCHAR(65535)   
	,eprctr VARCHAR(65535)   
	,activ VARCHAR(65535)   
	,afabe NUMERIC(38,10)   
	,oclnt NUMERIC(38,10)   
	,sbukrs VARCHAR(65535)   
	,sprctr VARCHAR(65535)   
	,shoart NUMERIC(38,10)   
	,sfarea VARCHAR(65535)   
	,cpudt VARCHAR(65535)   
	,cputm VARCHAR(65535)   
	,usnam VARCHAR(65535)   
	,sgtxt VARCHAR(65535)   
	,autom VARCHAR(65535)   
	,docty VARCHAR(65535)   
	,bldat VARCHAR(65535)   
	,wsdat VARCHAR(65535)   
	,awtyp VARCHAR(65535)   
	,aworg VARCHAR(65535)   
	,lstar VARCHAR(65535)   
	,aufnr VARCHAR(65535)   
	,aufpl VARCHAR(65535)   
	,anln1 VARCHAR(65535)   
	,anln2 VARCHAR(65535)   
	,bwkey VARCHAR(65535)   
	,anbwa VARCHAR(65535)   
	,lifnr VARCHAR(65535)   
	,rmvct VARCHAR(65535)   
	,ebeln VARCHAR(65535)   
	,ebelp VARCHAR(65535)   
	,kstrg VARCHAR(65535)   
	,erkrs VARCHAR(65535)   
	,paobjnr VARCHAR(65535)   
	,pasubnr NUMERIC(38,10)   
	,ps_psp_pnr NUMERIC(38,10)   
	,kdauf VARCHAR(65535)   
	,kdpos VARCHAR(65535)   
	,fkart VARCHAR(65535)   
	,aubel VARCHAR(65535)   
	,aupos VARCHAR(65535)   
	,spart VARCHAR(65535)   
	,vbeln VARCHAR(65535)   
	,posnr VARCHAR(65535)   
	,vbund VARCHAR(65535)   
	,logsys VARCHAR(65535)   
	,alebn VARCHAR(65535)   
	,awsys VARCHAR(65535)   
	,versa VARCHAR(65535)   
	,stflg VARCHAR(65535)   
	,stokz VARCHAR(65535)   
	,rep_matnr VARCHAR(65535)   
	,co_prznr VARCHAR(65535)   
	,imkey VARCHAR(65535)   
	,dabrz VARCHAR(65535)   
	,valut VARCHAR(65535)   
	,rscope VARCHAR(65535)   
	,awref_rev VARCHAR(65535)   
	,aworg_rev VARCHAR(65535)   
	,bwart VARCHAR(65535)   
	,blart BIGINT   
	,timestmp VARCHAR(65535)   
	,valuetype NUMERIC(38,10)   
	,chartaccts VARCHAR(65535)   
	,upmod VARCHAR(65535)   
	,valutyp VARCHAR(65535)   
	,runid BIGINT   
	,loaddatetime VARCHAR(65535)   
) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}