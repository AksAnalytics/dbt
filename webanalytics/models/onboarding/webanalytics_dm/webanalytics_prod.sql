-- DROP SCHEMA otifmart;

CREATE SCHEMA otifmart;

-- Drop table

-- DROP TABLE web_analytics_dm.dim_calendar;

--DROP TABLE web_analytics_dm.dim_calendar;
CREATE TABLE IF NOT EXISTS web_analytics_dm.dim_calendar
(
	id INTEGER   ENCODE RAW
	,date DATE   ENCODE az64
	,"year" SMALLINT   ENCODE az64
	,"month" SMALLINT   ENCODE az64
	,"day" SMALLINT   ENCODE az64
	,quarter SMALLINT   ENCODE az64
	,week SMALLINT   ENCODE az64
	,day_name VARCHAR(9)   ENCODE lzo
	,month_name VARCHAR(9)   ENCODE lzo
	,weekend_flag BOOLEAN   ENCODE RAW
)
DISTSTYLE ALL
 SORTKEY (
	id
	)
;
ALTER TABLE web_analytics_dm.dim_calendar owner to base_admin;

-- Drop table

-- DROP TABLE web_analytics_dm.dm_behaviour_campaign_google_ad;

--DROP TABLE web_analytics_dm.dm_behaviour_campaign_google_ad;
CREATE TABLE IF NOT EXISTS web_analytics_dm.dm_behaviour_campaign_google_ad
(
	clicks BIGINT   ENCODE az64
	,conversions NUMERIC(38,10)   ENCODE az64
	,impressions BIGINT   ENCODE az64
	,interactions BIGINT   ENCODE az64
	,business_unit VARCHAR(256)   ENCODE lzo
	,campaign VARCHAR(256)   ENCODE lzo
	,campaign_type VARCHAR(256)   ENCODE lzo
	,campaign_status VARCHAR(256)   ENCODE lzo
	,channel_type VARCHAR(256)   ENCODE lzo
	,country VARCHAR(256)   ENCODE lzo
	,date VARCHAR(100)   ENCODE lzo
	,device VARCHAR(256)   ENCODE lzo
	,"network" VARCHAR(256)   ENCODE lzo
	,campaign_cost NUMERIC(38,10)   ENCODE az64
	,cost_per_conversion NUMERIC(38,10)   ENCODE az64
	,averagecpc NUMERIC(38,10)   ENCODE az64
	,averagecpm NUMERIC(38,10)   ENCODE az64
	,budget NUMERIC(38,10)   ENCODE az64
	,client_id VARCHAR(256)   ENCODE lzo
	,session_id VARCHAR(256)   ENCODE lzo
	,hit_timestamp VARCHAR(100)   ENCODE lzo
	,etl_crte_user VARCHAR(100)   ENCODE lzo
	,etl_crte_ts VARCHAR(100)   ENCODE lzo
	,etl_updt_user VARCHAR(100)   ENCODE lzo
	,etl_updt_ts VARCHAR(100)   ENCODE lzo
)
DISTSTYLE EVEN
;
ALTER TABLE web_analytics_dm.dm_behaviour_campaign_google_ad owner to base_admin;

-- Drop table

-- DROP TABLE web_analytics_dm.dm_behaviour_campaign_salesforce;

--DROP TABLE web_analytics_dm.dm_behaviour_campaign_salesforce;
CREATE TABLE IF NOT EXISTS web_analytics_dm.dm_behaviour_campaign_salesforce
(
	sqls INTEGER   ENCODE az64
	,opportunity_status VARCHAR(500)   ENCODE lzo
	,attendees VARCHAR(500)   ENCODE lzo
	,channel_name VARCHAR(500)   ENCODE lzo
	,transaction_id VARCHAR(500)   ENCODE lzo
	,event_name VARCHAR(500)   ENCODE lzo
	,events_mode VARCHAR(500)   ENCODE lzo
	,event_type VARCHAR(500)   ENCODE lzo
	,aov_mql NUMERIC(38,10)   ENCODE az64
	,aov_sql NUMERIC(38,10)   ENCODE az64
	,budget NUMERIC(38,10)   ENCODE az64
	,cost NUMERIC(38,10)   ENCODE az64
	,spent NUMERIC(38,10)   ENCODE az64
	,country VARCHAR(500)   ENCODE lzo
	,lead_score NUMERIC(38,10)   ENCODE az64
	,account_name VARCHAR(500)   ENCODE lzo
	,lead_status VARCHAR(500)   ENCODE lzo
	,date VARCHAR(500)   ENCODE lzo
	,sites VARCHAR(500)   ENCODE lzo
	,business_unit VARCHAR(500)   ENCODE lzo
	,campaign_name VARCHAR(500)   ENCODE lzo
	,campaign_status VARCHAR(500)   ENCODE lzo
	,campaign_type VARCHAR(500)   ENCODE lzo
	,"region" VARCHAR(500)   ENCODE lzo
	,channel_type VARCHAR(500)   ENCODE lzo
	,is_campaign BOOLEAN   ENCODE RAW
	,etl_crte_user VARCHAR(100)   ENCODE lzo
	,etl_crte_ts VARCHAR(100)   ENCODE lzo
	,etl_updt_user VARCHAR(100)   ENCODE lzo
	,etl_updt_ts VARCHAR(100)   ENCODE lzo
	,curr_actual_conversin_rate NUMERIC(38,10)   ENCODE az64
	,curr_forecast_conversion_rate NUMERIC(38,10)   ENCODE az64
	,cost_actual_ammount_usd NUMERIC(38,10)   ENCODE az64
	,cost_forecast_ammount_usd NUMERIC(38,10)   ENCODE az64
	,spent_actual_ammount_usd NUMERIC(38,10)   ENCODE az64
	,spent_forecast_ammount_usd NUMERIC(38,10)   ENCODE az64
	,budget_actual_ammount_usd NUMERIC(38,10)   ENCODE az64
	,budget_forecast_ammount_usd NUMERIC(38,10)   ENCODE az64
	,campaign_category VARCHAR(500)   ENCODE lzo
	,lead_status_grp VARCHAR(500)   ENCODE lzo
	,opportunity_status_grp VARCHAR(500)   ENCODE lzo
	,opportunity_id VARCHAR(500)   ENCODE lzo
	,opportunity_value NUMERIC(38,10)   ENCODE az64
	,won_value NUMERIC(38,10)   ENCODE az64
	,curr_code VARCHAR(500)   ENCODE lzo
	,mqls INTEGER   ENCODE az64
)
DISTSTYLE EVEN
;
ALTER TABLE web_analytics_dm.dm_behaviour_campaign_salesforce owner to base_admin;

-- Drop table

-- DROP TABLE web_analytics_dm.dm_behaviour_web;

--DROP TABLE web_analytics_dm.dm_behaviour_web;
CREATE TABLE IF NOT EXISTS web_analytics_dm.dm_behaviour_web
(
	exit VARCHAR(3000)   ENCODE lzo
	,search VARCHAR(3000)   ENCODE lzo
	,search_refinement VARCHAR(3000)   ENCODE lzo
	,search_depth VARCHAR(3000)   ENCODE lzo
	,session_duration INTEGER   ENCODE az64
	,time_on_page INTEGER   ENCODE az64
	,entrance VARCHAR(3000)   ENCODE lzo
	,pageviews INTEGER   ENCODE az64
	,results_page_views VARCHAR(3000)   ENCODE lzo
	,time_after_search VARCHAR(3000)   ENCODE lzo
	,total_unique_searches VARCHAR(3000)   ENCODE lzo
	,unique_page_views VARCHAR(3000)   ENCODE lzo
	,users VARCHAR(3000)   ENCODE lzo
	,medium VARCHAR(3000)   ENCODE lzo
	,visits VARCHAR(3000)   ENCODE lzo
	,age_bracket VARCHAR(3000)   ENCODE lzo
	,browser VARCHAR(3000)   ENCODE lzo
	,business_units VARCHAR(3000)   ENCODE lzo
	,channel_name VARCHAR(3000)   ENCODE lzo
	,division_nm VARCHAR(3000)   ENCODE lzo
	,brand_nm VARCHAR(3000)   ENCODE lzo
	,country VARCHAR(3000)   ENCODE lzo
	,date VARCHAR(3000)   ENCODE lzo
	,device VARCHAR(3000)   ENCODE lzo
	,hostname VARCHAR(3000)   ENCODE lzo
	,landing_page VARCHAR(3000)   ENCODE lzo
	,landing_page_trim VARCHAR(3000)   ENCODE lzo
	,"language" VARCHAR(3000)   ENCODE lzo
	,page VARCHAR(3000)   ENCODE lzo
	,page_trim VARCHAR(3000)   ENCODE lzo
	,"region" VARCHAR(3000)   ENCODE lzo
	,query VARCHAR(3000)   ENCODE lzo
	,search_term VARCHAR(3000)   ENCODE lzo
	,industry_desc VARCHAR(3000)   ENCODE lzo
	,industry_grp VARCHAR(3000)   ENCODE lzo
	,fullvisitorid VARCHAR(3000)   ENCODE lzo
	,sites VARCHAR(3000)   ENCODE lzo
	,site_url VARCHAR(3000)   ENCODE lzo
	,hit_number INTEGER   ENCODE az64
	,source VARCHAR(3000)   ENCODE lzo
	,top_next_page VARCHAR(3000)   ENCODE lzo
	,top_previous_page VARCHAR(3000)   ENCODE lzo
	,user_type VARCHAR(3000)   ENCODE lzo
	,city VARCHAR(3000)   ENCODE lzo
	,company_domain VARCHAR(3000)   ENCODE lzo
	,company_fortune_1000_status VARCHAR(3000)   ENCODE lzo
	,company_isp VARCHAR(3000)   ENCODE lzo
	,company_naics_codes VARCHAR(3000)   ENCODE lzo
	,company_naics_description VARCHAR(3000)   ENCODE lzo
	,company_name VARCHAR(3000)   ENCODE lzo
	,company_region VARCHAR(3000)   ENCODE lzo
	,company_sic_codes VARCHAR(3000)   ENCODE lzo
	,company_sic_description VARCHAR(3000)   ENCODE lzo
	,industry_tier_1 VARCHAR(3000)   ENCODE lzo
	,industry_tier_2 VARCHAR(3000)   ENCODE lzo
	,job_function VARCHAR(3000)   ENCODE lzo
	,parent_company VARCHAR(3000)   ENCODE lzo
	,hits_type VARCHAR(3000)   ENCODE lzo
	,sales_band VARCHAR(3000)   ENCODE lzo
	,state VARCHAR(3000)   ENCODE lzo
	,total_employees_size VARCHAR(3000)   ENCODE lzo
	,user_job_seniority VARCHAR(3000)   ENCODE lzo
	,user_industry_name VARCHAR(3000)   ENCODE lzo
	,client_id VARCHAR(3000)   ENCODE lzo
	,session_id VARCHAR(3000)   ENCODE lzo
	,hit_timestamp VARCHAR(100)   ENCODE lzo
	,goal_id VARCHAR(3000)   ENCODE lzo
	,goal VARCHAR(3000)   ENCODE lzo
	,goal_value VARCHAR(3000)   ENCODE lzo
	,bounce INTEGER   ENCODE az64
	,relevance VARCHAR(3000)   ENCODE lzo
	,operating_system VARCHAR(3000)   ENCODE lzo
	,device_model VARCHAR(3000)   ENCODE lzo
	,device_branding VARCHAR(3000)   ENCODE lzo
	,search_exists VARCHAR(3000)   ENCODE lzo
	,new_user VARCHAR(3000)   ENCODE lzo
	,etl_crte_user VARCHAR(100)   ENCODE lzo
	,etl_crte_ts VARCHAR(100)   ENCODE lzo
	,etl_updt_user VARCHAR(100)   ENCODE lzo
	,etl_updt_ts VARCHAR(100)   ENCODE lzo
)
DISTSTYLE EVEN
;
ALTER TABLE web_analytics_dm.dm_behaviour_web owner to base_admin;

-- Drop table

-- DROP TABLE web_analytics_dm.dm_behaviour_web_aggr;

--DROP TABLE web_analytics_dm.dm_behaviour_web_aggr;
CREATE TABLE IF NOT EXISTS web_analytics_dm.dm_behaviour_web_aggr
(
	siteurl_dt VARCHAR(500)   ENCODE lzo
	,bu_dt VARCHAR(500)   ENCODE lzo
	,business_units VARCHAR(500)   ENCODE lzo
	,brand_nm VARCHAR(500)   ENCODE lzo
	,division_nm VARCHAR(500)   ENCODE lzo
	,site_url VARCHAR(500)   ENCODE lzo
	,channel_name VARCHAR(500)   ENCODE lzo
	,source VARCHAR(500)   ENCODE lzo
	,device_model VARCHAR(500)   ENCODE lzo
	,device VARCHAR(500)   ENCODE lzo
	,browser VARCHAR(500)   ENCODE lzo
	,operating_system VARCHAR(500)   ENCODE lzo
	,city VARCHAR(500)   ENCODE lzo
	,state VARCHAR(500)   ENCODE lzo
	,country VARCHAR(500)   ENCODE lzo
	,"region" VARCHAR(500)   ENCODE lzo
	,hostname VARCHAR(500)   ENCODE lzo
	,"language" VARCHAR(500)   ENCODE lzo
	,medium VARCHAR(500)   ENCODE lzo
	,user_industry_name VARCHAR(500)   ENCODE lzo
	,company_name VARCHAR(500)   ENCODE lzo
	,date VARCHAR(500)   ENCODE lzo
	,parent_company VARCHAR(500)   ENCODE lzo
	,sales_band VARCHAR(500)   ENCODE lzo
	,job_function VARCHAR(500)   ENCODE lzo
	,total_employees_size VARCHAR(500)   ENCODE lzo
	,users_count BIGINT   ENCODE az64
	,new_user_count BIGINT   ENCODE az64
	,visits_count BIGINT   ENCODE az64
	,pageviews_sum BIGINT   ENCODE az64
	,bounce_sum BIGINT   ENCODE az64
	,entrance_count BIGINT   ENCODE az64
	,exit_count BIGINT   ENCODE az64
	,time_on_page_sum BIGINT   ENCODE az64
	,session_duration_sum BIGINT   ENCODE az64
	,landing_page VARCHAR(2000)   ENCODE lzo
	,relevance VARCHAR(500)   ENCODE lzo
	,industry_grp VARCHAR(500)   ENCODE lzo
)
DISTSTYLE EVEN
;
ALTER TABLE web_analytics_dm.dm_behaviour_web_aggr owner to base_admin;

-- Drop table

-- DROP TABLE web_analytics_dm.dm_behaviour_web_cal_source;

--DROP TABLE web_analytics_dm.dm_behaviour_web_cal_source;
CREATE TABLE IF NOT EXISTS web_analytics_dm.dm_behaviour_web_cal_source
(
	id BIGINT  DEFAULT "identity"(1039824, 0, '1,1'::text) ENCODE az64
	,date VARCHAR(100)   ENCODE lzo
	,business_unit VARCHAR(100)   ENCODE lzo
	,site_url VARCHAR(100)   ENCODE lzo
	,bu_dt VARCHAR(100)   ENCODE lzo
	,site_dt VARCHAR(100)   ENCODE lzo
	,division VARCHAR(100)   ENCODE lzo
	,brand VARCHAR(100)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE web_analytics_dm.dm_behaviour_web_cal_source owner to base_admin;

-- Drop table

-- DROP TABLE web_analytics_dm.dm_behaviour_web_daily;

--DROP TABLE web_analytics_dm.dm_behaviour_web_daily;
CREATE TABLE IF NOT EXISTS web_analytics_dm.dm_behaviour_web_daily
(
	exit VARCHAR(3000)   ENCODE lzo
	,search VARCHAR(3000)   ENCODE lzo
	,search_refinement VARCHAR(3000)   ENCODE lzo
	,search_depth VARCHAR(3000)   ENCODE lzo
	,session_duration INTEGER   ENCODE az64
	,time_on_page INTEGER   ENCODE az64
	,entrance VARCHAR(3000)   ENCODE lzo
	,pageviews INTEGER   ENCODE az64
	,results_page_views VARCHAR(3000)   ENCODE lzo
	,time_after_search VARCHAR(3000)   ENCODE lzo
	,total_unique_searches VARCHAR(3000)   ENCODE lzo
	,unique_page_views VARCHAR(3000)   ENCODE lzo
	,users VARCHAR(3000)   ENCODE lzo
	,medium VARCHAR(3000)   ENCODE lzo
	,visits VARCHAR(3000)   ENCODE lzo
	,age_bracket VARCHAR(3000)   ENCODE lzo
	,browser VARCHAR(3000)   ENCODE lzo
	,business_units VARCHAR(3000)   ENCODE lzo
	,channel_name VARCHAR(3000)   ENCODE lzo
	,division_nm VARCHAR(3000)   ENCODE lzo
	,brand_nm VARCHAR(3000)   ENCODE lzo
	,country VARCHAR(3000)   ENCODE lzo
	,date VARCHAR(3000)   ENCODE lzo
	,device VARCHAR(3000)   ENCODE lzo
	,hostname VARCHAR(3000)   ENCODE lzo
	,landing_page VARCHAR(3000)   ENCODE lzo
	,landing_page_trim VARCHAR(3000)   ENCODE lzo
	,"language" VARCHAR(3000)   ENCODE lzo
	,page VARCHAR(3000)   ENCODE lzo
	,page_trim VARCHAR(3000)   ENCODE lzo
	,"region" VARCHAR(3000)   ENCODE lzo
	,query VARCHAR(3000)   ENCODE lzo
	,search_term VARCHAR(3000)   ENCODE lzo
	,industry_desc VARCHAR(3000)   ENCODE lzo
	,industry_grp VARCHAR(3000)   ENCODE lzo
	,fullvisitorid VARCHAR(3000)   ENCODE lzo
	,sites VARCHAR(3000)   ENCODE lzo
	,site_url VARCHAR(3000)   ENCODE lzo
	,hit_number INTEGER   ENCODE az64
	,source VARCHAR(3000)   ENCODE lzo
	,top_next_page VARCHAR(3000)   ENCODE lzo
	,top_previous_page VARCHAR(3000)   ENCODE lzo
	,user_type VARCHAR(3000)   ENCODE lzo
	,city VARCHAR(3000)   ENCODE lzo
	,company_domain VARCHAR(3000)   ENCODE lzo
	,company_fortune_1000_status VARCHAR(3000)   ENCODE lzo
	,company_isp VARCHAR(3000)   ENCODE lzo
	,company_naics_codes VARCHAR(3000)   ENCODE lzo
	,company_naics_description VARCHAR(3000)   ENCODE lzo
	,company_name VARCHAR(3000)   ENCODE lzo
	,company_region VARCHAR(3000)   ENCODE lzo
	,company_sic_codes VARCHAR(3000)   ENCODE lzo
	,company_sic_description VARCHAR(3000)   ENCODE lzo
	,industry_tier_1 VARCHAR(3000)   ENCODE lzo
	,industry_tier_2 VARCHAR(3000)   ENCODE lzo
	,job_function VARCHAR(3000)   ENCODE lzo
	,parent_company VARCHAR(3000)   ENCODE lzo
	,hits_type VARCHAR(3000)   ENCODE lzo
	,sales_band VARCHAR(3000)   ENCODE lzo
	,state VARCHAR(3000)   ENCODE lzo
	,total_employees_size VARCHAR(3000)   ENCODE lzo
	,user_job_seniority VARCHAR(3000)   ENCODE lzo
	,user_industry_name VARCHAR(3000)   ENCODE lzo
	,client_id VARCHAR(3000)   ENCODE lzo
	,session_id VARCHAR(3000)   ENCODE lzo
	,hit_timestamp VARCHAR(100)   ENCODE lzo
	,goal_id VARCHAR(3000)   ENCODE lzo
	,goal VARCHAR(3000)   ENCODE lzo
	,goal_value VARCHAR(3000)   ENCODE lzo
	,bounce INTEGER   ENCODE az64
	,relevance VARCHAR(3000)   ENCODE lzo
	,operating_system VARCHAR(3000)   ENCODE lzo
	,device_model VARCHAR(3000)   ENCODE lzo
	,device_branding VARCHAR(3000)   ENCODE lzo
	,search_exists VARCHAR(3000)   ENCODE lzo
	,new_user VARCHAR(3000)   ENCODE lzo
	,etl_crte_user VARCHAR(100)   ENCODE lzo
	,etl_crte_ts VARCHAR(100)   ENCODE lzo
	,etl_updt_user VARCHAR(100)   ENCODE lzo
	,etl_updt_ts VARCHAR(100)   ENCODE lzo
)
DISTSTYLE EVEN
;
ALTER TABLE web_analytics_dm.dm_behaviour_web_daily owner to base_admin;

-- Drop table

-- DROP TABLE web_analytics_dm.dm_behaviour_web_goal_aggr;

--DROP TABLE web_analytics_dm.dm_behaviour_web_goal_aggr;
CREATE TABLE IF NOT EXISTS web_analytics_dm.dm_behaviour_web_goal_aggr
(
	src_dt VARCHAR(100)   ENCODE lzo
	,sites VARCHAR(100)   ENCODE lzo
	,industry_grp VARCHAR(100)   ENCODE lzo
	,"region" VARCHAR(100)   ENCODE lzo
	,channel_name VARCHAR(100)   ENCODE lzo
	,city VARCHAR(100)   ENCODE lzo
	,country VARCHAR(100)   ENCODE lzo
	,state VARCHAR(100)   ENCODE lzo
	,"language" VARCHAR(100)   ENCODE lzo
	,date VARCHAR(100)   ENCODE lzo
	,goal VARCHAR(100)   ENCODE lzo
	,goal_id VARCHAR(100)   ENCODE lzo
	,goal_count BIGINT   ENCODE az64
	,siteurl_dt VARCHAR(500)   ENCODE lzo
	,brand_nm VARCHAR(500)   ENCODE lzo
	,division_nm VARCHAR(500)   ENCODE lzo
	,site_url VARCHAR(500)   ENCODE lzo
	,source VARCHAR(500)   ENCODE lzo
	,device_model VARCHAR(500)   ENCODE lzo
	,device VARCHAR(500)   ENCODE lzo
	,browser VARCHAR(500)   ENCODE lzo
	,operating_system VARCHAR(500)   ENCODE lzo
	,landing_page VARCHAR(2000)   ENCODE lzo
	,hostname VARCHAR(500)   ENCODE lzo
	,medium VARCHAR(500)   ENCODE lzo
	,user_industry_name VARCHAR(500)   ENCODE lzo
	,company_name VARCHAR(500)   ENCODE lzo
	,parent_company VARCHAR(500)   ENCODE lzo
	,sales_band VARCHAR(500)   ENCODE lzo
	,job_function VARCHAR(500)   ENCODE lzo
	,total_employees_size VARCHAR(500)   ENCODE lzo
	,relevance VARCHAR(500)   ENCODE lzo
)
DISTSTYLE EVEN
;
ALTER TABLE web_analytics_dm.dm_behaviour_web_goal_aggr owner to base_admin;

-- Drop table

-- DROP TABLE web_analytics_dm.dm_behaviour_web_page_trim_aggr;

--DROP TABLE web_analytics_dm.dm_behaviour_web_page_trim_aggr;
CREATE TABLE IF NOT EXISTS web_analytics_dm.dm_behaviour_web_page_trim_aggr
(
	siteurl_dt VARCHAR(500)   ENCODE lzo
	,bu_dt VARCHAR(500)   ENCODE lzo
	,business_units VARCHAR(500)   ENCODE lzo
	,brand_nm VARCHAR(500)   ENCODE lzo
	,division_nm VARCHAR(500)   ENCODE lzo
	,site_url VARCHAR(500)   ENCODE lzo
	,channel_name VARCHAR(500)   ENCODE lzo
	,source VARCHAR(500)   ENCODE lzo
	,device_model VARCHAR(500)   ENCODE lzo
	,device VARCHAR(500)   ENCODE lzo
	,browser VARCHAR(500)   ENCODE lzo
	,operating_system VARCHAR(500)   ENCODE lzo
	,city VARCHAR(500)   ENCODE lzo
	,landing_page VARCHAR(2000)   ENCODE lzo
	,search_term VARCHAR(2000)   ENCODE lzo
	,page VARCHAR(2000)   ENCODE lzo
	,state VARCHAR(500)   ENCODE lzo
	,country VARCHAR(500)   ENCODE lzo
	,"region" VARCHAR(500)   ENCODE lzo
	,hostname VARCHAR(500)   ENCODE lzo
	,"language" VARCHAR(500)   ENCODE lzo
	,medium VARCHAR(500)   ENCODE lzo
	,user_industry_name VARCHAR(500)   ENCODE lzo
	,company_name VARCHAR(500)   ENCODE lzo
	,date VARCHAR(500)   ENCODE lzo
	,parent_company VARCHAR(500)   ENCODE lzo
	,sales_band VARCHAR(500)   ENCODE lzo
	,job_function VARCHAR(500)   ENCODE lzo
	,total_employees_size VARCHAR(500)   ENCODE lzo
	,relevance VARCHAR(500)   ENCODE lzo
	,industry_grp VARCHAR(500)   ENCODE lzo
	,users_count BIGINT   ENCODE az64
	,new_user_count BIGINT   ENCODE az64
	,visits_count BIGINT   ENCODE az64
	,pageviews_sum BIGINT   ENCODE az64
	,bounce_sum BIGINT   ENCODE az64
	,entrance_count BIGINT   ENCODE az64
	,exit_count BIGINT   ENCODE az64
	,time_on_page_sum BIGINT   ENCODE az64
	,search_refinement BIGINT   ENCODE az64
	,search_depth BIGINT   ENCODE az64
	,total_unique_searches BIGINT   ENCODE az64
	,search BIGINT   ENCODE az64
	,search_exits BIGINT   ENCODE az64
	,unique_page_views BIGINT   ENCODE az64
	,results_page_views BIGINT   ENCODE az64
	,search_refinement_count BIGINT   ENCODE az64
	,search_depth_count BIGINT   ENCODE az64
	,total_unique_searches_count BIGINT   ENCODE az64
	,search_count BIGINT   ENCODE az64
	,search_exists_count BIGINT   ENCODE az64
	,unique_page_views_count BIGINT   ENCODE az64
	,results_page_views_count BIGINT   ENCODE az64
	,session_duration_sum BIGINT   ENCODE az64
)
DISTSTYLE EVEN
;
ALTER TABLE web_analytics_dm.dm_behaviour_web_page_trim_aggr owner to base_admin;

-- Drop table

-- DROP TABLE web_analytics_dm.dm_behaviour_web_rpt;

--DROP TABLE web_analytics_dm.dm_behaviour_web_rpt;
CREATE TABLE IF NOT EXISTS web_analytics_dm.dm_behaviour_web_rpt
(
	exit VARCHAR(3000)   ENCODE lzo
	,search VARCHAR(3000)   ENCODE lzo
	,search_refinement VARCHAR(3000)   ENCODE lzo
	,search_depth VARCHAR(3000)   ENCODE lzo
	,session_duration INTEGER   ENCODE az64
	,time_on_page INTEGER   ENCODE az64
	,entrance VARCHAR(3000)   ENCODE lzo
	,pageviews INTEGER   ENCODE az64
	,results_page_views VARCHAR(3000)   ENCODE lzo
	,time_after_search VARCHAR(3000)   ENCODE lzo
	,total_unique_searches VARCHAR(3000)   ENCODE lzo
	,unique_page_views VARCHAR(3000)   ENCODE lzo
	,users VARCHAR(3000)   ENCODE lzo
	,medium VARCHAR(3000)   ENCODE lzo
	,visits VARCHAR(3000)   ENCODE lzo
	,age_bracket VARCHAR(3000)   ENCODE lzo
	,browser VARCHAR(3000)   ENCODE lzo
	,business_units VARCHAR(3000)   ENCODE lzo
	,channel_name VARCHAR(3000)   ENCODE lzo
	,division_nm VARCHAR(3000)   ENCODE lzo
	,brand_nm VARCHAR(3000)   ENCODE lzo
	,country VARCHAR(3000)   ENCODE lzo
	,date VARCHAR(3000)   ENCODE lzo
	,device VARCHAR(3000)   ENCODE lzo
	,hostname VARCHAR(3000)   ENCODE lzo
	,landing_page VARCHAR(3000)   ENCODE lzo
	,landing_page_trim VARCHAR(3000)   ENCODE lzo
	,"language" VARCHAR(3000)   ENCODE lzo
	,page VARCHAR(3000)   ENCODE lzo
	,page_trim VARCHAR(3000)   ENCODE lzo
	,"region" VARCHAR(3000)   ENCODE lzo
	,query VARCHAR(3000)   ENCODE lzo
	,search_term VARCHAR(3000)   ENCODE lzo
	,industry_desc VARCHAR(3000)   ENCODE lzo
	,industry_grp VARCHAR(3000)   ENCODE lzo
	,fullvisitorid VARCHAR(3000)   ENCODE lzo
	,sites VARCHAR(3000)   ENCODE lzo
	,site_url VARCHAR(3000)   ENCODE lzo
	,hit_number INTEGER   ENCODE az64
	,source VARCHAR(3000)   ENCODE lzo
	,top_next_page VARCHAR(3000)   ENCODE lzo
	,top_previous_page VARCHAR(3000)   ENCODE lzo
	,user_type VARCHAR(3000)   ENCODE lzo
	,city VARCHAR(3000)   ENCODE lzo
	,company_domain VARCHAR(3000)   ENCODE lzo
	,company_fortune_1000_status VARCHAR(3000)   ENCODE lzo
	,company_isp VARCHAR(3000)   ENCODE lzo
	,company_naics_codes VARCHAR(3000)   ENCODE lzo
	,company_naics_description VARCHAR(3000)   ENCODE lzo
	,company_name VARCHAR(3000)   ENCODE lzo
	,company_region VARCHAR(3000)   ENCODE lzo
	,company_sic_codes VARCHAR(3000)   ENCODE lzo
	,company_sic_description VARCHAR(3000)   ENCODE lzo
	,industry_tier_1 VARCHAR(3000)   ENCODE lzo
	,industry_tier_2 VARCHAR(3000)   ENCODE lzo
	,job_function VARCHAR(3000)   ENCODE lzo
	,parent_company VARCHAR(3000)   ENCODE lzo
	,hits_type VARCHAR(3000)   ENCODE lzo
	,sales_band VARCHAR(3000)   ENCODE lzo
	,state VARCHAR(3000)   ENCODE lzo
	,total_employees_size VARCHAR(3000)   ENCODE lzo
	,user_job_seniority VARCHAR(3000)   ENCODE lzo
	,user_industry_name VARCHAR(3000)   ENCODE lzo
	,client_id VARCHAR(3000)   ENCODE lzo
	,session_id VARCHAR(3000)   ENCODE lzo
	,hit_timestamp VARCHAR(100)   ENCODE lzo
	,goal_id VARCHAR(3000)   ENCODE lzo
	,goal VARCHAR(3000)   ENCODE lzo
	,goal_value VARCHAR(3000)   ENCODE lzo
	,bounce INTEGER   ENCODE az64
	,relevance VARCHAR(3000)   ENCODE lzo
	,operating_system VARCHAR(3000)   ENCODE lzo
	,device_model VARCHAR(3000)   ENCODE lzo
	,device_branding VARCHAR(3000)   ENCODE lzo
	,search_exists VARCHAR(3000)   ENCODE lzo
	,new_user VARCHAR(3000)   ENCODE lzo
	,etl_crte_user VARCHAR(100)   ENCODE lzo
	,etl_crte_ts VARCHAR(100)   ENCODE lzo
	,etl_updt_user VARCHAR(100)   ENCODE lzo
	,etl_updt_ts VARCHAR(100)   ENCODE lzo
)
DISTSTYLE EVEN
;
ALTER TABLE web_analytics_dm.dm_behaviour_web_rpt owner to base_admin;

-- Drop table

-- DROP TABLE web_analytics_dm.dm_language_master;

--DROP TABLE web_analytics_dm.dm_language_master;
CREATE TABLE IF NOT EXISTS web_analytics_dm.dm_language_master
(
	id INTEGER  DEFAULT "identity"(1039819, 0, '1,1'::text) ENCODE az64
	,"language" VARCHAR(500)   ENCODE lzo
)
DISTSTYLE ALL
;
ALTER TABLE web_analytics_dm.dm_language_master owner to base_admin;

-- Drop table

-- DROP TABLE web_analytics_dm.industry_tyre_group;

--DROP TABLE web_analytics_dm.industry_tyre_group;
CREATE TABLE IF NOT EXISTS web_analytics_dm.industry_tyre_group
(
	sic_code VARCHAR(500)   ENCODE lzo
	,industry_description VARCHAR(500)   ENCODE lzo
	,group_nm VARCHAR(500)   ENCODE lzo
)
DISTSTYLE ALL
;
ALTER TABLE web_analytics_dm.industry_tyre_group owner to base_admin;

CREATE OR REPLACE VIEW web_analytics_dm.dm_behaviour_web_page_trim_aggr_vw
AS SELECT dm_behaviour_web_page_trim_aggr.siteurl_dt, dm_behaviour_web_page_trim_aggr.bu_dt, dm_behaviour_web_page_trim_aggr.business_units, dm_behaviour_web_page_trim_aggr.brand_nm, dm_behaviour_web_page_trim_aggr.division_nm, dm_behaviour_web_page_trim_aggr.site_url, dm_behaviour_web_page_trim_aggr.channel_name, dm_behaviour_web_page_trim_aggr.source, dm_behaviour_web_page_trim_aggr.device, dm_behaviour_web_page_trim_aggr.landing_page, dm_behaviour_web_page_trim_aggr.page, dm_behaviour_web_page_trim_aggr.country, dm_behaviour_web_page_trim_aggr."region", dm_behaviour_web_page_trim_aggr.hostname, dm_behaviour_web_page_trim_aggr.medium, dm_behaviour_web_page_trim_aggr.user_industry_name, dm_behaviour_web_page_trim_aggr.company_name, dm_behaviour_web_page_trim_aggr.date, dm_behaviour_web_page_trim_aggr.relevance, dm_behaviour_web_page_trim_aggr.new_user_count, dm_behaviour_web_page_trim_aggr.visits_count, dm_behaviour_web_page_trim_aggr.pageviews_sum, dm_behaviour_web_page_trim_aggr.bounce_sum, dm_behaviour_web_page_trim_aggr.entrance_count, dm_behaviour_web_page_trim_aggr.exit_count, dm_behaviour_web_page_trim_aggr.time_on_page_sum, dm_behaviour_web_page_trim_aggr.search, dm_behaviour_web_page_trim_aggr.unique_page_views, dm_behaviour_web_page_trim_aggr.search_count, dm_behaviour_web_page_trim_aggr.unique_page_views_count, dm_behaviour_web_page_trim_aggr.session_duration_sum
   FROM web_analytics_dm.dm_behaviour_web_page_trim_aggr;

CREATE OR REPLACE VIEW web_analytics_dm.ga_goal_aggr_vw_1
AS SELECT t1.channel_name, t1.date, t1.siteurl_dt, t1.brand_nm, t1.division_nm, t1.source, t1.industry_grp, t1.business_units, t1.site_url, t1.landing_page, t1.bu_dt, t1.device_model, t1.device, t1.browser, t1.operating_system, t1.city, t1.state, t1.country, t1."region", t1.hostname, t1."language", t1.medium, t1.user_industry_name, t1.company_name, t1.parent_company, t1.sales_band, t1.job_function, t1.total_employees_size, t1.relevance, t1.goal_1, t1.goal_2, t1.goal_3, t1.goal_4, t1.goal_5, t1.goal_6, t1.goal_7, t1.goal_8, t1.goal_9, t1.goal_10, t1.goal_11, t1.goal_12, t1.goal_13, t1.goal_14, t1.goal_15, t1.goal_16, t1.goal_17, t1.goal_18, t1.goal_19, t1.goal_20, t1.new_user_count, t1.visits_count, t1.pageviews_sum, t1.bounce_sum, t1.entrance_count, t1.exit_count, t1.time_on_page_sum, t1.session_duration_sum, t1.goal_cnt
   FROM ( SELECT web_final.channel_name, web_final.date, web_final.siteurl_dt, web_final.brand_nm, web_final.division_nm, web_final.source, web_final.industry_grp, web_final.business_units, web_final.site_url, web_final.landing_page, web_final.bu_dt, web_final.device_model, web_final.device, web_final.browser, web_final.operating_system, web_final.city, web_final.state, web_final.country, web_final."region", web_final.hostname, web_final."language", web_final.medium, web_final.user_industry_name, web_final.company_name, web_final.parent_company, web_final.sales_band, web_final.job_function, web_final.total_employees_size, web_final.relevance, "max"(web_final.goal_1) AS goal_1, "max"(web_final.goal_2) AS goal_2, "max"(web_final.goal_3) AS goal_3, "max"(web_final.goal_4) AS goal_4, "max"(web_final.goal_5) AS goal_5, "max"(web_final.goal_6) AS goal_6, "max"(web_final.goal_7) AS goal_7, "max"(web_final.goal_8) AS goal_8, "max"(web_final.goal_9) AS goal_9, "max"(web_final.goal_10) AS goal_10, "max"(web_final.goal_11) AS goal_11, "max"(web_final.goal_12) AS goal_12, "max"(web_final.goal_13) AS goal_13, "max"(web_final.goal_14) AS goal_14, "max"(web_final.goal_15) AS goal_15, "max"(web_final.goal_16) AS goal_16, "max"(web_final.goal_17) AS goal_17, "max"(web_final.goal_18) AS goal_18, "max"(web_final.goal_19) AS goal_19, "max"(web_final.goal_20) AS goal_20, web_final.new_user_count, web_final.visits_count, web_final.pageviews_sum, web_final.bounce_sum, web_final.entrance_count, web_final.exit_count, web_final.time_on_page_sum, web_final.session_duration_sum, sum(web_final.goal_1 + web_final.goal_2 + web_final.goal_3 + web_final.goal_4 + web_final.goal_5 + web_final.goal_6 + web_final.goal_7 + web_final.goal_8 + web_final.goal_9 + web_final.goal_10 + web_final.goal_11 + web_final.goal_12 + web_final.goal_13 + web_final.goal_14 + web_final.goal_15 + web_final.goal_16 + web_final.goal_17 + web_final.goal_18 + web_final.goal_19 + web_final.goal_20) AS goal_cnt
           FROM ( SELECT a.channel_name, a.date, a.siteurl_dt, a.brand_nm, a.division_nm, a.source, a.industry_grp, a.business_units, a.site_url, a.landing_page, a.bu_dt, a.device_model, a.device, a.browser, a.operating_system, a.city, a.state, a.country, a."region", a.hostname, a."language", a.medium, a.user_industry_name, a.company_name, a.parent_company, a.sales_band, a.job_function, a.total_employees_size, a.relevance, 
                        CASE
                            WHEN g.goal_id::text = 'Goal-1'::character varying::text THEN sum(g.goal_count)
                            ELSE 0::bigint
                        END AS goal_1, 
                        CASE
                            WHEN g.goal_id::text = 'Goal-2'::character varying::text THEN sum(g.goal_count)
                            ELSE 0::bigint
                        END AS goal_2, 
                        CASE
                            WHEN g.goal_id::text = 'Goal-3'::character varying::text THEN sum(g.goal_count)
                            ELSE 0::bigint
                        END AS goal_3, 
                        CASE
                            WHEN g.goal_id::text = 'Goal-4'::character varying::text THEN sum(g.goal_count)
                            ELSE 0::bigint
                        END AS goal_4, 
                        CASE
                            WHEN g.goal_id::text = 'Goal-5'::character varying::text THEN sum(g.goal_count)
                            ELSE 0::bigint
                        END AS goal_5, 
                        CASE
                            WHEN g.goal_id::text = 'Goal-6'::character varying::text THEN sum(g.goal_count)
                            ELSE 0::bigint
                        END AS goal_6, 
                        CASE
                            WHEN g.goal_id::text = 'Goal-7'::character varying::text THEN sum(g.goal_count)
                            ELSE 0::bigint
                        END AS goal_7, 
                        CASE
                            WHEN g.goal_id::text = 'Goal-8'::character varying::text THEN sum(g.goal_count)
                            ELSE 0::bigint
                        END AS goal_8, 
                        CASE
                            WHEN g.goal_id::text = 'Goal-9'::character varying::text THEN sum(g.goal_count)
                            ELSE 0::bigint
                        END AS goal_9, 
                        CASE
                            WHEN g.goal_id::text = 'Goal-10'::character varying::text THEN sum(g.goal_count)
                            ELSE 0::bigint
                        END AS goal_10, 
                        CASE
                            WHEN g.goal_id::text = 'Goal-11'::character varying::text THEN sum(g.goal_count)
                            ELSE 0::bigint
                        END AS goal_11, 
                        CASE
                            WHEN g.goal_id::text = 'Goal-12'::character varying::text THEN sum(g.goal_count)
                            ELSE 0::bigint
                        END AS goal_12, 
                        CASE
                            WHEN g.goal_id::text = 'Goal-13'::character varying::text THEN sum(g.goal_count)
                            ELSE 0::bigint
                        END AS goal_13, 
                        CASE
                            WHEN g.goal_id::text = 'Goal-14'::character varying::text THEN sum(g.goal_count)
                            ELSE 0::bigint
                        END AS goal_14, 
                        CASE
                            WHEN g.goal_id::text = 'Goal-15'::character varying::text THEN sum(g.goal_count)
                            ELSE 0::bigint
                        END AS goal_15, 
                        CASE
                            WHEN g.goal_id::text = 'Goal-16'::character varying::text THEN sum(g.goal_count)
                            ELSE 0::bigint
                        END AS goal_16, 
                        CASE
                            WHEN g.goal_id::text = 'Goal-17'::character varying::text THEN sum(g.goal_count)
                            ELSE 0::bigint
                        END AS goal_17, 
                        CASE
                            WHEN g.goal_id::text = 'Goal-18'::character varying::text THEN sum(g.goal_count)
                            ELSE 0::bigint
                        END AS goal_18, 
                        CASE
                            WHEN g.goal_id::text = 'Goal-19'::character varying::text THEN sum(g.goal_count)
                            ELSE 0::bigint
                        END AS goal_19, 
                        CASE
                            WHEN g.goal_id::text = 'Goal-20'::character varying::text THEN sum(g.goal_count)
                            ELSE 0::bigint
                        END AS goal_20, sum(a.new_user_count) AS new_user_count, sum(a.visits_count) AS visits_count, sum(a.pageviews_sum) AS pageviews_sum, sum(a.bounce_sum) AS bounce_sum, sum(a.entrance_count) AS entrance_count, sum(a.exit_count) AS exit_count, sum(a.time_on_page_sum) AS time_on_page_sum, sum(a.session_duration_sum) AS session_duration_sum
                   FROM ( SELECT dm_behaviour_web_aggr.siteurl_dt, dm_behaviour_web_aggr.bu_dt, dm_behaviour_web_aggr.business_units, dm_behaviour_web_aggr.brand_nm, dm_behaviour_web_aggr.division_nm, dm_behaviour_web_aggr.site_url, dm_behaviour_web_aggr.channel_name, dm_behaviour_web_aggr.source, dm_behaviour_web_aggr.device_model, dm_behaviour_web_aggr.device, dm_behaviour_web_aggr.browser, dm_behaviour_web_aggr.operating_system, dm_behaviour_web_aggr.city, dm_behaviour_web_aggr.state, dm_behaviour_web_aggr.country, dm_behaviour_web_aggr."region", dm_behaviour_web_aggr.hostname, dm_behaviour_web_aggr."language", dm_behaviour_web_aggr.medium, dm_behaviour_web_aggr.user_industry_name, dm_behaviour_web_aggr.company_name, dm_behaviour_web_aggr.date, dm_behaviour_web_aggr.parent_company, dm_behaviour_web_aggr.sales_band, dm_behaviour_web_aggr.job_function, dm_behaviour_web_aggr.total_employees_size, dm_behaviour_web_aggr.new_user_count, dm_behaviour_web_aggr.visits_count, dm_behaviour_web_aggr.pageviews_sum, dm_behaviour_web_aggr.bounce_sum, dm_behaviour_web_aggr.entrance_count, dm_behaviour_web_aggr.exit_count, dm_behaviour_web_aggr.time_on_page_sum, dm_behaviour_web_aggr.session_duration_sum, dm_behaviour_web_aggr.landing_page, dm_behaviour_web_aggr.relevance, dm_behaviour_web_aggr.industry_grp
                           FROM web_analytics_dm.dm_behaviour_web_aggr) a
              LEFT JOIN ( SELECT dm_behaviour_web_goal_aggr.src_dt, dm_behaviour_web_goal_aggr.sites, dm_behaviour_web_goal_aggr.industry_grp, dm_behaviour_web_goal_aggr."region", dm_behaviour_web_goal_aggr.channel_name, dm_behaviour_web_goal_aggr.city, dm_behaviour_web_goal_aggr.country, dm_behaviour_web_goal_aggr.state, dm_behaviour_web_goal_aggr."language", dm_behaviour_web_goal_aggr.date, dm_behaviour_web_goal_aggr.goal, dm_behaviour_web_goal_aggr.goal_id, dm_behaviour_web_goal_aggr.goal_count, dm_behaviour_web_goal_aggr.siteurl_dt, dm_behaviour_web_goal_aggr.brand_nm, dm_behaviour_web_goal_aggr.division_nm, dm_behaviour_web_goal_aggr.site_url, dm_behaviour_web_goal_aggr.source, dm_behaviour_web_goal_aggr.device_model, dm_behaviour_web_goal_aggr.device, dm_behaviour_web_goal_aggr.browser, dm_behaviour_web_goal_aggr.operating_system, dm_behaviour_web_goal_aggr.landing_page, dm_behaviour_web_goal_aggr.hostname, dm_behaviour_web_goal_aggr.medium, dm_behaviour_web_goal_aggr.user_industry_name, dm_behaviour_web_goal_aggr.company_name, dm_behaviour_web_goal_aggr.parent_company, dm_behaviour_web_goal_aggr.sales_band, dm_behaviour_web_goal_aggr.job_function, dm_behaviour_web_goal_aggr.total_employees_size, dm_behaviour_web_goal_aggr.relevance
                           FROM web_analytics_dm.dm_behaviour_web_goal_aggr) g ON COALESCE(g.src_dt, ''::character varying)::text = COALESCE(a.bu_dt, ''::character varying)::text AND COALESCE(g.sites, ''::character varying)::text = COALESCE(a.business_units, ''::character varying)::text AND COALESCE(g.industry_grp, ''::character varying)::text = COALESCE(a.industry_grp, ''::character varying)::text AND COALESCE(g."region", ''::character varying)::text = COALESCE(a."region", ''::character varying)::text AND COALESCE(g.channel_name, ''::character varying)::text = COALESCE(a.channel_name, ''::character varying)::text AND COALESCE(g.city, ''::character varying)::text = COALESCE(a.city, ''::character varying)::text AND COALESCE(g.country, ''::character varying)::text = COALESCE(a.country, ''::character varying)::text AND COALESCE(g.state, ''::character varying)::text = COALESCE(a.state, ''::character varying)::text AND COALESCE(g."language", ''::character varying)::text = COALESCE(a."language", ''::character varying)::text AND COALESCE(g.date, ''::character varying)::text = COALESCE(a.date, ''::character varying)::text AND COALESCE(g.siteurl_dt, ''::character varying)::text = COALESCE(a.siteurl_dt, ''::character varying)::text AND COALESCE(g.brand_nm, ''::character varying)::text = COALESCE(a.brand_nm, ''::character varying)::text AND COALESCE(g.division_nm, ''::character varying)::text = COALESCE(a.division_nm, ''::character varying)::text AND COALESCE(g.site_url, ''::character varying)::text = COALESCE(a.site_url, ''::character varying)::text AND COALESCE(g.source, ''::character varying)::text = COALESCE(a.source, ''::character varying)::text AND COALESCE(g.device_model, ''::character varying)::text = COALESCE(a.device_model, ''::character varying)::text AND COALESCE(g.device, ''::character varying)::text = COALESCE(a.device, ''::character varying)::text AND COALESCE(g.browser, ''::character varying)::text = COALESCE(a.browser, ''::character varying)::text AND COALESCE(g.operating_system, ''::character varying)::text = COALESCE(a.operating_system, ''::character varying)::text AND COALESCE(g.landing_page, ''::character varying)::text = COALESCE(a.landing_page, ''::character varying)::text AND COALESCE(g.hostname, ''::character varying)::text = COALESCE(a.hostname, ''::character varying)::text AND COALESCE(g.medium, ''::character varying)::text = COALESCE(a.medium, ''::character varying)::text AND COALESCE(g.user_industry_name, ''::character varying)::text = COALESCE(a.user_industry_name, ''::character varying)::text AND COALESCE(g.company_name, ''::character varying)::text = COALESCE(a.company_name, ''::character varying)::text AND COALESCE(g.parent_company, ''::character varying)::text = COALESCE(a.parent_company, ''::character varying)::text AND COALESCE(g.sales_band, ''::character varying)::text = COALESCE(a.sales_band, ''::character varying)::text AND COALESCE(g.job_function, ''::character varying)::text = COALESCE(a.job_function, ''::character varying)::text AND COALESCE(g.total_employees_size, ''::character varying)::text = COALESCE(a.total_employees_size, ''::character varying)::text AND COALESCE(g.relevance, ''::character varying)::text = COALESCE(a.relevance, ''::character varying)::text
             WHERE a.business_units::text ~~* '%%'::character varying::text
             GROUP BY a.site_url, a.landing_page, a.business_units, a.channel_name, a.date, g.goal_id, a.siteurl_dt, a.brand_nm, a.division_nm, a.source, a.industry_grp, a.bu_dt, a.device_model, a.device, a.browser, a.operating_system, a.city, a.state, a.country, a."region", a.hostname, a."language", a.medium, a.user_industry_name, a.company_name, a.parent_company, a.sales_band, a.job_function, a.total_employees_size, a.relevance) web_final
          GROUP BY web_final.channel_name, web_final.date, web_final.siteurl_dt, web_final.brand_nm, web_final.division_nm, web_final.source, web_final.industry_grp, web_final.business_units, web_final.site_url, web_final.landing_page, web_final.bu_dt, web_final.device_model, web_final.device, web_final.browser, web_final.operating_system, web_final.city, web_final.state, web_final.country, web_final."region", web_final.hostname, web_final."language", web_final.medium, web_final.user_industry_name, web_final.company_name, web_final.parent_company, web_final.sales_band, web_final.job_function, web_final.total_employees_size, web_final.relevance, web_final.new_user_count, web_final.visits_count, web_final.pageviews_sum, web_final.bounce_sum, web_final.entrance_count, web_final.exit_count, web_final.time_on_page_sum, web_final.session_duration_sum) t1;

CREATE OR REPLACE FUNCTION web_analytics_dm.f_string_trim(str varchar)
	RETURNS varchar
	LANGUAGE plpythonu
	STABLE
AS $$
	
    strVal = str
	
    if strVal.find('#') > 0 :
    	patPosition = strVal.index('#')
    else :
    	patPosition = 999999
        
    if strVal.find('*') > 0 :
    	startPosition = strVal.index('*')
    else :
    	startPosition = 999999

    if strVal.find('?') > 0 :
    	questionPosition = strVal.index('?')
    else :
    	questionPosition = 999999        
    
    minPosition = 999999
    
    if patPosition < minPosition :
        minPosition = patPosition
        
    if startPosition < minPosition :
        minPosition = startPosition
		
    if questionPosition < minPosition :
        minPosition = questionPosition 
            
        
    if minPosition == 999999 :
        return strVal
    else:
        return strVal[:minPosition]
  
  


$$
;

CREATE OR REPLACE PROCEDURE web_analytics_dm.sp_data_load_googleads()
	LANGUAGE plpgsql
AS $$
	

  DECLARE
	REC_TBL_DETAIL RECORD;
	QUERY_LIST  varchar(500);
	count INT DEFAULT 0;
	SITE_LIST varchar(5000) := '';
	SITE_COUNT INT;
	SITE_NM varchar(50);
	QUERY_INSERT  varchar(65535);
	QUERY_DELETE  varchar(65535);
	ROWS_INSERTED_COUNT BIGINT;
	CURRENT_RUN_SEQ INT;
	CURRENT_JOB_NAME varchar(65535); 

	
	--Columns details Variables
	REC_COLUMN_LIST RECORD;
	QUERY_COLUMN_LIST  varchar(500);
	SRC_COLUMNS_LIST VARCHAR(65535) := '';
	TGT_COLUMNS_LIST VARCHAR(65535) := '';
	
	
	--Tables List
	REC_TBL RECORD;
	QUERY_TBL  varchar(500);
	
  BEGIN
	
	/* Get List of All Google Analytics Tables*/
 	QUERY_TBL := 'select application_nm from ctrl.job_master where source ilike ''GOOGLE_ADS'' and  state ilike ''active''';
	
	
	FOR REC_TBL IN EXECUTE QUERY_TBL
	LOOP	
	
		SITE_LIST := SITE_LIST ||  REC_TBL.application_nm || ',' ;

	END LOOP;

	SITE_LIST := substr(SITE_LIST, 1,len(SITE_LIST)-1);
	
	/* Get List of All Google Analytics Tables*/
	--QUERY_LIST := 'Select source ,application_nm ,dataset_nm ,job_nm ,description ,tgt_tbl_nm ,src_tbl_nm ,src_filter  from ctrl.job_master where source ilike ''%GOOGLE%ANALYTICS%''';
	
		
	Select length(SITE_LIST) - length(REPLACE(SITE_LIST,',','')) + 1 INTO SITE_COUNT;

		
	WHILE count < SITE_COUNT 
	LOOP

		BEGIN
		
			SRC_COLUMNS_LIST := '' ;
			TGT_COLUMNS_LIST := '' ;
			QUERY_INSERT := '' ;
			CURRENT_RUN_SEQ := 0 ;
		
			SITE_NM := SPLIT_PART(SITE_LIST,',',count + 1 );
			
			Select source ,application_nm ,dataset_nm ,job_nm ,description ,tgt_tbl_nm ,src_tbl_nm ,src_filter INTO REC_TBL_DETAIL  from ctrl.job_master where source ilike '%GOOGLE%ADS%' and application_nm = SITE_NM;
			
			CURRENT_JOB_NAME := REC_TBL_DETAIL.job_nm;
						
			SELECT COALESCE(max(run_seq),0)+1 into CURRENT_RUN_SEQ from ctrl.job_run_history where job_nm = CURRENT_JOB_NAME AND DATE(GETDATE()) = DATE(run_date);
			
			
			INSERT INTO ctrl.job_run_history (project,job_nm,run_date,run_seq,start_time,job_status) values ('WEB_ANALYTICS',CURRENT_JOB_NAME,DATE(GETDATE()),CURRENT_RUN_SEQ,GETDATE(),'RUNNING');
			
			
			
			
			/* Get List of All Google Analytics Tables*/
			QUERY_COLUMN_LIST := 'Select src_column_nm , tgt_column_nm, transformation, description , job_nm  from ctrl.job_detail where job_nm ilike ''%GOOGLE%ADS%''';
		


			FOR REC_COLUMN_LIST IN EXECUTE QUERY_COLUMN_LIST
			LOOP
				
				SRC_COLUMNS_LIST := SRC_COLUMNS_LIST || REC_COLUMN_LIST.src_column_nm || ',';
				
			
				
				IF UPPER(LTRIM(RTRIM(REC_COLUMN_LIST.src_column_nm))) ilike 'business_unit'
				THEN
					TGT_COLUMNS_LIST := TGT_COLUMNS_LIST || '''' || SITE_NM || '''' || ',';				
				ELSIF LTRIM(RTRIM(REC_COLUMN_LIST.src_column_nm)) ilike 'hit_timestamp'
				THEN
					TGT_COLUMNS_LIST := TGT_COLUMNS_LIST || 'segments_date' || ' || '':'' || ' || 'segments_hour' || ',';			
				ELSIF LTRIM(RTRIM(REC_COLUMN_LIST.src_column_nm)) ilike 'clicks' OR LTRIM(RTRIM(REC_COLUMN_LIST.src_column_nm)) ilike 'impressions' OR LTRIM(RTRIM(REC_COLUMN_LIST.src_column_nm)) ilike 'interactions'
				THEN
					TGT_COLUMNS_LIST := TGT_COLUMNS_LIST || REC_COLUMN_LIST.tgt_column_nm || '::BIGINT,';						
				ELSE		
					TGT_COLUMNS_LIST := TGT_COLUMNS_LIST || REC_COLUMN_LIST.tgt_column_nm || ',';	
				END IF;
				
								
					
			END LOOP;
			
			
			
			SRC_COLUMNS_LIST := substr(SRC_COLUMNS_LIST, 1,len(SRC_COLUMNS_LIST)-1);
			TGT_COLUMNS_LIST := substr(TGT_COLUMNS_LIST, 1,len(TGT_COLUMNS_LIST)-1);

			
			
			/* Delete if record exists in DataMart for available dates */
			QUERY_DELETE := 'DELETE FROM ' || REC_TBL_DETAIL.tgt_tbl_nm || ' WHERE business_unit = ''' || SITE_NM || ''' AND date IN (SELECT DISTINCT segments_date from  ' || REC_TBL_DETAIL.src_tbl_nm || ');';
			
			EXECUTE QUERY_DELETE;
			
			

			
			
			QUERY_INSERT := 'INSERT INTO ' || REC_TBL_DETAIL.tgt_tbl_nm || ' ( '  || SRC_COLUMNS_LIST || ' ) (SELECT ' || TGT_COLUMNS_LIST || ' FROM ' || REC_TBL_DETAIL.src_tbl_nm || '  ) ;';
			
			EXECUTE QUERY_INSERT;			
			GET DIAGNOSTICS ROWS_INSERTED_COUNT = ROW_COUNT;
			

			

			UPDATE ctrl.job_run_history SET	no_rows_ins = ROWS_INSERTED_COUNT	where run_seq = CURRENT_RUN_SEQ and job_nm = CURRENT_JOB_NAME AND DATE(GETDATE()) = DATE(run_date);


			UPDATE ctrl.job_run_history SET
			end_time = GETDATE(),
			job_status = 'COMPLETED'
			where run_seq = CURRENT_RUN_SEQ and job_nm = CURRENT_JOB_NAME AND DATE(GETDATE()) = DATE(run_date);
			
			
			count := count + 1;
			
		END;
			
	END LOOP;
	

	END;

$$
;

CREATE OR REPLACE PROCEDURE web_analytics_dm.sp_data_load_googleanalytics()
	LANGUAGE plpgsql
AS $$
	
	
	

  DECLARE
	REC_JOB_DETAIL RECORD;
	QUERY_LIST  varchar(500);
	count INT DEFAULT 0;
	SITE_LIST varchar(5000) := '';
	SITE_COUNT INT;
	SITE_NM varchar(50);
	DIVISION_NM varchar(50);
	BRAND_NM varchar(50);		
	QUERY_INSERT  varchar(65535);
	QUERY_INSERT_DAILY  varchar(65535);
	QUERY_DELETE  varchar(65535);
	ROWS_INSERTED_COUNT BIGINT;
	CURRENT_RUN_SEQ INT;
	CURRENT_JOB_NAME varchar(65535); 
	GOAL_MULTIPLE_TYPE varchar(65535);
	GOAL_MULTIPLE_NAME varchar(65535);
	GOAL_MULTIPLE_VALUE varchar(65535);
	GOAL_COUNTER integer; 
	GOAL_MULTIPLE_FLAG integer;
	SITE_URL varchar(500);
	

	--Goals Variables
	REC RECORD;
	SITE varchar(50);
	QUERY varchar(500);
	GOAL_TYPE varchar(65535);
	GOAL_NAME varchar(65535);
	GOAL_VALUE varchar(65535);
	OR_FLAG Integer := 0;
	AND_FLAG Integer := 0;
	GOAL_FLAG Integer := 0;
	
	--Columns details Variables
	REC_COLUMN_LIST RECORD;
	QUERY_COLUMN_LIST  varchar(500);
	SRC_COLUMNS_LIST VARCHAR(65535) := '';
	TGT_COLUMNS_LIST VARCHAR(65535) := '';
	
	
	--Tables List
	REC_TBL RECORD;
	QUERY_TBL  varchar(500);
	
  BEGIN
	
	/* Get List of All Google Analytics Tables*/
 	QUERY_TBL := 'select application_nm from ctrl.job_master where source ilike ''GOOGLE_ANALYTICS'' and  state ilike ''active''';
	
	/*Truncate Daily table */
	TRUNCATE TABLE web_analytics_dm.dm_behaviour_web_daily;
	
	FOR REC_TBL IN EXECUTE QUERY_TBL
	LOOP	
	
		SITE_LIST := SITE_LIST ||  REC_TBL.application_nm || ',' ;

	END LOOP;

	SITE_LIST := substr(SITE_LIST, 1,len(SITE_LIST)-1);
	
	/* Get List of All Google Analytics Tables*/
	--QUERY_LIST := 'Select source ,application_nm ,dataset_nm ,job_nm ,description ,tgt_tbl_nm ,src_tbl_nm ,src_filter  from ctrl.job_master where source ilike ''%GOOGLE%ANALYTICS%''';
	
		
	Select length(SITE_LIST) - length(REPLACE(SITE_LIST,',','')) + 1 INTO SITE_COUNT;
	

		
	WHILE count < SITE_COUNT 
	LOOP

		BEGIN
		
			SRC_COLUMNS_LIST := '' ;
			TGT_COLUMNS_LIST := '' ;
			OR_FLAG := 0 ;
			AND_FLAG := 0 ;
			GOAL_FLAG := 0;
			GOAL_TYPE := 'case ';
			GOAL_NAME := 'case ';
			GOAL_VALUE := 'case when ( ';
			QUERY_INSERT := '' ;
			QUERY_INSERT_DAILY := '' ;
			CURRENT_RUN_SEQ := 0 ;
			GOAL_MULTIPLE_FLAG := 0;
		
			SITE_NM := SPLIT_PART(SITE_LIST,',',count + 1 );
			Select source ,application_nm ,dataset_nm ,job_nm ,brand,description,division,tgt_tbl_nm ,src_tbl_nm,website_url  INTO REC_JOB_DETAIL  from ctrl.job_master where source ilike '%GOOGLE%ANALYTICS%' and application_nm = SITE_NM;
			
			
			CURRENT_JOB_NAME := REC_JOB_DETAIL.job_nm;
			DIVISION_NM := REC_JOB_DETAIL.division;
			BRAND_NM := REC_JOB_DETAIL.brand;
			SITE_URL := REC_JOB_DETAIL.website_url;
			
						
			SELECT COALESCE(max(run_seq),0)+1 into CURRENT_RUN_SEQ from ctrl.job_run_history where job_nm = CURRENT_JOB_NAME AND DATE(GETDATE()) = DATE(run_date);
			
			
			INSERT INTO ctrl.job_run_history (project,job_nm,run_date,run_seq,start_time,job_status) values ('WEB_ANALYTICS',CURRENT_JOB_NAME,DATE(GETDATE()),CURRENT_RUN_SEQ,GETDATE(),'RUNNING');
			
			
			
			
			/* Get List of All Google Analytics Tables*/
			QUERY_COLUMN_LIST := 'Select src_column_nm , tgt_column_nm, transformation, description ,datatype, job_nm  from ctrl.job_detail where job_nm ilike ''%GOOGLE%ANALYTICS%''';
		


			/* Get details of all Goals*/
			QUERY := 'Select source ,job_nm ,goal_id, goal ,UPPER(goal_type) As goal_type ,condition_column_value_1 ,condition_column_value_2 ,condition_column_value_3 ,condition_column_value_4 ,state from ctrl.goal_master where job_nm = ''' || SITE_NM || ''' and state ilike ''ACTIVE''';
		
			
			FOR REC IN EXECUTE QUERY
			LOOP
			
				--OR_FLAG := 0;
				AND_FLAG := 0;
				GOAL_FLAG := 1;
				GOAL_COUNTER := 1;
				--RAISE INFO 'Job_Name: %  , src table_Name : %', REC.job_nm,REC.tgt_tbl_nm;
									
				IF REC.goal_type = 'EVENT'
				THEN
						
						IF REC.condition_column_value_1 IS NOT NULL 
						THEN
						
							IF REC.condition_column_value_1 like '%|%'
							THEN
								GOAL_MULTIPLE_TYPE := '';
								GOAL_MULTIPLE_NAME := '';
								GOAL_MULTIPLE_VALUE := '';
								GOAL_COUNTER := 1;
								GOAL_MULTIPLE_FLAG := 1 ;
											
								WHILE GOAL_COUNTER < (Select length(REC.condition_column_value_1) - length(REPLACE(REC.condition_column_value_1,'|',''))) + 2
								LOOP
									
									GOAL_MULTIPLE_TYPE := GOAL_MULTIPLE_TYPE || '  coalesce(hits_eventinfo_eventcategory,'''')  ilike ''%' || (SPLIT_PART(REC.condition_column_value_1,'|',GOAL_COUNTER )) || '%'' OR';
									
									GOAL_MULTIPLE_NAME:= GOAL_MULTIPLE_NAME || '  coalesce(hits_eventinfo_eventcategory,'''')  ilike ''%' || (SPLIT_PART(REC.condition_column_value_1,'|',GOAL_COUNTER )) || '%'' OR';
									
									GOAL_MULTIPLE_VALUE := GOAL_MULTIPLE_VALUE || '  coalesce(hits_eventinfo_eventcategory,'''')  ilike ''%' || (SPLIT_PART(REC.condition_column_value_1,'|',GOAL_COUNTER )) || '%'' OR';
									
									
									GOAL_COUNTER := GOAL_COUNTER + 1;
								END LOOP;
						
								GOAL_MULTIPLE_TYPE := substr(GOAL_MULTIPLE_TYPE, 1,len(GOAL_MULTIPLE_TYPE)-2);
								GOAL_MULTIPLE_NAME := substr(GOAL_MULTIPLE_NAME, 1,len(GOAL_MULTIPLE_NAME)-2);
								GOAL_MULTIPLE_VALUE := substr(GOAL_MULTIPLE_VALUE, 1,len(GOAL_MULTIPLE_VALUE)-2);
								
								GOAL_TYPE := GOAL_TYPE || ' when  ( ' || GOAL_MULTIPLE_TYPE || ') ';
								GOAL_NAME:= GOAL_NAME ||  ' when  ( ' || GOAL_MULTIPLE_NAME || ') ';

									IF OR_FLAG = 0
									THEN
										GOAL_VALUE := GOAL_VALUE || '( (' || GOAL_MULTIPLE_VALUE || ') and hits_type = ''PAGE'')';
									ELSE
										GOAL_VALUE :=  GOAL_VALUE ||  'OR ( (' || GOAL_MULTIPLE_VALUE || ') and hits_type = ''PAGE'')';
									END IF;	
									AND_FLAG := 1 ;
							
							
							ELSE
								GOAL_TYPE := GOAL_TYPE ||  ' when  coalesce(hits_eventinfo_eventcategory,'''')  ilike ''' || REC.condition_column_value_1 || '''';
								GOAL_NAME := GOAL_NAME ||  ' when  coalesce(hits_eventinfo_eventcategory,'''')  ilike ''' || REC.condition_column_value_1 || '''';

								IF OR_FLAG = 0
								THEN
									GOAL_VALUE := GOAL_VALUE || ' ( coalesce(hits_eventinfo_eventcategory,'''')  ilike ''' || REC.condition_column_value_1 || '''';
								ELSE
									GOAL_VALUE := GOAL_VALUE || ' OR ( coalesce(hits_eventinfo_eventcategory,'''')  ilike ''' || REC.condition_column_value_1 || '''';
								END IF;
								AND_FLAG := 1 ;
							
							END IF;
						END IF;
						
						IF REC.condition_column_value_2 IS NOT NULL 
						THEN 
							
						
							IF REC.condition_column_value_2 like '%|%'
							THEN
								GOAL_MULTIPLE_TYPE := '';
								GOAL_MULTIPLE_NAME := '';
								GOAL_MULTIPLE_VALUE := '';
								GOAL_COUNTER := 1;
								GOAL_MULTIPLE_FLAG := 1 ;
											
								WHILE GOAL_COUNTER < (Select length(REC.condition_column_value_2) - length(REPLACE(REC.condition_column_value_2,'|',''))) + 2
								LOOP
									
									GOAL_MULTIPLE_TYPE := GOAL_MULTIPLE_TYPE || '  coalesce(hits_eventinfo_eventaction,'''')  ilike ''%' || (SPLIT_PART(REC.condition_column_value_2,'|',GOAL_COUNTER )) || '%'' OR';
									
									GOAL_MULTIPLE_NAME:= GOAL_MULTIPLE_NAME || '  coalesce(hits_eventinfo_eventaction,'''')  ilike ''%' || (SPLIT_PART(REC.condition_column_value_2,'|',GOAL_COUNTER )) || '%'' OR';
									
									GOAL_MULTIPLE_VALUE := GOAL_MULTIPLE_VALUE || '  coalesce(hits_eventinfo_eventaction,'''')  ilike ''%' || (SPLIT_PART(REC.condition_column_value_2,'|',GOAL_COUNTER )) || '%'' OR';									

									GOAL_COUNTER := GOAL_COUNTER + 1;
								END LOOP;


								
								GOAL_MULTIPLE_TYPE := substr(GOAL_MULTIPLE_TYPE, 1,len(GOAL_MULTIPLE_TYPE)-2);
								GOAL_MULTIPLE_NAME := substr(GOAL_MULTIPLE_NAME, 1,len(GOAL_MULTIPLE_NAME)-2);
								GOAL_MULTIPLE_VALUE := substr(GOAL_MULTIPLE_VALUE, 1,len(GOAL_MULTIPLE_VALUE)-2);	
								
								IF AND_FLAG = 1
								THEN
									GOAL_TYPE := GOAL_TYPE || ' and ( ' || GOAL_MULTIPLE_TYPE || ' ) ';
									GOAL_NAME:= GOAL_NAME ||  ' and ( ' || GOAL_MULTIPLE_NAME || ' ) ';
									GOAL_VALUE := GOAL_VALUE || ' and ( ' || GOAL_MULTIPLE_VALUE || ' ) ';										
								
								ELSE

									GOAL_TYPE := GOAL_TYPE || ' when  ( ' || GOAL_MULTIPLE_TYPE || ') and hits_type = ''PAGE''';
									GOAL_NAME:= GOAL_NAME ||  ' when  ( ' || GOAL_MULTIPLE_NAME || ') and hits_type = ''PAGE''';

									IF OR_FLAG = 0
									THEN
										GOAL_VALUE := GOAL_VALUE || '( (' || GOAL_MULTIPLE_VALUE || ') and hits_type = ''PAGE'')';
									ELSE
										GOAL_VALUE :=  GOAL_VALUE ||  'OR ( (' || GOAL_MULTIPLE_VALUE || ') and hits_type = ''PAGE'')';
									END IF;	
									AND_FLAG := 1 ;								
								
								END IF;

							ELSE		
								IF AND_FLAG = 1
								THEN
								  GOAL_TYPE := GOAL_TYPE || ' and  coalesce(hits_eventinfo_eventaction,'''')  ilike ''' || REC.condition_column_value_2 || '''';
								  GOAL_NAME := GOAL_NAME || ' and  coalesce(hits_eventinfo_eventaction,'''')  ilike ''' || REC.condition_column_value_2 || '''';
								  GOAL_VALUE := GOAL_VALUE || ' and  coalesce(hits_eventinfo_eventaction,'''')  ilike ''' || REC.condition_column_value_2 || '''';
								ELSE
								  GOAL_TYPE := GOAL_TYPE || ' when  coalesce(hits_eventinfo_eventaction,'''')  ilike ''' || REC.condition_column_value_2 || '''';
								  GOAL_NAME := GOAL_NAME || ' when  coalesce(hits_eventinfo_eventaction,'''')  ilike ''' || REC.condition_column_value_2 || '''';
								  
								  IF OR_FLAG = 0
								  THEN
									GOAL_VALUE := GOAL_VALUE || ' ( coalesce(hits_eventinfo_eventaction,'''')  ilike ''' || REC.condition_column_value_2 || '''';
								  ELSE
									GOAL_VALUE := GOAL_VALUE || ' OR ( coalesce(hits_eventinfo_eventaction,'''')  ilike ''' || REC.condition_column_value_2 || '''';
								  END IF;
								  AND_FLAG := 1 ;
								END IF;
							END IF;
						  
						END IF;
						
						IF REC.condition_column_value_3 IS NOT NULL 
						THEN 
							

						
							IF REC.condition_column_value_3 like '%|%'
							THEN
								GOAL_MULTIPLE_TYPE := '';
								GOAL_MULTIPLE_NAME := '';
								GOAL_MULTIPLE_VALUE := '';
								GOAL_COUNTER := 1;
								GOAL_MULTIPLE_FLAG := 1 ;
											
								WHILE GOAL_COUNTER < (Select length(REC.condition_column_value_3) - length(REPLACE(REC.condition_column_value_3,'|',''))) + 2
								LOOP
									
									GOAL_MULTIPLE_TYPE := GOAL_MULTIPLE_TYPE || '  coalesce(hits_eventinfo_eventLabel,'''')  ilike ''%' || (SPLIT_PART(REC.condition_column_value_3,'|',GOAL_COUNTER )) || '%'' OR';
									
									GOAL_MULTIPLE_NAME:= GOAL_MULTIPLE_NAME || '  coalesce(hits_eventinfo_eventLabel,'''')  ilike ''%' || (SPLIT_PART(REC.condition_column_value_3,'|',GOAL_COUNTER )) || '%'' OR';
									
									GOAL_MULTIPLE_VALUE := GOAL_MULTIPLE_VALUE || '  coalesce(hits_eventinfo_eventLabel,'''')  ilike ''%' || (SPLIT_PART(REC.condition_column_value_3,'|',GOAL_COUNTER )) || '%'' OR';									

									GOAL_COUNTER := GOAL_COUNTER + 1;
								END LOOP;

								
								GOAL_MULTIPLE_TYPE := substr(GOAL_MULTIPLE_TYPE, 1,len(GOAL_MULTIPLE_TYPE)-2);
								GOAL_MULTIPLE_NAME := substr(GOAL_MULTIPLE_NAME, 1,len(GOAL_MULTIPLE_NAME)-2);
								GOAL_MULTIPLE_VALUE := substr(GOAL_MULTIPLE_VALUE, 1,len(GOAL_MULTIPLE_VALUE)-2);	
								
								IF AND_FLAG = 1
								THEN
									GOAL_TYPE := GOAL_TYPE || ' and ( ' || GOAL_MULTIPLE_TYPE || ' ) ';
									GOAL_NAME:= GOAL_NAME ||  ' and ( ' || GOAL_MULTIPLE_NAME || ' ) ';
									GOAL_VALUE := GOAL_VALUE || ' and ( ' || GOAL_MULTIPLE_VALUE || ' ) ';										
								
								ELSE

									GOAL_TYPE := GOAL_TYPE || ' when  ( ' || GOAL_MULTIPLE_TYPE || ') and hits_type = ''PAGE''';
									GOAL_NAME:= GOAL_NAME ||  ' when  ( ' || GOAL_MULTIPLE_NAME || ') and hits_type = ''PAGE''';

									IF OR_FLAG = 0
									THEN
										GOAL_VALUE := GOAL_VALUE || '( (' || GOAL_MULTIPLE_VALUE || ') and hits_type = ''PAGE'')';
									ELSE
										GOAL_VALUE :=  GOAL_VALUE ||  'OR ( (' || GOAL_MULTIPLE_VALUE || ') and hits_type = ''PAGE'')';
									END IF;	
									AND_FLAG := 1 ;								
								
								END IF;

							ELSE		
								IF AND_FLAG = 1
								THEN
								  GOAL_TYPE := GOAL_TYPE || ' and  coalesce(hits_eventinfo_eventLabel,'''')  ilike ''' || REC.condition_column_value_3 || '''';
								  GOAL_NAME := GOAL_NAME || ' and  coalesce(hits_eventinfo_eventLabel,'''')  ilike ''' || REC.condition_column_value_3 || '''';
								  GOAL_VALUE := GOAL_VALUE || ' and  coalesce(hits_eventinfo_eventLabel,'''')  ilike ''' || REC.condition_column_value_3 || '''';
								ELSE
								  GOAL_TYPE := GOAL_TYPE || ' when  coalesce(hits_eventinfo_eventLabel,'''')  ilike ''' || REC.condition_column_value_3 || '''';
								  GOAL_NAME := GOAL_NAME || ' when  coalesce(hits_eventinfo_eventLabel,'''')  ilike ''' || REC.condition_column_value_3 || '''';
								  
								  IF OR_FLAG = 0
								  THEN
									GOAL_VALUE := GOAL_VALUE || ' ( coalesce(hits_eventinfo_eventLabel,'''')  ilike ''' || REC.condition_column_value_3 || '''';
								  ELSE
									GOAL_VALUE := GOAL_VALUE || ' OR ( coalesce(hits_eventinfo_eventLabel,'''')  ilike ''' || REC.condition_column_value_3 || '''';
								  END IF;
								  AND_FLAG := 1 ;
								END IF;
							END IF;
						  
						END IF;

						  
						IF REC.condition_column_value_4 IS NOT NULL 
						THEN 
						
							IF REC.condition_column_value_4 like '%|%'
							THEN
								GOAL_MULTIPLE_TYPE := '';
								GOAL_MULTIPLE_NAME := '';
								GOAL_MULTIPLE_VALUE := '';
								GOAL_COUNTER := 1;
								GOAL_MULTIPLE_FLAG := 1 ;
											
								WHILE GOAL_COUNTER < (Select length(REC.condition_column_value_4) - length(REPLACE(REC.condition_column_value_4,'|',''))) + 2
								LOOP
									
									GOAL_MULTIPLE_TYPE := GOAL_MULTIPLE_TYPE || '  coalesce(hits_eventinfo_eventValue,'''')  ilike ''%' || (SPLIT_PART(REC.condition_column_value_4,'|',GOAL_COUNTER )) || '%'' OR';
									
									GOAL_MULTIPLE_NAME:= GOAL_MULTIPLE_NAME || '  coalesce(hits_eventinfo_eventValue,'''')  ilike ''%' || (SPLIT_PART(REC.condition_column_value_4,'|',GOAL_COUNTER )) || '%'' OR';
									
									GOAL_MULTIPLE_VALUE := GOAL_MULTIPLE_VALUE || '  coalesce(hits_eventinfo_eventValue,'''')  ilike ''%' || (SPLIT_PART(REC.condition_column_value_4,'|',GOAL_COUNTER )) || '%'' OR';									

									GOAL_COUNTER := GOAL_COUNTER + 1;
								END LOOP;


								
								GOAL_MULTIPLE_TYPE := substr(GOAL_MULTIPLE_TYPE, 1,len(GOAL_MULTIPLE_TYPE)-2);
								GOAL_MULTIPLE_NAME := substr(GOAL_MULTIPLE_NAME, 1,len(GOAL_MULTIPLE_NAME)-2);
								GOAL_MULTIPLE_VALUE := substr(GOAL_MULTIPLE_VALUE, 1,len(GOAL_MULTIPLE_VALUE)-2);	
								
								IF AND_FLAG = 1
								THEN
									GOAL_TYPE := GOAL_TYPE || ' and ( ' || GOAL_MULTIPLE_TYPE || ' ) ';
									GOAL_NAME:= GOAL_NAME ||  ' and ( ' || GOAL_MULTIPLE_NAME || ' ) ';
									GOAL_VALUE := GOAL_VALUE || ' and ( ' || GOAL_MULTIPLE_VALUE || ' ) ';										
								
								ELSE

									GOAL_TYPE := GOAL_TYPE || ' when  ( ' || GOAL_MULTIPLE_TYPE || ') and hits_type = ''PAGE''';
									GOAL_NAME:= GOAL_NAME ||  ' when  ( ' || GOAL_MULTIPLE_NAME || ') and hits_type = ''PAGE''';

									IF OR_FLAG = 0
									THEN
										GOAL_VALUE := GOAL_VALUE || '( (' || GOAL_MULTIPLE_VALUE || ') and hits_type = ''PAGE'')';
									ELSE
										GOAL_VALUE :=  GOAL_VALUE ||  'OR ( (' || GOAL_MULTIPLE_VALUE || ') and hits_type = ''PAGE'')';
									END IF;	
									AND_FLAG := 1 ;								
								
								END IF;

							ELSE		
								IF AND_FLAG = 1
								THEN
								  GOAL_TYPE := GOAL_TYPE || ' and  coalesce(hits_eventinfo_eventValue,'''')  ilike ''' || REC.condition_column_value_4 || '''';
								  GOAL_NAME := GOAL_NAME || ' and  coalesce(hits_eventinfo_eventValue,'''')  ilike ''' || REC.condition_column_value_4 || '''';
								  GOAL_VALUE := GOAL_VALUE || ' and  coalesce(hits_eventinfo_eventValue,'''')  ilike ''' || REC.condition_column_value_4 || '''';
								ELSE
								  GOAL_TYPE := GOAL_TYPE || ' when  coalesce(hits_eventinfo_eventValue,'''')  ilike ''' || REC.condition_column_value_4 || '''';
								  GOAL_NAME := GOAL_NAME || ' when  coalesce(hits_eventinfo_eventValue,'''')  ilike ''' || REC.condition_column_value_4 || '''';
								  
								  IF OR_FLAG = 0
								  THEN
									GOAL_VALUE := GOAL_VALUE || ' ( coalesce(hits_eventinfo_eventValue,'''')  ilike ''' || REC.condition_column_value_4 || '''';
								  ELSE
									GOAL_VALUE := GOAL_VALUE || ' OR ( coalesce(hits_eventinfo_eventValue,'''')  ilike ''' || REC.condition_column_value_4 || '''';
								  END IF;
								  AND_FLAG := 1 ;
								END IF;
							END IF;
						  
						END IF;

						
						GOAL_TYPE := GOAL_TYPE || ' THEN ''Goal-' || REC.goal_id || '''';
						GOAL_NAME := GOAL_NAME || ' THEN ''' || REC.goal || '''';
						IF GOAL_MULTIPLE_FLAG = 1
						THEN
							GOAL_VALUE := GOAL_VALUE || '))';
						ELSE
							GOAL_VALUE := GOAL_VALUE || ')';
						END IF;
					
				ELSIF REC.goal_type ilike 'DESTINATION'
				THEN
	
					GOAL_MULTIPLE_TYPE := '';
					GOAL_MULTIPLE_NAME := '';
					GOAL_MULTIPLE_VALUE := '';
					GOAL_COUNTER := 1;
					GOAL_MULTIPLE_FLAG := 1 ;				
					IF REC.condition_column_value_1 like '%|%'
					THEN
					
						
						WHILE GOAL_COUNTER < (Select length(REC.condition_column_value_1) - length(REPLACE(REC.condition_column_value_1,'|',''))) + 2
						LOOP
							
							GOAL_MULTIPLE_TYPE := GOAL_MULTIPLE_TYPE || ' hits_page_pagepath ilike ''%' || (SPLIT_PART(REC.condition_column_value_1,'|',GOAL_COUNTER )) || '%'' OR';
							
							GOAL_MULTIPLE_NAME:= GOAL_MULTIPLE_NAME || ' hits_page_pagepath ilike ''%' || (SPLIT_PART(REC.condition_column_value_1,'|',GOAL_COUNTER )) || '%'' OR';
							
							GOAL_MULTIPLE_VALUE := GOAL_MULTIPLE_VALUE || ' hits_page_pagepath ilike ''%' || (SPLIT_PART(REC.condition_column_value_1,'|',GOAL_COUNTER )) || '%'' OR';
							
							
							GOAL_COUNTER := GOAL_COUNTER + 1;
						END LOOP;
						
						GOAL_MULTIPLE_TYPE := substr(GOAL_MULTIPLE_TYPE, 1,len(GOAL_MULTIPLE_TYPE)-2);
						GOAL_MULTIPLE_NAME := substr(GOAL_MULTIPLE_NAME, 1,len(GOAL_MULTIPLE_NAME)-2);
						GOAL_MULTIPLE_VALUE := substr(GOAL_MULTIPLE_VALUE, 1,len(GOAL_MULTIPLE_VALUE)-2);
						
						GOAL_TYPE := GOAL_TYPE || ' when  ( ' || GOAL_MULTIPLE_TYPE || ') and hits_type = ''PAGE''  THEN ''Goal-' || REC.goal_id || '''';
						GOAL_NAME:= GOAL_NAME ||  ' when  ( ' || GOAL_MULTIPLE_NAME || ') and hits_type = ''PAGE''  THEN ''' || REC.goal || '''';

							IF OR_FLAG = 0
							THEN
								GOAL_VALUE := GOAL_VALUE || '( (' || GOAL_MULTIPLE_VALUE || ') and hits_type = ''PAGE'')';
							ELSE
								GOAL_VALUE :=  GOAL_VALUE ||  'OR ( (' || GOAL_MULTIPLE_VALUE || ') and hits_type = ''PAGE'')';
							END IF;						
						
						
						
						
						
					ELSE
						GOAL_TYPE := GOAL_TYPE ||  ' when hits_page_pagepath ilike ''%' || REC.condition_column_value_1 || '%'' and hits_type = ''PAGE''  THEN ''Goal-' || REC.goal_id || '''';
						GOAL_NAME := GOAL_NAME ||  ' when hits_page_pagepath ilike ''%' || REC.condition_column_value_1 || '%'' and hits_type = ''PAGE''  THEN ''' || REC.goal || '''';
						IF OR_FLAG = 0
						THEN
							GOAL_VALUE := GOAL_VALUE || '(hits_page_pagepath ilike ''%' || REC.condition_column_value_1 || '%'' and hits_type = ''PAGE'')';
						ELSE
							GOAL_VALUE := GOAL_VALUE || ' OR (hits_page_pagepath ilike ''%' || REC.condition_column_value_1 || '%'' and hits_type = ''PAGE'')';
						END IF;
					END IF;
					
					  
				END IF;
				
				
				OR_FLAG := 1;
				RAISE INFO 'Ran SP(pl_trans_fact_c11_sp) Successfull%',GOAL_TYPE;
			END LOOP;
			
			
			
			IF GOAL_FLAG = 1 
			THEN 
				GOAL_TYPE := GOAL_TYPE || ' else null end';	
				GOAL_NAME := GOAL_NAME || ' else null end';	
				GOAL_VALUE := GOAL_VALUE || '  then 1 else 0 end';
			ELSE
				GOAL_TYPE := 'NULL';	
				GOAL_NAME := 'NULL';	
				GOAL_VALUE := 'NULL';
			END IF;
			



						

			FOR REC_COLUMN_LIST IN EXECUTE QUERY_COLUMN_LIST
			LOOP
				
				SRC_COLUMNS_LIST := SRC_COLUMNS_LIST || REC_COLUMN_LIST.src_column_nm || ',';
				
			 
				
				IF UPPER(LTRIM(RTRIM(REC_COLUMN_LIST.src_column_nm))) = 'SITES' or UPPER(LTRIM(RTRIM(REC_COLUMN_LIST.src_column_nm))) = 'BUSINESS_UNITS'
				THEN
					TGT_COLUMNS_LIST := TGT_COLUMNS_LIST || '''' || SITE_NM || '''' || ',';
				ELSIF LTRIM(RTRIM(REC_COLUMN_LIST.src_column_nm)) ilike 'goal_id'
				THEN
					TGT_COLUMNS_LIST := TGT_COLUMNS_LIST || '(' || GOAL_TYPE || ')' || ',';
				ELSIF LTRIM(RTRIM(REC_COLUMN_LIST.src_column_nm)) ilike 'goal'
				THEN
					TGT_COLUMNS_LIST := TGT_COLUMNS_LIST || '(' || GOAL_NAME || ')' || ',';
				ELSIF LTRIM(RTRIM(REC_COLUMN_LIST.src_column_nm)) ilike 'goal_value'
				THEN
					TGT_COLUMNS_LIST := TGT_COLUMNS_LIST || '(' || GOAL_VALUE || ')' || ',';
				ELSIF LTRIM(RTRIM(REC_COLUMN_LIST.src_column_nm)) ilike 'hit_timestamp'
				THEN
					TGT_COLUMNS_LIST := TGT_COLUMNS_LIST || 'visit_date' || ' || '':'' || ' || 'hits_hour' || ' || '':'' || ' || 'hits_minute' || ',';			
				ELSIF LTRIM(RTRIM(REC_COLUMN_LIST.src_column_nm)) ilike 'brand_nm'
				THEN
					TGT_COLUMNS_LIST := TGT_COLUMNS_LIST || '''' || BRAND_NM || '''' || ',';	
				ELSIF LTRIM(RTRIM(REC_COLUMN_LIST.src_column_nm)) ilike 'site_url'
				THEN
					TGT_COLUMNS_LIST := TGT_COLUMNS_LIST || '''' || SITE_URL || '''' || ',';			
				ELSIF LTRIM(RTRIM(REC_COLUMN_LIST.src_column_nm)) ilike 'division_nm'
				THEN
					TGT_COLUMNS_LIST := TGT_COLUMNS_LIST || '''' || DIVISION_NM || '''' || ',';						
				ELSE		
					TGT_COLUMNS_LIST := TGT_COLUMNS_LIST || REC_COLUMN_LIST.tgt_column_nm || ',';	
				END IF;
				
								
					
			END LOOP;
			
			
			
			SRC_COLUMNS_LIST := substr(SRC_COLUMNS_LIST, 1,len(SRC_COLUMNS_LIST)-1);
			TGT_COLUMNS_LIST := substr(TGT_COLUMNS_LIST, 1,len(TGT_COLUMNS_LIST)-1);

			
			
			/* Delete if record exists in DataMart for available dates */
			QUERY_DELETE := 'DELETE FROM ' || REC_JOB_DETAIL.tgt_tbl_nm || ' WHERE sites = ''' || SITE_NM || ''' AND date IN (SELECT DISTINCT visit_date from  ' || REC_JOB_DETAIL.src_tbl_nm || ');';
			
			EXECUTE QUERY_DELETE;
			
			

			
			--INSERT INTO REC_JOB_DETAIL.tgt_tbl_nm (SRC_COLUMNS_LIST)  (Select TGT_COLUMNS_LIST FROM REC_JOB_DETAIL.src_tbl_nm limit 100);
			QUERY_INSERT := 'INSERT INTO ' || REC_JOB_DETAIL.tgt_tbl_nm || ' ( '  || SRC_COLUMNS_LIST || ' ) (SELECT ' || TGT_COLUMNS_LIST || ' FROM ' || REC_JOB_DETAIL.src_tbl_nm || ' s  where LTRIM(RTRIM(device_language)) in (Select language from web_analytics_dm.dm_language_master) ) ;';
			
			
			EXECUTE QUERY_INSERT;			
			GET DIAGNOSTICS ROWS_INSERTED_COUNT = ROW_COUNT;

			/* LOAD DAILY TABLE */
			
			--TRUNCATE TABLE web_analytics_dm.dm_behaviour_web_daily;
			QUERY_INSERT_DAILY := 'INSERT INTO web_analytics_dm.dm_behaviour_web_daily ( '  || SRC_COLUMNS_LIST || ' ) (SELECT ' || TGT_COLUMNS_LIST || ' FROM ' || REC_JOB_DETAIL.src_tbl_nm || ' s  where LTRIM(RTRIM(device_language)) in (Select language from web_analytics_dm.dm_language_master) ) ;';
			
			EXECUTE QUERY_INSERT_DAILY;
			

			UPDATE ctrl.job_run_history SET	no_rows_ins = ROWS_INSERTED_COUNT	where run_seq = CURRENT_RUN_SEQ and job_nm = CURRENT_JOB_NAME AND DATE(GETDATE()) = DATE(run_date);


			UPDATE ctrl.job_run_history SET
			end_time = GETDATE(),
			job_status = 'COMPLETED'
			where run_seq = CURRENT_RUN_SEQ and job_nm = CURRENT_JOB_NAME AND DATE(GETDATE()) = DATE(run_date);
			
			
			count := count + 1;
			
		END;
			
	END LOOP;
	
	GOAL_TYPE := GOAL_TYPE || ' else null end';	
	GOAL_VALUE := GOAL_VALUE || '  then 1 else 0 end';
	END;



$$
;

CREATE OR REPLACE PROCEDURE web_analytics_dm.sp_data_load_googleanalytics_aggr()
	LANGUAGE plpgsql
AS $$
	
	
	
	
	

  DECLARE
	QUERY_TBL  varchar(500);
	SITE_LIST varchar(5000) := '';
	SITE_COUNT INT;
	SITE_NM varchar(50);	
	QUERY_DELETE  varchar(65535);
	QUERY_INSERT  varchar(65535);	
	QUERY_DELETE_AGGR  varchar(65535);
	QUERY_INSERT_AGGR  varchar(65535);	
	QUERY_DELETE_GOAL_AGGR  varchar(65535);
	QUERY_INSERT_GOAL_AGGR  varchar(65535);	
	QUERY_DELETE_PG_AGGR  varchar(65535);
	QUERY_INSERT_PG_AGGR  varchar(65535);		
	cnt INT DEFAULT 0;
	
	--Tables List
	REC_TBL RECORD;	

	
  BEGIN
  
	/* Create a temporary table for populating Page aggregate table */
	create temporary table dm_behaviour_web_daily_temp as select * from web_analytics_dm.dm_behaviour_web_daily;
	
	/* Get List of All Google Analytics Tables*/
 	QUERY_TBL := 'select application_nm from ctrl.job_master where source ilike ''GOOGLE_ANALYTICS'' and  state ilike ''active''';
	
	FOR REC_TBL IN EXECUTE QUERY_TBL
	LOOP	
	
		SITE_LIST := SITE_LIST ||  REC_TBL.application_nm || ',' ;

	END LOOP;

	SITE_LIST := substr(SITE_LIST, 1,len(SITE_LIST)-1);			
	Select length(SITE_LIST) - length(REPLACE(SITE_LIST,',','')) + 1 INTO SITE_COUNT;
	
	/* Copy data to a temp table*/
	--drop table web_analytics_dm.dm_behaviour_web_rpt;
	--create table web_analytics_dm.dm_behaviour_web_rpt as select * from web_analytics_dm.dm_behaviour_web;
	
	
	
	update web_analytics_dm.dm_behaviour_web_daily as o
	set company_name = u.company_name 
	from web_analytics_dm.dm_behaviour_web_daily as u 
	inner join 
	(select min(hit_number::int)as hit_number_min,visits, date, sites from web_analytics_dm.dm_behaviour_web_daily group by visits, date, sites) t2
	on (t2.visits=u.visits and u.date=t2.date and t2.sites=u.sites)
	where o.visits = u.visits and o.sites = u.sites  and o.date = u.date 
	and u.hit_number =t2.hit_number_min;


	update web_analytics_dm.dm_behaviour_web_daily as o
	set city = u.city 
	from web_analytics_dm.dm_behaviour_web_daily as u 
		inner join 
	(select min(hit_number::int)as hit_number_min,visits, date, sites from web_analytics_dm.dm_behaviour_web_daily group by visits, date, sites) t2
	on (t2.visits=u.visits and u.date=t2.date and t2.sites=u.sites)
	where o.visits = u.visits and o.sites = u.sites  and o.date = u.date 
	and u.hit_number =t2.hit_number_min;


	update web_analytics_dm.dm_behaviour_web_daily as o
	set state = u.state 
	from web_analytics_dm.dm_behaviour_web_daily as u 
	inner join 
	(select min(hit_number::int)as hit_number_min,visits, date, sites from web_analytics_dm.dm_behaviour_web_daily group by visits, date, sites) t2
	on (t2.visits=u.visits and u.date=t2.date and t2.sites=u.sites)
	where o.visits = u.visits and o.sites = u.sites  and o.date = u.date 
	and u.hit_number =t2.hit_number_min;

	update web_analytics_dm.dm_behaviour_web_daily as o
	set exit = u.exit 
	from web_analytics_dm.dm_behaviour_web_daily as u 
	where o.visits = u.visits and o.sites = u.sites and o.date = u.date 
	and o.exit is null and u.exit is not null;



	update web_analytics_dm.dm_behaviour_web_daily as o
	set entrance = u.entrance 
	from web_analytics_dm.dm_behaviour_web_daily as u 
	where o.visits = u.visits and o.sites = u.sites and o.date = u.date 
	and o.entrance is null and u.entrance  is not null;

	update web_analytics_dm.dm_behaviour_web_daily as o
	set parent_company = u.parent_company 
	from web_analytics_dm.dm_behaviour_web_daily as u 
	inner join 
	(select min(hit_number::int)as hit_number_min,visits, date, sites from web_analytics_dm.dm_behaviour_web_daily group by visits, date, sites) t2
	on (t2.visits=u.visits and u.date=t2.date and t2.sites=u.sites)
	where o.visits = u.visits and o.sites = u.sites  and o.date = u.date 
	and u.hit_number =t2.hit_number_min;
--
	update web_analytics_dm.dm_behaviour_web_daily as o
	set sales_band = u.sales_band 
	from web_analytics_dm.dm_behaviour_web_daily as u 
	inner join 
	(select min(hit_number::int)as hit_number_min,visits, date, sites from web_analytics_dm.dm_behaviour_web_daily group by visits, date, sites) t2
	on (t2.visits=u.visits and u.date=t2.date and t2.sites=u.sites)
	where o.visits = u.visits and o.sites = u.sites  and o.date = u.date 
	and u.hit_number =t2.hit_number_min;

	update web_analytics_dm.dm_behaviour_web_daily as o
	set job_function = u.job_function 
	from web_analytics_dm.dm_behaviour_web_daily as u 
	inner join 
	(select min(hit_number::int)as hit_number_min,visits, date, sites from web_analytics_dm.dm_behaviour_web_daily group by visits, date, sites) t2
	on (t2.visits=u.visits and u.date=t2.date and t2.sites=u.sites)
	where o.visits = u.visits and o.sites = u.sites  and o.date = u.date 
	and u.hit_number =t2.hit_number_min;

	update web_analytics_dm.dm_behaviour_web_daily as o
	set total_employees_size = u.total_employees_size 
	from web_analytics_dm.dm_behaviour_web_daily as u 
	inner join 
	(select min(hit_number::int)as hit_number_min,visits, date, sites from web_analytics_dm.dm_behaviour_web_daily group by visits, date, sites) t2
	on (t2.visits=u.visits and u.date=t2.date and t2.sites=u.sites)
	where o.visits = u.visits and o.sites = u.sites  and o.date = u.date 
	and u.hit_number =t2.hit_number_min;



	Update web_analytics_dm.dm_behaviour_web_daily
	set pageviews=t2.maxpageview
	from web_analytics_dm.dm_behaviour_web_daily t1
	inner join 
	(select max(pageviews)as maxpageview,visits, date, sites from web_analytics_dm.dm_behaviour_web_daily group by visits, date, sites) t2
	on t2.visits=t1.visits and t1.date=t2.date and t2.sites=t1.sites;  


	update web_analytics_dm.dm_behaviour_web_daily as o
	set company_sic_codes = u.company_sic_codes 
	from web_analytics_dm.dm_behaviour_web_daily as u  
	where o.visits = u.visits and o.sites = u.sites  and o.date = u.date 
	and o.company_sic_codes is null and u.company_sic_codes is not null;
	
	/*
	update web_analytics_dm.dm_behaviour_web_daily as o
	set landing_page_trim = u.landing_page_trim 
	from web_analytics_dm.dm_behaviour_web_daily as u 
	where o.visits = u.visits and o.sites = u.sites  and o.date = u.date and u.hit_number =1;
	*/
	
	update web_analytics_dm.dm_behaviour_web_daily as o
	set landing_page_trim = u.landing_page_trim 
	from web_analytics_dm.dm_behaviour_web_daily as u 
	inner join 
	(select min(hit_number::int)as hit_number_min,visits, date, sites from web_analytics_dm.dm_behaviour_web_daily group by visits, date, sites) t2
	on (t2.visits=u.visits and u.date=t2.date and t2.sites=u.sites)
	where o.visits = u.visits and o.sites = u.sites  and o.date = u.date 
	and u.hit_number =t2.hit_number_min;	
	
	
	
	update web_analytics_dm.dm_behaviour_web_daily as o
	set hostname = u.hostname 
	from web_analytics_dm.dm_behaviour_web_daily as u 
	inner join 
	(select min(hit_number::int)as hit_number_min,visits, date, sites from web_analytics_dm.dm_behaviour_web_daily group by visits, date, sites) t2
	on (t2.visits=u.visits and u.date=t2.date and t2.sites=u.sites)
	where o.visits = u.visits and o.sites = u.sites  and o.date = u.date 
	and u.hit_number =t2.hit_number_min;	


	update web_analytics_dm.dm_behaviour_web_daily as o
	set industry_desc = u.industry_description , industry_grp = u.group_nm
	from web_analytics_dm.industry_tyre_group as u  
	where o.company_sic_codes = u.sic_code;


	update web_analytics_dm.dm_behaviour_web_daily as o
	set industry_desc = u.industry_description  , industry_grp = u.group_nm
	from web_analytics_dm.industry_tyre_group as u  
	where SUBSTRING(o.company_sic_codes,1,6) = u.sic_code and o.industry_desc  is null;

	update web_analytics_dm.dm_behaviour_web_daily as o
	set industry_desc = u.industry_description  , industry_grp = u.group_nm
	from web_analytics_dm.industry_tyre_group as u  
	where SUBSTRING(o.company_sic_codes,1,4) = u.sic_code and o.industry_desc  is null;


	update web_analytics_dm.dm_behaviour_web_daily as o
	set industry_desc = u.industry_description  , industry_grp = u.group_nm
	from web_analytics_dm.industry_tyre_group as u  
	where SUBSTRING(o.company_sic_codes,1,2) = u.sic_code and o.industry_desc  is null;
	
	update web_analytics_dm.dm_behaviour_web_daily as o
	set industry_desc = 'Unknown' , industry_grp = 'Unknown'
	where o.company_sic_codes is null;
	
	------------------------------------------------------------------------------------------------
	

	update web_analytics_dm.dm_behaviour_web_daily as o
	set country = u.country 
	from web_analytics_dm.dm_behaviour_web_daily as u 
	inner join 
	(select min(hit_number::int)as hit_number_min,visits, date, sites from web_analytics_dm.dm_behaviour_web_daily group by visits, date, sites) t2
	on (t2.visits=u.visits and u.date=t2.date and t2.sites=u.sites)
	where o.visits = u.visits and o.sites = u.sites  and o.date = u.date 
	and u.hit_number =t2.hit_number_min;	

	update web_analytics_dm.dm_behaviour_web_daily as o
	set region = u.region 
	from web_analytics_dm.dm_behaviour_web_daily as u 
	inner join 
	(select min(hit_number::int)as hit_number_min,visits, date, sites from web_analytics_dm.dm_behaviour_web_daily group by visits, date, sites) t2
	on (t2.visits=u.visits and u.date=t2.date and t2.sites=u.sites)
	where o.visits = u.visits and o.sites = u.sites  and o.date = u.date 
	and u.hit_number =t2.hit_number_min;	
	
	
	/*update web_analytics_dm.dm_behaviour_web_daily as o
	set fullvisitorid = u.fullvisitorid 
	from web_analytics_dm.dm_behaviour_web_daily as u 
	where o.visits = u.visits and o.sites = u.sites  and o.date = u.date and u.hit_number =1;*/

	
	------------------------------------------------------------------------------------------------

	update web_analytics_dm.dm_behaviour_web_daily 
	set relevance = case when company_isp = 1 then 'ISP' when industry_grp = 'Unknown' then 'Unknown' when industry_grp in ('Distribution and Wholesale', 'Finance, insurance & Real Estate', 'Electronics & communication' ) then 'Lower Relevance' else  'Higher Relevance' END
	where relevance is null;
	
	
	/* Delete from Update table for exisiting data */
	WHILE cnt < SITE_COUNT 
	LOOP

			SITE_NM := SPLIT_PART(SITE_LIST,',',cnt + 1 );
			QUERY_DELETE := 'DELETE FROM web_analytics_dm.dm_behaviour_web_rpt WHERE sites = ''' || SITE_NM || ''' AND date IN (SELECT DISTINCT date from  web_analytics_dm.dm_behaviour_web_daily where sites = ''' || SITE_NM || ''' );';
			
			
			EXECUTE QUERY_DELETE;
			
			
			QUERY_INSERT := 'INSERT INTO web_analytics_dm.dm_behaviour_web_rpt Select * from web_analytics_dm.dm_behaviour_web_daily where sites =  ''' || SITE_NM || ''' ;';	

			QUERY_DELETE_AGGR := 'DELETE FROM web_analytics_dm.dm_behaviour_web_aggr WHERE business_units = ''' || SITE_NM || ''' AND date IN (SELECT DISTINCT date from  web_analytics_dm.dm_behaviour_web_daily where sites = ''' || SITE_NM || ''' );';

			QUERY_DELETE_GOAL_AGGR := 'DELETE FROM web_analytics_dm.dm_behaviour_web_goal_aggr WHERE sites = ''' || SITE_NM || ''' AND date IN (SELECT DISTINCT date from  web_analytics_dm.dm_behaviour_web_daily where sites = ''' || SITE_NM || ''' );';			
			
			
			QUERY_DELETE_PG_AGGR := 'DELETE FROM web_analytics_dm.dm_behaviour_web_page_trim_aggr WHERE business_units = ''' || SITE_NM || ''' AND date IN (SELECT DISTINCT date from  web_analytics_dm.dm_behaviour_web_daily where sites = ''' || SITE_NM || ''' );';					
			
			EXECUTE QUERY_INSERT;
			EXECUTE QUERY_DELETE_AGGR;
			EXECUTE QUERY_DELETE_GOAL_AGGR;
			EXECUTE QUERY_DELETE_PG_AGGR;
			
			
				
			cnt := cnt + 1;
	
	END LOOP;
	
	
	/* Populate Aggregate Table */
	/* Populate Aggregate Table */
	
	
	--truncate table web_analytics_dm.dm_behaviour_web_aggr;
	INSERT INTO web_analytics_dm.dm_behaviour_web_aggr (siteurl_dt ,bu_dt ,business_units ,brand_nm ,division_nm ,site_url ,landing_page,channel_name ,source ,device_model ,device ,browser ,operating_system ,city ,state ,country ,region ,hostname ,language ,medium ,user_industry_name ,company_name ,parent_company ,sales_band ,job_function ,total_employees_size ,date ,relevance,industry_grp,new_user_count ,visits_count ,pageviews_sum ,bounce_sum ,entrance_count ,exit_count ,time_on_page_sum ,session_duration_sum  )
	Select site_url || date ,sites || date ,sites ,brand_nm ,division_nm ,site_url ,landing_page,channel_name ,source ,device_model ,device ,browser ,operating_system ,city ,state ,country ,region ,hostname ,language ,medium ,user_industry_name ,company_name ,parent_company ,sales_band ,job_function ,total_employees_size ,date ,relevance,industry_grp,new_user_sum ,visit_count ,pageview_sum ,bounce_sum ,entrance ,exit ,time_on_page ,session_duration from 
	(Select parent_company,sales_band,job_function,total_employees_size, site_url, date,sites,  division_nm, brand_nm,landing_page, channel_name,source,  device_model , device , browser , operating_system ,city ,state ,country ,region ,hostname ,language ,medium,user_industry_name,company_name,relevance,industry_grp,SUM(session_duration) session_duration ,SUM(time_on_page) time_on_page , count(visits) visit_count,count(new_user) new_user_sum, sum(pageviews) pageview_sum,sum(bounce)  bounce_sum, count(exit) exit, count(entrance) entrance   from 
	(Select distinct site_url, date,landing_page_trim as  landing_page,channel_name,source,  device_model , device , browser , operating_system,  sites,  division_nm , brand_nm ,city ,state ,country ,region ,hostname ,language,user_industry_name,medium ,company_name,job_function,total_employees_size, parent_company,relevance,industry_grp,visits, max(pageviews) as pageviews,sales_band, new_user, max(bounce) as bounce,exit as exit,entrance,time_on_page,session_duration   from web_analytics_dm.dm_behaviour_web_daily where relevance <> 'ISP' group by site_url, date,  landing_page_trim,channel_name,source,  device_model , device , browser , operating_system,  sites,  division_nm , brand_nm ,city ,state ,country ,region ,hostname ,language,user_industry_name,medium ,company_name,job_function,total_employees_size, parent_company,relevance,industry_grp,visits, sales_band, new_user, exit ,entrance,time_on_page,session_duration
	) group by sites ,brand_nm ,division_nm ,site_url ,landing_page,channel_name ,source ,device_model ,device ,browser ,operating_system ,city ,state ,country ,region ,hostname ,language ,medium ,user_industry_name ,company_name ,parent_company ,relevance,industry_grp,sales_band ,job_function ,total_employees_size ,date  );
	
	/* Populate Goal Aggregate Table */
	--truncate table web_analytics_dm.dm_behaviour_web_goal_aggr;
	INSERT INTO web_analytics_dm.dm_behaviour_web_goal_aggr (src_dt,siteurl_dt,sites ,industry_grp ,region ,channel_name ,city ,country ,state ,language ,date ,brand_nm ,division_nm ,site_url ,source ,device_model ,device ,browser ,operating_system ,landing_page ,hostname ,medium ,user_industry_name ,company_name ,parent_company ,sales_band ,job_function ,total_employees_size ,relevance ,goal ,goal_id,goal_count)
	Select sites || date , site_url || date , sites ,industry_grp ,region ,channel_name ,city ,country ,state ,language ,date ,brand_nm ,division_nm ,site_url ,source ,device_model ,device ,browser ,operating_system ,landing_page ,hostname ,medium ,user_industry_name ,company_name ,parent_company ,sales_band ,job_function ,total_employees_size ,relevance ,goal ,goal_id, SUM(goal_count) from 
	(Select sites ,industry_grp ,region ,channel_name ,city ,country ,state ,language ,date ,visits ,brand_nm ,division_nm ,site_url ,source ,device_model ,device ,browser ,operating_system ,landing_page ,hostname ,medium ,user_industry_name ,company_name ,parent_company ,sales_band ,job_function ,total_employees_size ,relevance ,goal ,goal_id, count(*) goal_count from 
	(Select distinct sites ,industry_grp ,region ,channel_name ,city ,country ,state ,language ,date ,visits ,brand_nm ,division_nm ,site_url ,source ,device_model ,device ,browser ,operating_system ,landing_page_trim as landing_page ,hostname ,medium ,user_industry_name ,company_name ,parent_company ,sales_band ,job_function ,total_employees_size ,relevance ,goal ,goal_id from web_analytics_dm.dm_behaviour_web_daily where goal is not null) group by sites ,industry_grp ,region ,channel_name ,city ,country ,state ,language ,date ,visits ,brand_nm ,division_nm ,site_url ,source ,device_model ,device ,browser ,operating_system ,landing_page ,hostname ,medium ,user_industry_name ,company_name ,parent_company ,sales_band ,job_function ,total_employees_size ,relevance ,goal ,goal_id )
	group by sites ,industry_grp ,region ,channel_name ,city ,country ,state ,language ,date ,brand_nm ,division_nm ,site_url ,source ,device_model ,device ,browser ,operating_system ,landing_page ,hostname ,medium ,user_industry_name ,company_name ,parent_company ,sales_band ,job_function ,total_employees_size ,relevance ,goal ,goal_id;
	

	--truncate table web_analytics_dm.dm_behaviour_web_page_trim_aggr;

		INSERT INTO web_analytics_dm.dm_behaviour_web_page_trim_aggr (siteurl_dt,bu_dt,business_units,brand_nm,division_nm,site_url,channel_name,source,device,landing_page,page,country,region,hostname,medium,user_industry_name,company_name,date,relevance,new_user_count,visits_count,pageviews_sum,bounce_sum,entrance_count,exit_count,time_on_page_sum,unique_page_views,search_count,unique_page_views_count,session_duration_sum) 

		(Select site_url ,sites || date ,sites ,brand_nm ,division_nm ,site_url ,channel_name ,source ,device ,landing_page ,page ,country ,region ,hostname ,medium ,user_industry_name ,company_name ,date ,relevance ,SUM(new_user_count) ,sum(visits_count) visits_count ,sum(pageviews_count) pageviews_count ,sum(bounce_count) ,sum(entrance_count) entrance_count ,sum(exit_count) exit_count ,sum(time_on_page_sum) ,sum(Unique_Page_Views_count) ,sum(search_exists_count) ,sum(Unique_Page_Views_count) ,sum(session_duration_sum) from 


		(Select A.date,A.site_url,A.sites,A.page,A.landing_page,A.channel_name,A.source,A.device,A.operating_system,A.division_nm ,A.brand_nm ,A.country ,A.region,A.hostname ,A.user_industry_name,A.medium ,A.company_name,A.relevance,A.visits,A.page_trim, SUM(B.visits_count) visits_count , sum(A.pagecount) pageviews_count, sum(B.new_user) new_user_count, sum(B.entrance) entrance_count, sum(B.bounce_count) bounce_count, sum(C.exit_count) exit_count, sum(B.time_on_page_sum) time_on_page_sum, sum(B.Unique_Page_Views_count) Unique_Page_Views_count, SUM(search_exists_count) search_exists_count, sum(session_duration_sum) session_duration_sum from         
		(Select     date,site_url,    sites,      page,visits, landing_page,channel_name,source,device,operating_system,division_nm ,brand_nm ,country ,region,hostname ,user_industry_name,medium ,company_name,relevance,search_exists ,Unique_Page_Views ,page_trim, sum(pagecount) pagecount from
			  (
			  Select distinct  site_url,date,   (case when page_trim = landing_page_trim THEN landing_page_trim else page_trim  END) as  landing_page ,page_trim page,channel_name,source,device ,operating_system,sites,division_nm ,brand_nm ,country ,region ,hostname ,user_industry_name,medium ,company_name,relevance,visits,new_user,max(bounce) bounce,exit ,entrance,time_on_page,MAX(session_duration) session_duration,search ,search_exists ,Unique_Page_Views ,page_trim ,count(page) pagecount
			  from        web_analytics_dm.dm_behaviour_web_daily
			  where             hits_type = 'PAGE'            AND relevance <> 'ISP' 
					--AND page_trim = landing_page_trim
					--and sites = 'SEF'
					--and date between  20210801 and 20210915
					--and page in ('/about-us','/about-us/careers','/brands','/brands/avdel','/brands/integra','/brands/nelson','/brands/optia','/brands/optia/dodge','/brands/proset','/catalog-global','/elu')
			  group by site_url,date,(case when page_trim = landing_page_trim THEN landing_page_trim else page_trim  END)     ,page_trim ,channel_name,source,device ,operating_system,sites,division_nm ,brand_nm ,country ,region ,hostname ,user_industry_name,medium ,company_name,relevance,visits,new_user,exit ,entrance,time_on_page,search ,search_exists ,Unique_Page_Views            )
		group by    page ,site_url,visits,  date, sites, landing_page,channel_name,source,device,operating_system,division_nm ,brand_nm ,country ,region,hostname ,user_industry_name,medium ,company_name,relevance,search_exists ,Unique_Page_Views ,page_trim) A
		LEFT OUTER JOIN         
		(Select     date,site_url,    sites,      page, landing_page,channel_name,source,device,operating_system,division_nm ,brand_nm ,country ,region,hostname ,user_industry_name,medium ,company_name,relevance,count(search_exists) search_exists_count ,count(Unique_Page_Views) Unique_Page_Views_count ,page_trim,visits,count(new_user) new_user,count(entrance) entrance, count(visits) visits_count,count(exit) exit_count,sum(time_on_page) time_on_page_sum,sum(session_duration) session_duration_sum,count(search) search_count, count(bounce) bounce_count  from
			  (
			  Select distinct   site_url,date,landing_page_trim landing_page ,page_trim page,channel_name,source,device ,operating_system,sites,division_nm ,brand_nm ,country ,region ,hostname ,user_industry_name,medium ,company_name,relevance,visits,new_user,max(bounce) bounce,exit ,entrance,time_on_page,MAX(session_duration) session_duration,search ,search_exists ,Unique_Page_Views ,page_trim 
			  from        web_analytics_dm.dm_behaviour_web_daily
			  where             hits_type = 'PAGE'            and relevance <> 'ISP' 
					and page_trim = landing_page_trim
					--and sites = 'SEF'
					--and date between  20210801 and 20210915
					--and page in ('/about-us','/about-us/careers','/brands','/brands/avdel','/brands/integra','/brands/nelson','/brands/optia','/brands/optia/dodge','/brands/proset','/catalog-global','/elu')
			  group by site_url,date,landing_page_trim ,page_trim ,channel_name,source,device ,operating_system,sites,division_nm ,brand_nm ,country ,region ,hostname ,user_industry_name,medium ,company_name,relevance,visits,new_user,exit ,entrance,time_on_page,search ,search_exists ,Unique_Page_Views            )
		group by    page ,site_url,   date, sites, landing_page,channel_name,source,device,operating_system,division_nm ,brand_nm ,country ,region,hostname ,user_industry_name,medium ,company_name,relevance,page_trim,visits, new_user,entrance) B

		on (
		coalesce(A.date,'') = coalesce(B.date,'') 
		AND coalesce(A.sites,'') = coalesce(B.sites,'')
		AND coalesce(A.page,'') = coalesce(B.page,'')
		AND coalesce(A.landing_page,'') = coalesce(B.landing_page,'')
		AND coalesce(A.channel_name,'') = coalesce(B.channel_name,'')
		AND coalesce(A.source,'') = coalesce(B.source,'')
		AND coalesce(A.device,'') = coalesce(B.device,'')
		AND coalesce(A.operating_system,'') = coalesce(B.operating_system,'')
		AND coalesce(A.division_nm ,'') = coalesce(B.division_nm ,'')
		AND coalesce(A.brand_nm ,'') = coalesce(B.brand_nm ,'')
		AND coalesce(A.country ,'') = coalesce(B.country ,'')
		AND coalesce(A.region,'') = coalesce(B.region,'')
		AND coalesce(A.hostname ,'') = coalesce(B.hostname ,'')
		AND coalesce(A.user_industry_name,'') = coalesce(B.user_industry_name,'')
		AND coalesce(A.medium ,'') = coalesce(B.medium ,'')
		AND coalesce(A.company_name,'') = coalesce(B.company_name,'')
		--AND coalesce(A.relevance,'') = coalesce(B.relevance,'')
		AND (coalesce(A.visits,'') = coalesce(B.visits,''))
		--AND coalesce(A.search_exists ,'') = coalesce(B.search_exists ,'')  
		--AND coalesce(A.Unique_Page_Views ,'') = coalesce(B.Unique_Page_Views ,'')  
		--AND coalesce(A.page_trim,'') = coalesce(B.page_trim ,'') 
		)
		LEFT OUTER JOIN 
		(Select     date,site_url,    sites,      page, landing_page,channel_name,source,device,operating_system,division_nm ,brand_nm ,country ,region,hostname ,user_industry_name,medium ,company_name,visits,count(exit) exit_count  from
			  (
			  Select distinct   site_url,date,landing_page_trim landing_page ,page_trim page,channel_name,source,device ,operating_system,sites,division_nm ,brand_nm ,country ,region ,hostname ,user_industry_name,medium ,company_name,relevance,visits,new_user,max(bounce) bounce,exit ,entrance,time_on_page,search ,search_exists ,Unique_Page_Views ,page_trim 
			  from        dm_behaviour_web_daily_temp
			  where             hits_type = 'PAGE' 
			  --    and relevance <> 'ISP' 
			  --    and page_trim = landing_page_trim
					and exit = 'true'
					--and sites = 'SEF'
					--and date between  20210801 and 20210915
					--and page in ('/about-us','/about-us/careers','/brands','/brands/avdel','/brands/integra','/brands/nelson','/brands/optia','/brands/optia/dodge','/brands/proset','/catalog-global','/elu')
			  group by site_url,date,landing_page_trim ,page_trim ,channel_name,source,device ,operating_system,sites,division_nm ,brand_nm ,country ,region ,hostname ,user_industry_name,medium ,company_name,relevance,visits,new_user,exit ,entrance,time_on_page,search ,search_exists ,Unique_Page_Views            )
		group by    page ,site_url,   date, sites, landing_page,channel_name,source,device,operating_system,division_nm ,brand_nm ,country ,region,hostname ,user_industry_name,medium ,company_name,relevance,page_trim,visits, new_user,entrance) C
		on (
		coalesce(A.date,'') = coalesce(C.date,'') 
		AND coalesce(A.sites,'') = coalesce(C.sites,'')
		--AND coalesce(A.page,'') = coalesce(B.page,'')
		AND coalesce(A.landing_page,'') = coalesce(C.landing_page,'')
		AND coalesce(A.channel_name,'') = coalesce(C.channel_name,'')
		AND coalesce(A.source,'') = coalesce(C.source,'')
		AND coalesce(A.device,'') = coalesce(C.device,'')
		AND coalesce(A.operating_system,'') = coalesce(C.operating_system,'')
		AND coalesce(A.division_nm ,'') = coalesce(C.division_nm ,'')
		AND coalesce(A.brand_nm ,'') = coalesce(C.brand_nm ,'')
		AND coalesce(A.country ,'') = coalesce(C.country ,'')
		AND coalesce(A.region,'') = coalesce(C.region,'')
		AND coalesce(A.hostname ,'') = coalesce(C.hostname ,'')
		AND coalesce(A.user_industry_name,'') = coalesce(C.user_industry_name,'')
		AND coalesce(A.medium ,'') = coalesce(C.medium ,'')
		AND coalesce(A.company_name,'') = coalesce(C.company_name,'')
		AND (coalesce(A.visits,'') = coalesce(C.visits,''))
		AND coalesce(A.page,'') = coalesce(C.page ,'') 
		)

		group by A.date,A.site_url,A.sites,A.page,A.landing_page,A.channel_name,A.source,A.device,A.operating_system,A.division_nm ,A.brand_nm ,A.country ,A.region,A.hostname ,A.user_industry_name,A.medium ,A.company_name,A.relevance,A.visits ,A.page_trim) 

		group by 
		site_url ,sites || date ,sites ,brand_nm ,division_nm ,site_url ,channel_name ,source ,device ,landing_page ,page ,country ,region ,hostname ,medium ,user_industry_name ,company_name ,date ,relevance);	
	
	




	END;





$$
;

CREATE OR REPLACE PROCEDURE web_analytics_dm.sp_data_load_sfdc($1 varchar)
	LANGUAGE plpgsql
AS $$
	

  DECLARE
	REC_TBL_DETAIL RECORD;
	QUERY_LIST  varchar(500);
	count INT DEFAULT 0;
	SITE_LIST varchar(5000) := '';
	SITE_COUNT INT;
	SITE_NM varchar(50);
	QUERY_INSERT  varchar(65535);
	QUERY_DELETE  varchar(65535);
	ROWS_INSERTED_COUNT BIGINT;
	CURRENT_RUN_SEQ INT :=0;
	CURRENT_JOB_NAME varchar(65535); 

	
	--Columns details Variables
	REC_COLUMN_LIST RECORD;
	QUERY_COLUMN_LIST  varchar(500);
	SRC_COLUMNS_LIST VARCHAR(65535) := '';
	TGT_COLUMNS_LIST VARCHAR(65535) := '';
	
	

	
  BEGIN
  
	SITE_NM := $1;
	
	/* Get Table's detail from control Table*/
	
	Select source ,application_nm ,dataset_nm ,job_nm ,description ,tgt_tbl_nm ,src_tbl_nm ,src_filter,website_url INTO REC_TBL_DETAIL  from ctrl.job_master where source ilike '%SFDC%' and application_nm = SITE_NM;
	
	CURRENT_JOB_NAME := REC_TBL_DETAIL.job_nm;
				
	SELECT COALESCE(max(run_seq),0)+1 into CURRENT_RUN_SEQ from ctrl.job_run_history where job_nm = CURRENT_JOB_NAME AND DATE(GETDATE()) = DATE(run_date);
	
	
	INSERT INTO ctrl.job_run_history (project,job_nm,run_date,run_seq,start_time,job_status) values ('WEB_ANALYTICS',CURRENT_JOB_NAME,DATE(GETDATE()),CURRENT_RUN_SEQ,GETDATE(),'RUNNING');
	
	
	
	
	/* Get List of All Google Analytics Tables*/
	QUERY_COLUMN_LIST := 'Select src_column_nm , tgt_column_nm, transformation,datatype, description , job_nm  from ctrl.job_detail where job_nm ilike ''%SFDC%'' and src_type ilike ''' || SITE_NM  ||  '''';



	FOR REC_COLUMN_LIST IN EXECUTE QUERY_COLUMN_LIST
	LOOP
		
		SRC_COLUMNS_LIST := SRC_COLUMNS_LIST || REC_COLUMN_LIST.src_column_nm || ',';
		
		IF LTRIM(RTRIM(REC_COLUMN_LIST.src_column_nm)) ilike 'business_unit'
		THEN
			TGT_COLUMNS_LIST := TGT_COLUMNS_LIST || '''' || SITE_NM || '''' || ',';	
		ELSIF LTRIM(RTRIM(REC_COLUMN_LIST.src_column_nm)) ilike 'SITES'
		THEN
			TGT_COLUMNS_LIST := TGT_COLUMNS_LIST || '''' || REC_TBL_DETAIL.website_url || '''' || ',';				
		ELSIF LTRIM(RTRIM(REC_COLUMN_LIST.datatype)) ilike '%integer%' OR LTRIM(RTRIM(REC_COLUMN_LIST.datatype)) ilike '%bigint%'
		THEN
			TGT_COLUMNS_LIST := TGT_COLUMNS_LIST || 'COALESCE(' || REC_COLUMN_LIST.tgt_column_nm ||  ',''0''),';			
		ELSIF LTRIM(RTRIM(REC_COLUMN_LIST.datatype)) ilike '%varchar%'
		THEN
			TGT_COLUMNS_LIST := TGT_COLUMNS_LIST || 'COALESCE(' || REC_COLUMN_LIST.tgt_column_nm ||  ',''''),';			
		ELSIF LTRIM(RTRIM(REC_COLUMN_LIST.datatype)) ilike '%decimal%'
		THEN
			TGT_COLUMNS_LIST := TGT_COLUMNS_LIST || 'COALESCE(' || REC_COLUMN_LIST.tgt_column_nm ||  '::varchar,''0''),';								
		ELSE		
			TGT_COLUMNS_LIST := TGT_COLUMNS_LIST || REC_COLUMN_LIST.tgt_column_nm || ',';	
		END IF;
		
						
			
	END LOOP;
	
				
	SRC_COLUMNS_LIST := substr(SRC_COLUMNS_LIST, 1,len(SRC_COLUMNS_LIST)-1);
	TGT_COLUMNS_LIST := substr(TGT_COLUMNS_LIST, 1,len(TGT_COLUMNS_LIST)-1);

	
	
	/* Delete if record exists in DataMart for available dates */
	QUERY_DELETE := 'DELETE FROM ' || REC_TBL_DETAIL.tgt_tbl_nm || ' WHERE business_unit = ''' || SITE_NM || ''' ;';
	
	EXECUTE QUERY_DELETE;
	
	

	
	
	QUERY_INSERT := 'INSERT INTO ' || REC_TBL_DETAIL.tgt_tbl_nm || ' ( '  || SRC_COLUMNS_LIST || ' ) (SELECT ' || TGT_COLUMNS_LIST || ' FROM ' || REC_TBL_DETAIL.src_tbl_nm || '  ) ;';
	
	EXECUTE QUERY_INSERT;			
	GET DIAGNOSTICS ROWS_INSERTED_COUNT = ROW_COUNT;
	

	

	UPDATE ctrl.job_run_history SET	no_rows_ins = ROWS_INSERTED_COUNT	where run_seq = CURRENT_RUN_SEQ and job_nm = CURRENT_JOB_NAME AND DATE(GETDATE()) = DATE(run_date);


	UPDATE ctrl.job_run_history SET
	end_time = GETDATE(),
	job_status = 'COMPLETED'
	where run_seq = CURRENT_RUN_SEQ and job_nm = CURRENT_JOB_NAME AND DATE(GETDATE()) = DATE(run_date);
			
			

	

	END;

$$
;

