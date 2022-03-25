-- ctrl.job_master definition

-- Drop table

-- DROP TABLE ctrl.job_master;

--DROP TABLE ctrl.job_master;
CREATE TABLE IF NOT EXISTS ctrl.job_master
(
	source VARCHAR(100)   ENCODE lzo
	,application_nm VARCHAR(100)   ENCODE lzo
	,division VARCHAR(100)   ENCODE lzo
	,brand VARCHAR(100)   ENCODE lzo
	,dataset_nm VARCHAR(100)   ENCODE lzo
	,job_nm VARCHAR(100) NOT NULL  ENCODE lzo
	,description VARCHAR(500)   ENCODE lzo
	,tgt_tbl_nm VARCHAR(100)   ENCODE lzo
	,src_filter VARCHAR(100)   ENCODE lzo
	,state VARCHAR(100)   ENCODE lzo
	,crte_user VARCHAR(100)   ENCODE lzo
	,crte_ts VARCHAR(100)   ENCODE lzo
	,website_url VARCHAR(500)   ENCODE lzo
	,src_tbl_nm VARCHAR(2000)   ENCODE lzo
	,PRIMARY KEY (job_nm)
)
DISTSTYLE ALL
;
ALTER TABLE ctrl.job_master owner to base_admin;

-- Table Triggers

CREATE CONSTRAINT TRIGGER "RI_ConstraintTrigger_1019873" AFTER
DELETE
    ON
    ctrl.job_master
FROM
    ctrl.job_detail NOT DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE PROCEDURE "RI_FKey_noaction_del"('job_detail_job_nm_fkey',
    'job_detail',
    'job_master',
    'UNSPECIFIED',
    'job_nm',
    'job_nm');
CREATE CONSTRAINT TRIGGER "RI_ConstraintTrigger_1019874" AFTER
UPDATE
    ON
    ctrl.job_master
FROM
    ctrl.job_detail NOT DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE PROCEDURE "RI_FKey_noaction_upd"('job_detail_job_nm_fkey',
    'job_detail',
    'job_master',
    'UNSPECIFIED',
    'job_nm',
    'job_nm');
CREATE CONSTRAINT TRIGGER "RI_ConstraintTrigger_1019881" AFTER
DELETE
    ON
    ctrl.job_master
FROM
    ctrl.job_run_master NOT DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE PROCEDURE "RI_FKey_noaction_del"('job_run_master_job_nm_fkey',
    'job_run_master',
    'job_master',
    'UNSPECIFIED',
    'job_nm',
    'job_nm');
CREATE CONSTRAINT TRIGGER "RI_ConstraintTrigger_1019882" AFTER
UPDATE
    ON
    ctrl.job_master
FROM
    ctrl.job_run_master NOT DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE PROCEDURE "RI_FKey_noaction_upd"('job_run_master_job_nm_fkey',
    'job_run_master',
    'job_master',
    'UNSPECIFIED',
    'job_nm',
    'job_nm');
CREATE CONSTRAINT TRIGGER "RI_ConstraintTrigger_1019890" AFTER
DELETE
    ON
    ctrl.job_master
FROM
    ctrl.job_run_history NOT DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE PROCEDURE "RI_FKey_noaction_del"('job_run_history_job_nm_fkey',
    'job_run_history',
    'job_master',
    'UNSPECIFIED',
    'job_nm',
    'job_nm');
CREATE CONSTRAINT TRIGGER "RI_ConstraintTrigger_1019891" AFTER
UPDATE
    ON
    ctrl.job_master
FROM
    ctrl.job_run_history NOT DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE PROCEDURE "RI_FKey_noaction_upd"('job_run_history_job_nm_fkey',
    'job_run_history',
    'job_master',
    'UNSPECIFIED',
    'job_nm',
    'job_nm');
CREATE CONSTRAINT TRIGGER "RI_ConstraintTrigger_1019899" AFTER
DELETE
    ON
    ctrl.job_master
FROM
    ctrl.goal_master NOT DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE PROCEDURE "RI_FKey_noaction_del"('goal_master_job_nm_fkey',
    'goal_master',
    'job_master',
    'UNSPECIFIED',
    'job_nm',
    'job_nm');
CREATE CONSTRAINT TRIGGER "RI_ConstraintTrigger_1019900" AFTER
UPDATE
    ON
    ctrl.job_master
FROM
    ctrl.goal_master NOT DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE PROCEDURE "RI_FKey_noaction_upd"('goal_master_job_nm_fkey',
    'goal_master',
    'job_master',
    'UNSPECIFIED',
    'job_nm',
    'job_nm');


-- ctrl.goal_master definition

-- Drop table

-- DROP TABLE ctrl.goal_master;

--DROP TABLE ctrl.goal_master;
CREATE TABLE IF NOT EXISTS ctrl.goal_master
(
	id INTEGER NOT NULL DEFAULT "identity"(1019892, 0, '1,1'::text) ENCODE az64
	,source VARCHAR(100)   ENCODE lzo
	,job_nm VARCHAR(100)   ENCODE lzo
	,goal VARCHAR(100)   ENCODE lzo
	,goal_id VARCHAR(100)   ENCODE lzo
	,goal_type VARCHAR(100)   ENCODE lzo
	,condition_column_value_1 VARCHAR(100)   ENCODE lzo
	,condition_column_value_2 VARCHAR(100)   ENCODE lzo
	,condition_column_value_3 VARCHAR(100)   ENCODE lzo
	,condition_column_value_4 VARCHAR(100)   ENCODE lzo
	,state VARCHAR(100)   ENCODE lzo
	,PRIMARY KEY (id)
)
DISTSTYLE ALL
;
ALTER TABLE ctrl.goal_master owner to base_admin;

-- Table Triggers

CREATE CONSTRAINT TRIGGER "RI_ConstraintTrigger_1019898" AFTER
INSERT
    OR
UPDATE
    ON
    ctrl.goal_master
FROM
    ctrl.job_master NOT DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE PROCEDURE "RI_FKey_check_ins"('goal_master_job_nm_fkey',
    'goal_master',
    'job_master',
    'UNSPECIFIED',
    'job_nm',
    'job_nm');


-- ctrl.job_detail definition

-- Drop table

-- DROP TABLE ctrl.job_detail;

--DROP TABLE ctrl.job_detail;
CREATE TABLE IF NOT EXISTS ctrl.job_detail
(
	id INTEGER NOT NULL DEFAULT "identity"(1019866, 0, '1,1'::text) ENCODE az64
	,src_column_nm VARCHAR(500)   ENCODE lzo
	,tgt_column_nm VARCHAR(5000)   ENCODE lzo
	,datatype VARCHAR(100)   ENCODE lzo
	,src_type VARCHAR(100)   ENCODE lzo
	,transformation VARCHAR(500)   ENCODE lzo
	,description VARCHAR(500)   ENCODE lzo
	,crte_user VARCHAR(100)   ENCODE lzo
	,crte_ts VARCHAR(100)   ENCODE lzo
	,job_nm VARCHAR(100)   ENCODE lzo
	,PRIMARY KEY (id)
)
DISTSTYLE ALL
;
ALTER TABLE ctrl.job_detail owner to base_admin;

-- Table Triggers

CREATE CONSTRAINT TRIGGER "RI_ConstraintTrigger_1019872" AFTER
INSERT
    OR
UPDATE
    ON
    ctrl.job_detail
FROM
    ctrl.job_master NOT DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE PROCEDURE "RI_FKey_check_ins"('job_detail_job_nm_fkey',
    'job_detail',
    'job_master',
    'UNSPECIFIED',
    'job_nm',
    'job_nm');


-- ctrl.job_run_history definition

-- Drop table

-- DROP TABLE ctrl.job_run_history;

--DROP TABLE ctrl.job_run_history;
CREATE TABLE IF NOT EXISTS ctrl.job_run_history
(
	run_id INTEGER NOT NULL DEFAULT "identity"(1019883, 0, '1,1'::text) ENCODE az64
	,project VARCHAR(100)   ENCODE lzo
	,job_nm VARCHAR(100)   ENCODE lzo
	,run_date DATE   ENCODE az64
	,run_seq INTEGER   ENCODE az64
	,job_status VARCHAR(100)   ENCODE lzo
	,no_rows_ins INTEGER   ENCODE az64
	,start_time TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_time TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,PRIMARY KEY (run_id)
)
DISTSTYLE KEY
 DISTKEY (run_id)
;
ALTER TABLE ctrl.job_run_history owner to base_admin;

-- Table Triggers

CREATE CONSTRAINT TRIGGER "RI_ConstraintTrigger_1019889" AFTER
INSERT
    OR
UPDATE
    ON
    ctrl.job_run_history
FROM
    ctrl.job_master NOT DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE PROCEDURE "RI_FKey_check_ins"('job_run_history_job_nm_fkey',
    'job_run_history',
    'job_master',
    'UNSPECIFIED',
    'job_nm',
    'job_nm');


-- ctrl.job_run_master definition

-- Drop table

-- DROP TABLE ctrl.job_run_master;

--DROP TABLE ctrl.job_run_master;
CREATE TABLE IF NOT EXISTS ctrl.job_run_master
(
	job_nm VARCHAR(100) NOT NULL  ENCODE lzo
	,description VARCHAR(500)   ENCODE lzo
	,frequency VARCHAR(100)   ENCODE lzo
	,job_state VARCHAR(100)   ENCODE lzo
	,crte_user VARCHAR(100)   ENCODE lzo
	,crte_ts VARCHAR(100)   ENCODE lzo
	,PRIMARY KEY (job_nm)
)
DISTSTYLE ALL
;
ALTER TABLE ctrl.job_run_master owner to base_admin;

-- Table Triggers

CREATE CONSTRAINT TRIGGER "RI_ConstraintTrigger_1019880" AFTER
INSERT
    OR
UPDATE
    ON
    ctrl.job_run_master
FROM
    ctrl.job_master NOT DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE PROCEDURE "RI_FKey_check_ins"('job_run_master_job_nm_fkey',
    'job_run_master',
    'job_master',
    'UNSPECIFIED',
    'job_nm',
    'job_nm');
