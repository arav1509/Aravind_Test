CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_stage_nrd_dedicated_invocie_reset_load_flags`()
BEGIN
Delete from stage_one.load_flag where Upper(ETL_Group)='DEDICATED INVOICE STAGING';
INSERT INTO 
	  stage_one.load_flag(ETL_Completion_Flag,ETL_Completion_DateTime,DB,ETL_Group)
SELECT
	0 as ETL_Completion_Flag,
	cast('9999-12-31' as date)  as ETL_Completion_DateTime,
	'Dedicated Invoice Staging' as DB,
	'Dedicated Invoice Staging' as ETL_Group;
	call `rax-staging-dev`.stage_one.udsp_etl_refresh_dedicated_brm_tables_current_load();
END;
