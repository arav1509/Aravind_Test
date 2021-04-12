CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.stage_nrd_email_apps_invoice_reset_load_flags`()
BEGIN

Delete from stage_one.load_flag where Upper(ETL_Group)='EMAIL AND APPS INVOICE STAGING';
INSERT INTO 
	  stage_one.load_flag(ETL_Completion_Flag,ETL_Completion_DateTime,DB,ETL_Group)
SELECT
	0 as ETL_Completion_Flag,
	cast('9999-12-31' as date)  as ETL_Completion_DateTime,
	'Email and Apps Invoice Staging' as DB,
	'Email and Apps Invoice Staging'as ETL_Group;
	
END;
