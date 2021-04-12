CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.stage_nrd_email_apps_invoice_updating_flag_in_load_flag_table`()
BEGIN

UPDATE stage_one.load_flag
SET	
	ETL_Completion_Flag=1,
	ETL_Completion_DateTime=current_date()	
WHERE 	Upper(DB)='EMAIL AND APPS INVOICE STAGING' AND Upper(ETL_GROUP) = 'EMAIL AND APPS INVOICE STAGING';
END;
