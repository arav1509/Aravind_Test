CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_stage_nrd_dedicated_invocie_updating_flag_in_load_flag_table`()
BEGIN

UPDATE stage_one.load_flag
SET	
	ETL_Completion_Flag=1,
	ETL_Completion_DateTime=current_date()	
WHERE 	Upper(DB)='DEDICATED INVOICE STAGING' AND Upper(ETL_GROUP) = 'DEDICATED INVOICE STAGING';
END;
