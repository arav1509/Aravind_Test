CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.stage_nrd_cloud_invocie_update_load_flags`()
BEGIN

/*
Update Load Flags
*/
		
		UPDATE stage_one.load_flag
SET	ETL_Completion_Flag=1,
	ETL_Completion_DateTime=current_date()	
WHERE 	lower(DB)='cloud invoice staging' AND lower(ETL_GROUP) = 'cloud invoice staging';


END;
