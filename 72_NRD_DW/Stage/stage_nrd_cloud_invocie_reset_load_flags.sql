CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.stage_nrd_cloud_invocie_reset_load_flags`()
BEGIN

/*
Reset Load Flags
*/
		
		Delete from stage_one.load_flag where lower(DB)='cloud invoice staging';
		
		INSERT INTO stage_one.load_flag(etl_completion_flag,etl_completion_datetime,db,etl_group)
		SELECT  0 AS etl_completion_flag, 
			   Cast('9999-12-31' AS DATETIME) AS etl_completion_datetime, 
			   'cloud invoice staging'                  AS db, 
			   'cloud invoice staging'                  AS etl_group; 


END;
