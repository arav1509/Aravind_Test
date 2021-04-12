CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_two_dw.udsp_stage_item_load`()
BEGIN

	delete stage_two_dw.stage_item target
	where item_tag in
	(
	select item_tag
	from stage_one.raw_item source
	);

	insert into stage_two_dw.stage_item
	(Item_Tag
	,Item_Name
	,Item_Description
	,Item_Type
	,Item_Sub_Type
	,item_source_record_id	
	,Record_Created_By
	,Record_Created_Datetime
	,Record_Updated_By
	,Record_Updated_Datetime
	, Chk_Sum_Md5
	)
	SELECT 
		   Item_Tag
		  ,Item_Name
		  ,Item_Description
		  ,Item_Type
		  ,Item_Sub_Type
		  ,Rec_Id
		  ,'SSIS_Stage_Item' As Record_Created_By
		  ,current_datetime() AS Record_Created_Datetime
		  ,'SSIS_Stage_Item' As Record_Updated_By
		  ,current_datetime() AS Record_Updated_Datetime
		  , TO_BASE64(MD5( CONCAT(
			Item_Name, '|'
		   ,Item_Description,'|'
		   ,Item_Type,'|'
		   ,Item_Sub_Type,'|'
			) ) ) AS Chk_Sum_Md5
	FROM (
	SELECT 
		   Item_Tag
		  ,Item_Name
		  ,Item_Description
		  ,Item_Type
		  ,Item_Sub_Type
		  ,Rec_Id
	 FROM stage_one.raw_item ) SRC;
	 

call `rax-staging-dev.stage_two_dw.udsp_insert_missing_item_tags_stage_item`();
END;
