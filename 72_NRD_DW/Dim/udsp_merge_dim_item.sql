CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_three_dw.udsp_merge_dim_item`()
BEGIN


declare v_current_cst_time datetime default  `rax-staging-dev`.bq_functions.get_utc_to_cst_time(current_datetime());

declare v_item_key int64;

set v_item_key= (select ifnull(max(item_key),0) from `rax-datamart-dev`.corporate_dmart.dim_item);


MERGE INTO `rax-datamart-dev`.corporate_dmart.dim_item as TARGET
	USING
	(
	 SELECT 
	 ROW_NUMBER() OVER()+v_item_key as item_key
	  , Item_Tag
      ,InitCap(Item_Name) As Item_Name
      ,Item_Description
      ,Item_Type
      ,InitCap(Item_Sub_Type) As Item_Sub_Type
	  ,Item_Tag AS Item_Nk
      ,Item_Source_Record_Id
      ,current_timestamp() AS Effective_Start_Datetime_Utc
      ,timestamp('9999-12-31') AS Effective_End_Datetime_Utc
      ,v_current_cst_time AS Effective_Start_Datetime_Cst
      ,timestamp('9999-12-31')  AS Effective_End_Datetime_Cst
      ,1 AS Current_Record
      ,'BRM' AS Source_System_Name
      ,'SSIS_Dim_Item' AS Record_Created_By
      ,current_timestamp() AS Record_Created_Datetime
      ,'SSIS_Dim_Item' AS Record_Updated_By
      ,current_timestamp() AS Record_Updated_Datetime
      ,Chk_Sum_Md5
      ,timestamp('1900-01-01')  AS Effective_Start_Datetime_Utc_First
      ,timestamp('1900-01-01') AS Effective_Start_Datetime_Cst_First
	 FROM `rax-staging-dev`.stage_two_dw.stage_item
	)AS SOURCE
	ON
	(   
	    TARGET.Item_Tag = SOURCE.Item_Tag
	)
	WHEN NOT MATCHED BY TARGET
	THEN INSERT
      (
		 item_key
		,Item_Tag
		,Item_Name
		,Item_Description
		,Item_Type
		,Item_Sub_Type
		,Item_Nk
		,Item_Source_Record_Id
		,Effective_Start_Datetime_Utc
		,Effective_End_Datetime_Utc
		,Effective_Start_Datetime_Cst
		,Effective_End_Datetime_Cst
		,Current_Record
		,Record_Created_By
		,Record_Created_Datetime
		,Record_Updated_By
		,Record_Updated_Datetime
		,Source_System_Name
		,Chk_Sum_Md5
	  )
	  Values
	  (
		 SOURCE.item_key
	    ,SOURCE.Item_Tag
        ,SOURCE.Item_Name
        ,SOURCE.Item_Description
        ,SOURCE.Item_Type
        ,SOURCE.Item_Sub_Type
        ,SOURCE.Item_Nk
        ,SOURCE.Item_Source_Record_Id
        ,SOURCE.Effective_Start_Datetime_Utc_First
        ,SOURCE.Effective_End_Datetime_Utc
        ,SOURCE.Effective_Start_Datetime_Cst_First
        ,SOURCE.Effective_End_Datetime_Cst
        ,SOURCE.Current_Record
        ,SOURCE.Record_Created_By
        ,SOURCE.Record_Created_Datetime
        ,SOURCE.Record_Updated_By
        ,SOURCE.Record_Updated_Datetime
        ,SOURCE.Source_System_Name
        ,SOURCE.Chk_Sum_Md5
	  )
	WHEN MATCHED AND 
	(
		SOURCE.Chk_Sum_Md5 <> TARGET.Chk_Sum_Md5 AND TARGET.Current_Record = 1
	)

	THEN UPDATE
	SET
		TARGET.Effective_End_Datetime_Utc = current_timestamp(),
		TARGET.Effective_End_Datetime_Cst = cast(v_current_cst_time as timestamp),
		TARGET.Current_Record = 0,
		TARGET.Record_Updated_By = SOURCE.Record_Updated_By,
		TARGET.Record_Updated_Datetime = current_timestamp();
	

insert into `rax-datamart-dev`.corporate_dmart.dim_item
	(
		 item_key
		,Item_Tag
		,Item_Name
		,Item_Description
		,Item_Type
		,Item_Sub_Type
		,Item_Nk
		,Item_Source_Record_Id
		,Effective_Start_Datetime_Utc
		,Effective_End_Datetime_Utc
		,Effective_Start_Datetime_Cst
		,Effective_End_Datetime_Cst
		,Current_Record
		,Record_Created_By
		,Record_Created_Datetime
		,Record_Updated_By
		,Record_Updated_Datetime
		,Source_System_Name
		,Chk_Sum_Md5
	  )
	select
	src.item_key
	,src.Item_Tag
	,src.Item_Name
	,src.Item_Description
	,src.Item_Type
	,src.Item_Sub_Type
	,src.Item_Nk
	,src.Item_Source_Record_Id
	,src.Effective_Start_Datetime_Utc_First
	,src.Effective_End_Datetime_Utc
	,src.Effective_Start_Datetime_Cst_First
	,src.Effective_End_Datetime_Cst
	,src.Current_Record
	,src.Record_Created_By
	,src.Record_Created_Datetime
	,src.Record_Updated_By
	,src.Record_Updated_Datetime
	,src.Source_System_Name
	,src.Chk_Sum_Md5
	from
	(
	 SELECT 
	  ROW_NUMBER() OVER()+v_item_key as item_key
	  ,Item_Tag
      ,InitCap(Item_Name) As Item_Name
      ,Item_Description
      ,Item_Type
      ,InitCap(Item_Sub_Type) As Item_Sub_Type
	  ,Item_Tag AS Item_Nk
      ,Item_Source_Record_Id
      ,current_datetime() AS Effective_Start_Datetime_Utc
      ,timestamp('9999-12-31') AS Effective_End_Datetime_Utc
      ,v_current_cst_time AS Effective_Start_Datetime_Cst
      ,timestamp('9999-12-31')  AS Effective_End_Datetime_Cst
      ,1 AS Current_Record
      ,'BRM' AS Source_System_Name
      ,'SSIS_Dim_Item' AS Record_Created_By
      ,current_timestamp() AS Record_Created_Datetime
      ,'SSIS_Dim_Item' AS Record_Updated_By
      ,current_timestamp() AS Record_Updated_Datetime
      ,Chk_Sum_Md5
      ,timestamp('1900-01-01')  AS Effective_Start_Datetime_Utc_First
      ,timestamp('1900-01-01') AS Effective_Start_Datetime_Cst_First
	 FROM `rax-staging-dev`.stage_two_dw.stage_item
	) src
	inner join 
	`rax-datamart-dev`.corporate_dmart.dim_item as trg
	on ( trg.Item_Tag = src.Item_Tag) and (src.Chk_Sum_Md5 <> trg.Chk_Sum_Md5);
END;
