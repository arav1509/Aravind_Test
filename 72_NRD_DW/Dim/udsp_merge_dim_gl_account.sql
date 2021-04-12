CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_three_dw.udsp_merge_dim_gl_account`()
BEGIN

declare v_account_key int64;
declare v_current_cst_time datetime default  `rax-staging-dev`.bq_functions.get_utc_to_cst_time(current_datetime());

set v_account_key= (select ifnull(max(GL_ACCOUNT_KEY),0) from `rax-datamart-dev`.corporate_dmart.dim_gl_account);


MERGE INTO `rax-datamart-dev`.corporate_dmart.dim_gl_account as TARGET
	USING
	(
	SELECT 
		ROW_NUMBER() OVER()+v_account_key as GL_ACCOUNT_KEY
	  ,GL_Account_Id
      ,InitCap(GL_Account_Name) AS GL_Account_Name
      ,GL_Account_Description
      ,cast(GL_Account_Created_Datetime_Utc as timestamp) as GL_Account_Created_Datetime_Utc
      ,cast(GL_Account_Created_Datetime_Cst as timestamp) as GL_Account_Created_Datetime_Cst
      ,InitCap('Unknown') AS GL_Account_Group
      ,GL_Account_Is_Active
      ,GL_Account_End_Date_Active
      ,GL_Account_Id AS GL_Account_Nk
      ,current_timestamp() AS Effective_Start_Datetime_Utc
      ,timestamp('9999-12-31')  AS Effective_End_Datetime_Utc
	  , v_current_cst_time  AS Effective_Start_Datetime_Cst
	  ,timestamp('9999-12-31')  AS Effective_End_Datetime_Cst
      ,1 AS Current_Record
      ,'BRM' AS Source_System_Name
	  ,'SSIS_Dim_GL_Account' AS Record_Created_By
      ,current_timestamp()  AS Record_Created_Datetime --- @CURRENT_TIMESTAMP default is CST in SQl server
	  ,'SSIS_Dim_GL_Account' AS Record_Updated_By
      , current_timestamp()  AS Record_Updated_Datetime --- @CURRENT_TIMESTAMP default is CST in SQl server
    ,cast(GL_Account_Created_Datetime_Utc as timestamp) AS Effective_Start_Datetime_Utc_Cret
	  ,cast(GL_Account_Created_Datetime_Cst as timestamp) AS Effective_Start_Datetime_Cst_Cret
      ,Chk_Sum_Md5
	FROM `rax-staging-dev`.stage_two_dw.stage_gl_account
	)AS SOURCE
	ON
	(   
	    TARGET.GL_Account_Id = SOURCE.GL_Account_Id
	)
	
	WHEN NOT MATCHED BY TARGET
	THEN INSERT
      (
		 GL_ACCOUNT_KEY
		,GL_Account_Id
		,GL_Account_Name
		,GL_Account_Description
		,GL_Account_Created_Datetime_Utc
		,GL_Account_Created_Datetime_Cst
		,GL_Account_Group
		,GL_Account_Is_Active
		,GL_Account_End_Date_Active
		,GL_Account_Nk
		,Effective_Start_Datetime_Utc
		,Effective_End_Datetime_Utc
		,Effective_Start_Datetime_Cst
		,Effective_End_Datetime_Cst
		,Current_Record
		,Source_System_Name
		,Record_Created_By
		,Record_Created_Datetime
		,Record_Updated_By
		,Record_Updated_Datetime
		,Chk_Sum_Md5
	  )
	  Values
	  (
		 GL_ACCOUNT_KEY
		,SOURCE.GL_Account_Id
		,SOURCE.GL_Account_Name
		,SOURCE.GL_Account_Description
		,SOURCE.GL_Account_Created_Datetime_Utc
		,SOURCE.GL_Account_Created_Datetime_Cst
		,SOURCE.GL_Account_Group
		,SOURCE.GL_Account_Is_Active
		,SOURCE.GL_Account_End_Date_Active
		,SOURCE.GL_Account_Nk
		,SOURCE.Effective_Start_Datetime_Utc_Cret
		,SOURCE.Effective_End_Datetime_Utc
		,SOURCE.Effective_Start_Datetime_Cst_Cret
		,SOURCE.Effective_End_Datetime_Cst
		,SOURCE.Current_Record
		,SOURCE.Source_System_Name
		,SOURCE.Record_Created_By
		,SOURCE.Record_Created_Datetime
		,SOURCE.Record_Updated_By
		,SOURCE.Record_Updated_Datetime
		,SOURCE.Chk_Sum_Md5
	  )

	WHEN MATCHED AND 
	(
		SOURCE.Chk_Sum_Md5 <> TARGET.Chk_Sum_Md5 AND TARGET.Current_Record = 1
	)
	THEN UPDATE
	SET
		TARGET.Effective_End_Datetime_Utc = current_timestamp(),
		TARGET.Effective_End_Datetime_Cst =  cast(v_current_cst_time as timestamp),--@CURRENT_TIMESTAMP,
		TARGET.Current_Record = 0,
		TARGET.Record_Updated_By = SOURCE.Record_Updated_By,
		TARGET.Record_Updated_Datetime = current_timestamp()--@CURRENT_TIMESTAMP,		
	;
	insert into `rax-datamart-dev`.corporate_dmart.dim_gl_account( 
	GL_ACCOUNT_KEY
	,GL_Account_Id
	,GL_Account_Name
	,GL_Account_Description
	,GL_Account_Created_Datetime_Utc
	,GL_Account_Created_Datetime_Cst
	,GL_Account_Group
	,GL_Account_Is_Active
	,GL_Account_End_Date_Active
	,GL_Account_Nk
	,Effective_Start_Datetime_Utc
	,Effective_End_Datetime_Utc
	,Effective_Start_Datetime_Cst
	,Effective_End_Datetime_Cst
	,Current_Record
	,Source_System_Name
	,Record_Created_By
	,Record_Created_Datetime
	,Record_Updated_By
	,Record_Updated_Datetime
	,Chk_Sum_Md5
	)
	select  
	ROW_NUMBER() OVER()+v_account_key as GL_ACCOUNT_KEY
	,SOURCE.GL_Account_Id
	,SOURCE.GL_Account_Name
	,SOURCE.GL_Account_Description
	,cast(SOURCE.GL_Account_Created_Datetime_Utc as timestamp) as GL_Account_Created_Datetime_Utc
	,cast(SOURCE.GL_Account_Created_Datetime_Cst as timestamp) as GL_Account_Created_Datetime_Cst
	,SOURCE.GL_Account_Group
	,SOURCE.GL_Account_Is_Active
	,SOURCE.GL_Account_End_Date_Active
	,SOURCE.GL_Account_Nk
	,cast(SOURCE.Effective_Start_Datetime_Utc as timestamp) as Effective_Start_Datetime_Utc
	,cast(SOURCE.Effective_End_Datetime_Utc as timestamp) as Effective_End_Datetime_Utc
	,cast(SOURCE.Effective_Start_Datetime_Cst as timestamp) as Effective_Start_Datetime_Cst
	,cast(SOURCE.Effective_End_Datetime_Cst as timestamp) as Effective_End_Datetime_Cst
	,SOURCE.Current_Record
	,SOURCE.Source_System_Name
	,SOURCE.Record_Created_By
	,cast(SOURCE.Record_Created_Datetime as timestamp) as Record_Created_Datetime
	,SOURCE.Record_Updated_By
	,cast(SOURCE.Record_Updated_Datetime as timestamp) as Record_Updated_Datetime
	,SOURCE.Chk_Sum_Md5
	from
	(
	SELECT 
	   GL_Account_Id
      ,InitCap(GL_Account_Name) AS GL_Account_Name
      ,GL_Account_Description
      ,GL_Account_Created_Datetime_Utc
      ,GL_Account_Created_Datetime_Cst
      ,InitCap('Unknown') AS GL_Account_Group
      ,GL_Account_Is_Active
      ,GL_Account_End_Date_Active
      ,GL_Account_Id AS GL_Account_Nk
      ,current_datetime() AS Effective_Start_Datetime_Utc
      ,datetime('9999-12-31')  AS Effective_End_Datetime_Utc
	  , v_current_cst_time  AS Effective_Start_Datetime_Cst
	  ,datetime('9999-12-31')  AS Effective_End_Datetime_Cst
      ,1 AS Current_Record
      ,'BRM' AS Source_System_Name
	  ,'SSIS_Dim_GL_Account' AS Record_Created_By
      , current_datetime() as Record_Created_Datetime --- @CURRENT_TIMESTAMP default is CST in SQl server
	  ,'SSIS_Dim_GL_Account' AS Record_Updated_By
      , current_datetime() as Record_Updated_Datetime--- @CURRENT_TIMESTAMP default is CST in SQl server
      ,GL_Account_Created_Datetime_Utc AS Effective_Start_Datetime_Utc_Cret
	  ,GL_Account_Created_Datetime_Cst AS Effective_Start_Datetime_Cst_Cret
      ,Chk_Sum_Md5
	FROM `rax-staging-dev`.stage_two_dw.stage_gl_account
	) SOURCE
	inner join `rax-datamart-dev`.corporate_dmart.dim_gl_account trg
	on trg.GL_Account_Id = SOURCE.GL_Account_Id
	and 
	(
		SOURCE.Chk_Sum_Md5 <> trg.Chk_Sum_Md5
	)
	;
	
END;
