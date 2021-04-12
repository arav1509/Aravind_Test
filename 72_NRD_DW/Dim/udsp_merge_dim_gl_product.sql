CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_three_dw.udsp_merge_dim_gl_product`()
BEGIN



declare v_gl_product_key int64;
declare v_current_cst_time datetime default  `rax-staging-dev`.bq_functions.get_utc_to_cst_time(current_datetime());

set v_gl_product_key= (select ifnull(max(gl_product_key),0) from `rax-datamart-dev.corporate_dmart.dim_gl_product`);


MERGE INTO `rax-datamart-dev.corporate_dmart.dim_gl_product` as TARGET
	USING
	(
	  SELECT 
	  ROW_NUMBER() OVER()+v_gl_product_key as GL_PRODUCT_KEY
      ,GL_Product_Code
      ,InitCap(GL_Product_Name) AS GL_Product_Name
      ,GL_Product_Description
      ,cast(GL_Product_Created_Datetime_Utc as timestamp) as GL_Product_Created_Datetime_Utc
      ,cast(GL_Product_Created_Datetime_Cst as timestamp) as GL_Product_Created_Datetime_Cst
      ,InitCap('Unknown') AS GL_Product_Group
      ,InitCap(GL_Product_BU) As GL_Product_BU
      ,InitCap(GL_Product_BU_Name) As GL_Product_BU_Name
      ,InitCap(GL_Product_Internal_BU) As GL_Product_Internal_BU
      ,InitCap(GL_Product_Internal_BU_Name) As GL_Product_Internal_BU_Name
      ,InitCap(GL_Product_Parent_BU) As GL_Product_Parent_BU
      ,InitCap(GL_Product_Parent_BU_Name) As GL_Product_Parent_BU_Name
      ,GL_Product_Is_Ded_Churn_Base
      ,GL_Product_Is_Active
      ,cast(GL_Product_End_Date_Active as date) as GL_Product_End_Date_Active
      ,GL_Product_Code AS GL_Product_Nk
      ,CURRENT_timestamp() AS Effective_Start_Datetime_Utc
      ,TIMESTAMP('9999-12-31')AS Effective_End_Datetime_Utc
      ,v_current_cst_time AS Effective_Start_Datetime_Cst
      ,TIMESTAMP('9999-12-31') AS Effective_End_Datetime_Cst
      ,1 AS Current_Record
      ,'BRM' AS Source_System_Name
      ,'SSIS_Dim_GL_Product' AS Record_Created_By
      ,CURRENT_timestamp() AS Record_Created_Datetime
      ,'SSIS_Dim_GL_Product' AS Record_Updated_By
      ,CURRENT_timestamp() AS Record_Updated_Datetime
      ,Chk_Sum_Md5
      ,cast(GL_Product_Created_Datetime_Utc as timestamp)  AS Effective_Start_Datetime_Utc_Cret
      ,cast(GL_Product_Created_Datetime_Cst as timestamp)  AS Effective_Start_Datetime_Cst_Cret
  FROM `rax-staging-dev`.stage_two_dw.stage_gl_product
	)AS SOURCE
	ON
	(   
	    TARGET.GL_Product_Code = SOURCE.GL_Product_Code
	)
	WHEN NOT MATCHED BY TARGET
	THEN INSERT
      (
		 GL_PRODUCT_KEY
		,GL_Product_Code 
		,GL_Product_Name 
		,GL_Product_Description 
		,GL_Product_Created_Datetime_Utc 
		,GL_Product_Created_Datetime_Cst 
		,GL_Product_Group 
		,GL_Product_BU 
		,GL_Product_BU_Name 
		,GL_Product_Internal_BU 
		,GL_Product_Internal_BU_Name
		,GL_Product_Parent_BU 
		,GL_Product_Parent_BU_Name 
		,GL_Product_Is_Ded_Churn_Base 
		,GL_Product_Is_Active
		,GL_Product_End_Date_Active 
		,GL_Product_Nk 
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
		 GL_PRODUCT_KEY
		, SOURCE.GL_Product_Code 
		,SOURCE.GL_Product_Name 
		,SOURCE.GL_Product_Description 
		,SOURCE.GL_Product_Created_Datetime_Utc 
		,SOURCE.GL_Product_Created_Datetime_Cst 
		,SOURCE.GL_Product_Group 
		,SOURCE.GL_Product_BU 
		,SOURCE.GL_Product_BU_Name 
		,SOURCE.GL_Product_Internal_BU 
		,SOURCE.GL_Product_Internal_BU_Name
		,SOURCE.GL_Product_Parent_BU 
		,SOURCE.GL_Product_Parent_BU_Name 
		,SOURCE.GL_Product_Is_Ded_Churn_Base 
		,SOURCE.GL_Product_Is_Active
		,SOURCE.GL_Product_End_Date_Active 
		,SOURCE.GL_Product_Nk 
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
		TARGET.Effective_End_Datetime_Cst = cast(v_current_cst_time as timestamp),
		TARGET.Current_Record = 0,
		TARGET.Record_Updated_By = SOURCE.Record_Updated_By,
		TARGET.Record_Updated_Datetime = current_timestamp()
		;
		
INSERT into `rax-datamart-dev.corporate_dmart.dim_gl_product`
      (
      gl_product_key
		, GL_Product_Code 
		,GL_Product_Name 
		,GL_Product_Description 
		,GL_Product_Created_Datetime_Utc 
		,GL_Product_Created_Datetime_Cst 
		,GL_Product_Group 
		,GL_Product_BU 
		,GL_Product_BU_Name 
		,GL_Product_Internal_BU 
		,GL_Product_Internal_BU_Name
		,GL_Product_Parent_BU 
		,GL_Product_Parent_BU_Name 
		,GL_Product_Is_Ded_Churn_Base 
		,GL_Product_Is_Active
		,GL_Product_End_Date_Active 
		,GL_Product_Nk 
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
select  ROW_NUMBER() OVER()+v_gl_product_key as gl_product_key
    ,SOURCE.GL_Product_Code 
		,SOURCE.GL_Product_Name 
		,SOURCE.GL_Product_Description 
		,cast(SOURCE.GL_Product_Created_Datetime_Utc as timestamp) as GL_Product_Created_Datetime_Utc
		,cast(SOURCE.GL_Product_Created_Datetime_Cst as timestamp) as GL_Product_Created_Datetime_Cst
		,SOURCE.GL_Product_Group 
		,SOURCE.GL_Product_BU 
		,SOURCE.GL_Product_BU_Name 
		,SOURCE.GL_Product_Internal_BU 
		,SOURCE.GL_Product_Internal_BU_Name
		,SOURCE.GL_Product_Parent_BU 
		,SOURCE.GL_Product_Parent_BU_Name 
		,SOURCE.GL_Product_Is_Ded_Churn_Base 
		,SOURCE.GL_Product_Is_Active
		,cast(SOURCE.GL_Product_End_Date_Active  as date) as GL_Product_End_Date_Active
		,SOURCE.GL_Product_Nk 
		,cast(SOURCE.Effective_Start_Datetime_Utc_Cret  as timestamp) as Effective_Start_Datetime_Utc_Cret
		,cast(SOURCE.Effective_End_Datetime_Utc  as timestamp) as Effective_End_Datetime_Utc
		,cast(SOURCE.Effective_Start_Datetime_Cst_Cret  as timestamp) as Effective_Start_Datetime_Cst_Cret
		,cast(SOURCE.Effective_End_Datetime_Cst as timestamp) as Effective_End_Datetime_Cst
		,SOURCE.Current_Record 
		,SOURCE.Source_System_Name 
		,SOURCE.Record_Created_By 
		,cast(SOURCE.Record_Created_Datetime  as timestamp) as Record_Created_Datetime
		,SOURCE.Record_Updated_By 
		,cast(SOURCE.Record_Updated_Datetime  as timestamp) as Record_Updated_Datetime
		,SOURCE.Chk_Sum_Md5
    from
(
	  SELECT 
       GL_Product_Code
      ,InitCap(GL_Product_Name) AS GL_Product_Name
      ,GL_Product_Description
      ,GL_Product_Created_Datetime_Utc
      ,GL_Product_Created_Datetime_Cst
      ,InitCap('Unknown') AS GL_Product_Group
      ,InitCap(GL_Product_BU) As GL_Product_BU
      ,InitCap(GL_Product_BU_Name) As GL_Product_BU_Name
      ,InitCap(GL_Product_Internal_BU) As GL_Product_Internal_BU
      ,InitCap(GL_Product_Internal_BU_Name) As GL_Product_Internal_BU_Name
      ,InitCap(GL_Product_Parent_BU) As GL_Product_Parent_BU
      ,InitCap(GL_Product_Parent_BU_Name) As GL_Product_Parent_BU_Name
      ,GL_Product_Is_Ded_Churn_Base
      ,GL_Product_Is_Active
      ,GL_Product_End_Date_Active
      ,GL_Product_Code AS GL_Product_Nk
      ,CURRENT_DATETIME() AS Effective_Start_Datetime_Utc
      ,DATETIME('9999-12-31')AS Effective_End_Datetime_Utc
      ,v_current_cst_time AS Effective_Start_Datetime_Cst
      ,DATETIME('9999-12-31') AS Effective_End_Datetime_Cst
      ,1 AS Current_Record
      ,'BRM' AS Source_System_Name
      ,'SSIS_Dim_GL_Product' AS Record_Created_By
      ,CURRENT_DATETIME() AS Record_Created_Datetime
      ,'SSIS_Dim_GL_Product' AS Record_Updated_By
      ,CURRENT_DATETIME() AS Record_Updated_Datetime
      ,Chk_Sum_Md5
      ,GL_Product_Created_Datetime_Utc AS Effective_Start_Datetime_Utc_Cret
      ,GL_Product_Created_Datetime_Cst AS Effective_Start_Datetime_Cst_Cret
  FROM `rax-staging-dev`.stage_two_dw.stage_gl_product
	)AS SOURCE
  inner join `rax-datamart-dev.corporate_dmart.dim_gl_product` as TARGET
	ON
	(   
	    TARGET.GL_Product_Code = SOURCE.GL_Product_Code
	)and (		SOURCE.Chk_Sum_Md5 <> TARGET.Chk_Sum_Md5 )
	;
  
END;
