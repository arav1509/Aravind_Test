CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_three_dw.udsp_merge_dim_glid_configuration`()
BEGIN

--declare v_cst_current_date datetime default `rax-staging-dev`.bq_functions.get_utc_to_cst_time(current_datetime());
MERGE INTO `rax-datamart-dev`.corporate_dmart.dim_glid_configuration as TARGET
USING
	(
	 SELECT 
	   GLid_Configuration_Nk
      ,GLid_Record_Id
      ,InitCap(GLid_Description) AS GLid_Description
      ,GLid_Tax_Code
      ,GLid_Segment_Name
      ,GLid_Segment_Description
      ,GLid_Parent_Segment_Name
      ,GLid_Parent_Segment_Description
      ,GLid_Account_Record_Type_Id
      ,GLid_Account_Record_Type_Description
      ,GLid_Account_Attribute_Id
      ,InitCap(GLid_Account_Attribute_Description) AS GLid_Account_Attribute_Description
      ,GLid_GL_Offset_Account
      ,GLid_GL_AR_Account
      ,GLid_Source_Id
      ,'BRM' AS Source_System_Name
      ,'SSIS_Dim_Glid_Configuration' AS Record_Created_By
      ,current_timestamp() AS Record_Created_Datetime
      ,'SSIS_Dim_Glid_Configuration' AS Record_Updated_By
      ,current_timestamp() AS Record_Updated_Datetime
      ,GLid_Source_Field_NK
      ,Chk_Sum_Md5
     FROM `rax-staging-dev`.stage_two_dw.stage_glid_configuration
	)AS SOURCE
	ON
	(   
	    TARGET.GLid_Configuration_Nk = SOURCE.GLid_Configuration_Nk
	)

	
	WHEN NOT MATCHED BY TARGET
	THEN INSERT
      (
	     GLid_Configuration_Nk
        ,GLid_Record_Id
        ,GLid_Description
        ,GLid_Tax_Code
        ,GLid_Segment_Name
        ,GLid_Segment_Description
        ,GLid_Parent_Segment_Name
        ,GLid_Parent_Segment_Description
        ,GLid_Account_Record_Type_Id
        ,GLid_Account_Record_Type_Description
        ,GLid_Account_Attribute_Id
        ,GLid_Account_Attribute_Description
        ,GLid_GL_Offset_Account
        ,GLid_GL_AR_Account
        ,GLid_Source_Id
        ,Source_System_Name
        ,Record_Created_By
        ,Record_Created_Datetime
        ,Record_Updated_By
        ,Record_Updated_Datetime
        ,GLid_Source_Field_NK
        ,Chk_Sum_Md5
	  )
	  Values
	  (
	     SOURCE.GLid_Configuration_Nk
        ,SOURCE.GLid_Record_Id
        ,SOURCE.GLid_Description
        ,SOURCE.GLid_Tax_Code
        ,SOURCE.GLid_Segment_Name
        ,SOURCE.GLid_Segment_Description
        ,SOURCE.GLid_Parent_Segment_Name
        ,SOURCE.GLid_Parent_Segment_Description
        ,SOURCE.GLid_Account_Record_Type_Id
        ,SOURCE.GLid_Account_Record_Type_Description
        ,SOURCE.GLid_Account_Attribute_Id
        ,SOURCE.GLid_Account_Attribute_Description
        ,SOURCE.GLid_GL_Offset_Account
        ,SOURCE.GLid_GL_AR_Account
        ,SOURCE.GLid_Source_Id
        ,SOURCE.Source_System_Name
        ,SOURCE.Record_Created_By
        ,SOURCE.Record_Created_Datetime
        ,SOURCE.Record_Updated_By
        ,SOURCE.Record_Updated_Datetime
        ,SOURCE.GLid_Source_Field_NK
        ,SOURCE.Chk_Sum_Md5
	  )

	WHEN MATCHED AND 
	(
		SOURCE.Chk_Sum_Md5 <> TARGET.Chk_Sum_Md5
	)

	THEN UPDATE
	SET
	     TARGET.GLid_Configuration_Nk                = SOURCE.GLid_Configuration_Nk,
         TARGET.GLid_Description                     = SOURCE.GLid_Description,
         TARGET.GLid_Tax_Code                        = SOURCE.GLid_Tax_Code,
         TARGET.GLid_Segment_Description             = SOURCE.GLid_Segment_Description,
         TARGET.GLid_Parent_Segment_Name             = SOURCE.GLid_Parent_Segment_Name,
         TARGET.GLid_Parent_Segment_Description      = SOURCE.GLid_Parent_Segment_Description,
         TARGET.GLid_Account_Record_Type_Description = SOURCE.GLid_Account_Record_Type_Description,
         TARGET.GLid_Account_Attribute_Description   = SOURCE.GLid_Account_Attribute_Description,
         TARGET.GLid_GL_Offset_Account               = SOURCE.GLid_GL_Offset_Account,
         TARGET.GLid_GL_AR_Account                   = SOURCE.GLid_GL_AR_Account,
         TARGET.GLid_Source_Id                       = SOURCE.GLid_Source_Id,
		 TARGET.GLid_Source_Field_NK                 = SOURCE.GLid_Source_Field_NK,
		 TARGET.Chk_Sum_Md5                          = SOURCE.Chk_Sum_Md5,
		 TARGET.Record_Updated_By                    = SOURCE.Record_Updated_By,
		 TARGET.Record_Updated_Datetime              = current_timestamp() --@CURRENT_TIMESTAMP
;

END;
