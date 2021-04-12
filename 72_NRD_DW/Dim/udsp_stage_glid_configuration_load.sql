CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_two_dw.udsp_stage_glid_configuration_load`()
BEGIN


DELETE stage_two_dw.stage_glid_configuration TARGET
where  TARGET.Glid_Configuration_Nk in
(
select concat(cast(Rec_Id as string),'-',name,'-',cast(Type as string),'-',cast(Attribute as string))
from stage_one.raw_glid_configuration
);

insert into stage_two_dw.stage_glid_configuration(
 Glid_Configuration_Nk
,Glid_Record_Id
,Glid_Description
,Glid_Tax_Code
,Glid_Segment_Name
,Glid_Segment_Description
,Glid_Parent_Segment_Name
,Glid_Parent_Segment_Description
,Glid_Account_Record_Type_Id
,Glid_Account_Record_Type_Description
,Glid_Account_Attribute_Id
,Glid_Account_Attribute_Description
,Glid_GL_Offset_Account
,Glid_GL_AR_Account
,Glid_Source_Id
,Record_Created_By
,Record_Created_Datetime
,Record_Updated_By
,Record_Updated_Datetime
,Glid_Source_Field_Nk
,Chk_Sum_Md5
)
Select  SRC.Glid_Configuration_Nk
       , cast(SRC.Glid_Record_Id as int64) as Glid_Record_Id
	   ,SRC.Glid_Description
	   ,SRC.Glid_Tax_Code
	   ,SRC.Glid_Segment_Name
	   ,SRC.Glid_Segment_Description
	   ,SRC.Glid_Parent_Segment_Name
	   ,SRC.Glid_Parent_Segment_Description
	   ,cast(SRC.Glid_Account_Record_Type_Id as int64) as Glid_Account_Record_Type_Id
	   ,SRC.Glid_Account_Record_Type_Description
	   ,cast(SRC.Glid_Account_Attribute_Id as int64) as Glid_Account_Attribute_Id
	   ,SRC.Glid_Account_Attribute_Description
	   ,SRC.Glid_GL_Offset_Account
	   ,SRC.Glid_GL_AR_Account
	   ,SRC.Glid_Source_Id
	   ,'SSIS_Stage_Glid_Configuration' AS Record_Created_By
	   ,SRC.Record_Created_Datetime
	   ,'SSIS_Stage_Glid_Configuration' AS Record_Updated_By
	   ,SRC.Record_Created_Datetime AS Record_Updated_Datetime
	   ,SRC.Glid_Source_Field_Nk
	   ,TO_BASE64(MD5( CONCAT(
		Glid_Configuration_Nk, '|'
       ,Glid_Description,'|'
       ,Glid_Tax_Code,'|'
	   ,Glid_Segment_Description,'|'
	   ,Glid_Parent_Segment_Name,'|'
       ,Glid_Parent_Segment_Description,'|'
       ,Glid_Account_Record_Type_Description,'|'
	   ,Glid_Account_Attribute_Description,'|'
       ,Glid_GL_Offset_Account,'|'
	   ,Glid_GL_AR_Account,'|'
       ,Glid_Source_Id,'|'
	   ) ) ) AS Chk_Sum_Md5
From (
Select 
Distinct 
		CAST(Rec_Id AS string),'-',name,'-',CAST(Type AS string),'-',CAST(Attribute AS string) AS Glid_Configuration_Nk,
		Rec_Id AS Glid_Record_Id,
		Descr AS Glid_Description,
		Tax_Code AS Glid_Tax_Code,
		name AS Glid_Segment_Name,
		Segment_Descr As Glid_Segment_Description,
		Parent_Segment AS Glid_Parent_Segment_Name,
		Parent_Segement_Description AS Glid_Parent_Segment_Description,
		Type AS Glid_Account_Record_Type_Id,
		CASE when LTRIM(RTRIM(cast(Type as string )))='1'  Then 'Unbilled'
			 When LTRIM(RTRIM(cast(Type as string )))='2'  Then 'Billed'
			 When LTRIM(RTRIM(cast(Type as string )))='4'  Then 'Unbilled_Earned'
			 When LTRIM(RTRIM(cast(Type as string )))='8'  Then 'Billed_Earned'
			 When LTRIM(RTRIM(cast(Type as string )))='16' Then 'Unbilled_Unearned'
			 When LTRIM(RTRIM(cast(Type as string )))='32' Then 'Billed_Unearned'
			 When LTRIM(RTRIM(cast(Type as string )))='64' Then 'Billed_Earned'
		Else 'Unknown' End  AS Glid_Account_Record_Type_Description,
		Attribute AS Glid_Account_Attribute_Id,
		CASE when LTRIM(RTRIM(cast( Attribute as string )))='1'  Then 'Net'
			 When LTRIM(RTRIM(cast( Attribute as string )))='2'  Then 'Disc'
			 When LTRIM(RTRIM(cast( Attribute as string )))='4'  Then 'Tax'
			 When LTRIM(RTRIM(cast( Attribute as string )))='8'  Then 'Gross'
		Else 'Unknown' End AS Glid_Account_Attribute_Description,
		Gl_Offset_Acct AS Glid_GL_Offset_Account,
		GL_Ar_Acct AS Glid_GL_AR_Account,
		Obj_Id0	AS Glid_Source_Id,
		CONCAT('Glid_Record_Id','-','Glid_Segment_Name','-','Glid_Account_Record_Type_Id','-','Glid_Account_Attribute_Id') AS Glid_Source_Field_Nk,
	    current_datetime() AS Record_Created_Datetime
From 
    stage_one.raw_glid_configuration
	) SRC;

END;
