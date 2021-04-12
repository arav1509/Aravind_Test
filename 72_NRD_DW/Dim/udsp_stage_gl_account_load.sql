CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_two_dw.udsp_stage_gl_account_load`()
BEGIN

	  
UPDATE stage_two_dw.stage_gl_account
SET GL_ACCOUNT_END_DATE_ACTIVE=current_date()
   ,GL_ACCOUNT_IS_ACTIVE='N'
   ,Chk_Sum_Md5=TO_BASE64(MD5( CONCAT(
   A.GL_Account_Name, '|'
        ,A.GL_Account_Description,'|'
        ,A.GL_Account_Group,'|'
     ,'N','|'
     ,current_date()
   ) ) )
from stage_two_dw.stage_gl_account a
left join stage_one.raw_gl_account b
on a.gl_account_id=b.flex_value
where a.gl_account_end_date_active is null and b.flex_value is null;


delete from stage_two_dw.stage_gl_account
where  gl_account_id in
(
select b.flex_value
from stage_one.raw_gl_account b
);

insert into stage_two_dw.stage_gl_account
(GL_Account_Id
,GL_Account_Name
,GL_Account_Description
,GL_Account_Created_Datetime_Utc
,GL_Account_Created_Datetime_Cst
,GL_Account_Group
,GL_ACCOUNT_Is_Active
,gl_account_end_date_active
,Record_Created_Datetime
,Record_Updated_Datetime
,Chk_Sum_Md5
,record_created_by
,record_updated_by
)
SELECT
GL_Account_Id
,GL_Account_Name
,GL_Account_Description
,GL_Account_Created_Datetime_Utc
,GL_Account_Created_Datetime_Cst
,GL_Account_Group
,GL_ACCOUNT_Is_Active
,date(END_DATE_ACTIVE) as gl_account_end_date_active
,Record_Created_Datetime
,Record_Updated_Datetime
,TO_BASE64(MD5( CONCAT(
GL_Account_Name, '|'
,GL_Account_Description,'|'
,GL_Account_Group,'|'
,GL_ACCOUNT_Is_Active ,'|'
,END_DATE_ACTIVE
) ) ) AS Chk_Sum_Md5
,'udsp_stage_gl_account_load' as  record_created_by
,'udsp_stage_gl_account_load' as  record_updated_by
FROM
(
SELECT
SRC.GL_Account_Id
,SRC.GL_Account_Name
,SRC.GL_Account_Description
,SRC.GL_Account_Created_Datetime_Utc
,SRC.GL_Account_Created_Datetime_Cst
,SRC.GL_Account_Group
,CASE WHEN SRC.END_DATE_ACTIVE IS NOT
NULL THEN 'N' ELSE 'Y' END AS
GL_ACCOUNT_Is_Active
,SRC.END_DATE_ACTIVE
,1 AS Current_Record
,SRC.Source_System_Name
,SRC.Record_Created_Datetime
,SRC.Record_Updated_Datetime
FROM
(
SELECT
Flex_Value AS GL_Account_Id
,Description AS GL_Account_Name
,Description AS GL_Account_Description
,bq_functions.get_cst_to_utc_time(Creation_Date) AS GL_Account_Created_Datetime_Utc
,Creation_Date AS GL_Account_Created_Datetime_Cst
,'Unknown' AS GL_Account_Group
,CASE 	WHEN (END_DATE_ACTIVE IS NULL AND	(Description LIKE '%DISABLE%' and REGEXP_INSTR(Description,'[0-9]')<>0))
			   THEN PARSE_DATE('%m/%d/%Y',(replace(substr(Description,13,11),'.','/')))
		    WHEN END_DATE_ACTIVE IS NOT NULL
			   THEN END_DATE_ACTIVE
		    ELSE NULL
END AS END_DATE_ACTIVE
,Flex_Value AS GL_Account_Nk
,'BRM' AS Source_System_Name
,current_datetime() AS Record_Created_Datetime
,current_datetime()  AS Record_Updated_Datetime 
FROM stage_one.raw_gl_account  
)SRC

)A;

END;
