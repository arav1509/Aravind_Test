CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_two_dw.udsp_stage_gl_product`()
BEGIN


UPDATE stage_two_dw.stage_gl_product TARGET
SET 
    TARGET.GL_PRODUCT_END_DATE_ACTIVE = cast(current_date() as string)
   ,TARGET.GL_PRODUCT_IS_ACTIVE = 'N'
   ,TARGET.RECORD_UPDATED_DATETIME=current_datetime()
   ,TARGET.Chk_Sum_Md5 = TO_BASE64(MD5( CONCAT(
        TARGET.GL_Product_Name,'|'              
       ,TARGET.GL_Product_Description,'|'       
       ,TARGET.GL_Product_Group,'|'             
       ,TARGET.GL_Product_BU,'|'                
       ,TARGET.GL_Product_BU_Name,'|'           
       ,TARGET.GL_Product_Internal_BU,'|'       
       ,TARGET.GL_Product_Internal_BU_Name,'|' 
       ,TARGET.GL_Product_Parent_BU,'|'         
       ,TARGET.GL_Product_Parent_BU_Name,'|'    
       ,TARGET.GL_Product_Is_Ded_Churn_Base,'|'        
       ,'N','|'
       ,current_datetime()
      ) ) )

from stage_two_dw.stage_gl_product trg
left join stage_one.raw_gl_product source
on trg.gl_product_code = source.GL_PRODUCT_CODE
where trg.gl_product_end_date_active is null and source.GL_PRODUCT_CODE is null;


delete from stage_two_dw.stage_gl_product target
 where target.gl_product_code in (
 select  source.gl_product_code from stage_one.raw_gl_product source);



insert into `rax-staging-dev.stage_two_dw.stage_gl_product`(
gl_product_code,gl_product_name,gl_product_description,
gl_product_created_datetime_utc
		, gl_product_created_datetime_cst
		, gl_product_group
    , gl_product_bu
    ,gl_product_bu_name
    , gl_product_internal_bu
    , gl_product_internal_bu_name
		, gl_product_parent_bu
		, gl_product_parent_bu_name
    , gl_product_is_ded_churn_base
		, gl_product_is_active
        , gl_product_end_date_active
        ,Record_Created_By
        ,Record_Created_Datetime
        ,Record_Updated_By
        ,Record_Updated_Datetime
        , chk_sum_md5 )
SELECT   
         gl_product_code
		,GL_Product_Name
		,Product_Description
		,GL_Product_Created_Datetime_Utc
		,GL_Product_Created_Datetime_Cst
		,Gl_Product_Group
		,Parent_Prod_Bu
		,Parent_Prod_Bu_Name
		,Pres_Layer
		,Pres_Layer_Name
		,Top_Parent_Prod_Bu
		,Top_Parent_Prod_Bu_Name
		,cast(GL_Product_Is_Ded_Churn_Base as int64) as GL_Product_Is_Ded_Churn_Base
		,GL_Product_Is_Active
     ,End_Date_Active as gl_product_end_date_active
		,'SSIS_Stage_GL_Product' AS Record_Created_By
		,current_datetime() AS Record_Created_Datetime
		,'SSIS_Stage_GL_Product' AS Record_Updated_By
		,current_datetime() AS Record_Updated_Datetime
		,TO_BASE64(MD5( CONCAT(
		 GL_Product_Name, '|'
        ,Product_Description,'|'
        ,GL_Product_Group,'|'
	    ,Parent_Prod_Bu,'|'
	    ,Parent_Prod_Bu_Name,'|'
        ,Pres_Layer,'|'
        ,Pres_Layer_Name,'|'
	    ,Top_Parent_Prod_Bu,'|'
        ,Top_Parent_Prod_Bu_Name,'|'
	    ,GL_Product_Is_Ded_Churn_Base,'|'
        ,GL_Product_Is_Active,'|'
	    ,End_Date_Active
		 ) ) ) AS Chk_Sum_Md5
FROM (
SELECT 
		 SRC.gl_product_code
		,SRC.GL_Product_Name
		,SRC.Product_Description
		,SRC.GL_Product_Created_Datetime_Utc
		,SRC.GL_Product_Created_Datetime_Cst
		,SRC.Gl_Product_Group
		,SRC.Parent_Prod_Bu
		,SRC.Parent_Prod_Bu_Name
		,SRC.Pres_Layer
		,SRC.Pres_Layer_Name
		,SRC.Top_Parent_Prod_Bu
		,SRC.Top_Parent_Prod_Bu_Name
		,SRC.GL_Product_Is_Ded_Churn_Base
		,CASE WHEN SRC.End_Date_Active IS NOT NULL THEN 'N' ELSE 'Y' END AS GL_Product_Is_Active
		,SRC.End_Date_Active
FROM
(
SELECT gl_product_code
      ,Product_Description AS GL_Product_Name	
      ,Product_Description As Product_Description
      ,`rax-staging-dev`.bq_functions.get_cst_to_utc_time(GL_Product_Created_Date) AS GL_Product_Created_Datetime_Utc
      ,GL_Product_Created_Date AS GL_Product_Created_Datetime_Cst
      ,'Unknown' AS Gl_Product_Group
      ,GL_Product_Internal_BU As Parent_Prod_Bu
      ,GL_Product_Internal_BU_Name As Parent_Prod_Bu_Name
      ,GL_Product_Internal_BU As Pres_Layer
      ,GL_Product_Internal_BU_Name As Pres_Layer_Name
      ,GL_Product_Parent_BU As Top_Parent_Prod_Bu
      ,GL_Product_Parent_BU_Name As Top_Parent_Prod_Bu_Name
      ,GL_Product_Is_Ded_Churn_Base
	  ,CASE WHEN (END_DATE_ACTIVE IS NULL AND Product_Description LIKE '%DISABLE%')
			THEN SUBSTR(REPLACE(Product_Description,'.','/'),14,STRPOS(Product_Description,')')-14)
			WHEN END_DATE_ACTIVE IS NOT NULL
			THEN END_DATE_ACTIVE
			ELSE NULL
	   END AS End_Date_Active
  FROM stage_one.raw_gl_product) SRC
 ) A
 ;
 END;
