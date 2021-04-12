CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_raw_gl_product_load`()
BEGIN
create or replace table `rax-staging-dev.stage_one.raw_gl_product` as
SELECT
       GL_Product_Code,
	   Product_Description,
	   GL_Product_Created_Date,
	   GL_Product_BU,
	   GL_Product_BU_Name,
	   GL_Product_Internal_BU,
	   GL_Product_Internal_BU_Name,
	   GL_Product_Parent_BU,
	   GL_Product_Parent_BU_Name,
	   GL_Product_Is_Ded_Churn_Base,
	   CASE WHEN (CAST(END_DATE_ACTIVE AS STRING) IS NULL AND upper(Product_Description) LIKE '%DISABLE%')
			THEN SUBSTR(REPLACE(Product_Description,'.','/'),14,STRPOS(Product_Description,')')-14)
			WHEN CAST(END_DATE_ACTIVE AS STRING) IS NOT NULL
			THEN CAST(END_DATE_ACTIVE AS STRING)
			ELSE NULL
	   END AS END_DATE_ACTIVE,
	   Record_Created_Datetime
FROM (
SELECT DISTINCT
      LTRIM(RTRIM(CAST(IFNULL(ffv.flex_value,'Unknown') AS STRING))) As GL_Product_Code, 
	  LTRIM(RTRIM(CAST(IFNULL(IFNULL(CAST(FinGlProd.Product_Description AS STRING), CAST(ffvt.Description AS STRING)), 'Unknown') AS STRING)))  As Product_Description,
	  IFNULL(ffv.Creation_Date,'1900-01-01') AS GL_Product_Created_Date,
	  LTRIM(RTRIM(CAST(IFNULL(FinGlProd.Parent_Prod_Bu,'Unknown') AS STRING))) As GL_Product_BU,
	  LTRIM(RTRIM(CAST(IFNULL(FinGlProd.Parent_Prod_Bu_Name,'Unknown') AS STRING))) As GL_Product_BU_Name,
	  LTRIM(RTRIM(CAST(IFNULL(FinGlProd.Pres_Layer,'Unknown') AS STRING))) As GL_Product_Internal_BU,
	  LTRIM(RTRIM(CAST(IFNULL(FinGlProd.Pres_Layer_Name,'Unknown') AS STRING))) As GL_Product_Internal_BU_Name,
	  LTRIM(RTRIM(CAST(IFNULL(FinGlProd.Top_Parent_Prod_Bu,'Unknown') AS STRING))) As GL_Product_Parent_BU,
	  LTRIM(RTRIM(CAST(IFNULL(FinGlProd.Top_Parent_Prod_Bu_Name,'Unknown') AS STRING))) As GL_Product_Parent_BU_Name,
	  IFNULL(FinGlProd.Ded_churn_base,'0') As GL_Product_Is_Ded_Churn_Base,
	  ffv.END_DATE_ACTIVE As End_Date_Active,
	  CURRENT_DATETIME() AS Record_Created_Datetime
FROM
      `rax-landing-qa`.ebs_ods.raw_fnd_flex_values ffv
	  left outer join
      `rax-landing-qa`.ebs_ods.raw_fnd_flex_value_sets ffvs
		on ffv.flex_value_set_id = ffvs.flex_value_set_id
	  left outer join
      `rax-landing-qa`.ebs_ods.raw_fnd_flex_values_tl ffvt
		on ffv.flex_value_id = ffvt.flex_value_id 
	  left join `rax-landing-dev`.r_accounting.fin_prod_bu finglprod  
	   on finglprod.product_number=flex_value
WHERE
      LOWER(ffvs.flex_value_set_name) IN ('rs_product') ) SRC
order by 1;

END;
