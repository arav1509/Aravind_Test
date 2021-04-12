CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_three_dw.udsp_update_dim_hier_gl_account`()
BEGIN  

UPDATE
`rax-datamart-dev.corporate_dmart.dim_hier_gl_account`
SET
	Leaf_Member = CASE WHEN B.NumberOfChildren = 0 THEN 1 ELSE 0 END
	,Number_Of_Children = B.NumberOfChildren
FROM
	`rax-datamart-dev.corporate_dmart.dim_hier_gl_account` A,
	
			(SELECT
				M.GL_Account_Id
				,COUNT(C.GL_Account_Id) AS NumberOfChildren
				
			FROM
				`rax-datamart-dev.corporate_dmart.dim_hier_gl_account` M
				LEFT JOIN `rax-datamart-dev.corporate_dmart.dim_hier_gl_account` C
					ON M.GL_Account_Id = C.Parent_GL_Account_Id
			GROUP BY
				M.GL_Account_Id
		) B
	where A.gl_account_id = B.GL_Account_Id;
  
/*
 Update flattened hierarchy information
*/

Update 
		`rax-datamart-dev.corporate_dmart.dim_hier_gl_account`
Set
	 `rax-datamart-dev.corporate_dmart.dim_hier_gl_account`.GL_Account_Name=COALESCE(B.GL_Account_Name,'Unknown')
	,`rax-datamart-dev.corporate_dmart.dim_hier_gl_account`.Parent_GL_Account_Name=COALESCE(P.GL_Account_Name,'Unknown')
	,`rax-datamart-dev.corporate_dmart.dim_hier_gl_account`.Hier_Level_1=DOHL1.GL_Account_Id 
	,`rax-datamart-dev.corporate_dmart.dim_hier_gl_account`.Hier_Level_1_Name=DOHL1.GL_Account_Name 
	,`rax-datamart-dev.corporate_dmart.dim_hier_gl_account`.Hier_Level_2=COALESCE(DOHL2.GL_Account_Id,DOHL1.GL_Account_Id)
	,`rax-datamart-dev.corporate_dmart.dim_hier_gl_account`.Hier_Level_2_Name=COALESCE(DOHL2.GL_Account_Name,DOHL1.GL_Account_Name)
	,`rax-datamart-dev.corporate_dmart.dim_hier_gl_account`.Hier_Level_3=COALESCE(DOHL3.GL_Account_Id,DOHL2.GL_Account_Id,DOHL1.GL_Account_Id)
	,`rax-datamart-dev.corporate_dmart.dim_hier_gl_account`.Hier_Level_3_Name=COALESCE(DOHL3.GL_Account_Name,DOHL2.GL_Account_Name,DOHL1.GL_Account_Name)
	,`rax-datamart-dev.corporate_dmart.dim_hier_gl_account`.Hier_Level_4=COALESCE(DOHL4.GL_Account_Id,DOHL3.GL_Account_Id,DOHL2.GL_Account_Id,DOHL1.GL_Account_Id)
	,`rax-datamart-dev.corporate_dmart.dim_hier_gl_account`.Hier_Level_4_Name=COALESCE(DOHL4.GL_Account_Name,DOHL3.GL_Account_Name,DOHL2.GL_Account_Name,DOHL1.GL_Account_Name)
	,`rax-datamart-dev.corporate_dmart.dim_hier_gl_account`.Hier_Level_5=COALESCE(DOHL5.GL_Account_Id,DOHL4.GL_Account_Id,DOHL3.GL_Account_Id,DOHL2.GL_Account_Id,DOHL1.GL_Account_Id)
	,`rax-datamart-dev.corporate_dmart.dim_hier_gl_account`.Hier_Level_5_Name=COALESCE(DOHL5.GL_Account_Name,DOHL4.GL_Account_Name,DOHL3.GL_Account_Name,DOHL2.GL_Account_Name,DOHL1.GL_Account_Name)
	,`rax-datamart-dev.corporate_dmart.dim_hier_gl_account`.Hier_Level_6=COALESCE(DOHL6.GL_Account_Id,DOHL5.GL_Account_Id,DOHL4.GL_Account_Id,DOHL3.GL_Account_Id,DOHL2.GL_Account_Id,DOHL1.GL_Account_Id)
	,`rax-datamart-dev.corporate_dmart.dim_hier_gl_account`.Hier_Level_6_Name=COALESCE(DOHL6.GL_Account_Name,DOHL5.GL_Account_Name,DOHL4.GL_Account_Name,DOHL3.GL_Account_Name,DOHL2.GL_Account_Name,DOHL1.GL_Account_Name)
	,`rax-datamart-dev.corporate_dmart.dim_hier_gl_account`.Hier_Level_7=COALESCE(DOHL7.GL_Account_Id,DOHL6.GL_Account_Id,DOHL5.GL_Account_Id,DOHL4.GL_Account_Id,DOHL3.GL_Account_Id,DOHL2.GL_Account_Id,DOHL1.GL_Account_Id)
,`rax-datamart-dev.corporate_dmart.dim_hier_gl_account`.Hier_Level_7_Name=COALESCE(DOHL7.GL_Account_Name,DOHL6.GL_Account_Name,DOHL5.GL_Account_Name,DOHL4.GL_Account_Name,DOHL3.GL_Account_Name,DOHL2.GL_Account_Name,DOHL1.GL_Account_Name)
,`rax-datamart-dev.corporate_dmart.dim_hier_gl_account`.Hier_Level_8=COALESCE(DOHL8.GL_Account_Id,DOHL7.GL_Account_Id,DOHL6.GL_Account_Id,DOHL5.GL_Account_Id,DOHL4.GL_Account_Id,DOHL3.GL_Account_Id,DOHL2.GL_Account_Id,DOHL1.GL_Account_Id)
	,`rax-datamart-dev.corporate_dmart.dim_hier_gl_account`.Hier_Level_8_Name=COALESCE(DOHL8.GL_Account_Name,DOHL7.GL_Account_Name,DOHL6.GL_Account_Name,DOHL5.GL_Account_Name,DOHL4.GL_Account_Name,DOHL3.GL_Account_Name,DOHL2.GL_Account_Name,DOHL1.GL_Account_Name)
	,`rax-datamart-dev.corporate_dmart.dim_hier_gl_account`.Hier_Level_9=COALESCE(DOHL9.GL_Account_Id,DOHL8.GL_Account_Id,DOHL7.GL_Account_Id,DOHL6.GL_Account_Id,DOHL5.GL_Account_Id,DOHL4.GL_Account_Id,DOHL3.GL_Account_Id,DOHL2.GL_Account_Id,DOHL1.GL_Account_Id)
	,`rax-datamart-dev.corporate_dmart.dim_hier_gl_account`.Hier_Level_9_Name=COALESCE(DOHL9.GL_Account_Name,DOHL8.GL_Account_Name,DOHL7.GL_Account_Name,DOHL6.GL_Account_Name,DOHL5.GL_Account_Name,DOHL4.GL_Account_Name,DOHL3.GL_Account_Name,DOHL2.GL_Account_Name,DOHL1.GL_Account_Name)
	,`rax-datamart-dev.corporate_dmart.dim_hier_gl_account`.Hier_Level_10=COALESCE(DOHL10.GL_Account_Id,DOHL9.GL_Account_Id,DOHL8.GL_Account_Id,DOHL7.GL_Account_Id,DOHL6.GL_Account_Id,DOHL5.GL_Account_Id,DOHL4.GL_Account_Id,DOHL3.GL_Account_Id,DOHL2.GL_Account_Id,DOHL1.GL_Account_Id)
,`rax-datamart-dev.corporate_dmart.dim_hier_gl_account`.Hier_Level_10_Name=COALESCE(DOHL10.GL_Account_Name,DOHL9.GL_Account_Name,DOHL8.GL_Account_Name,DOHL7.GL_Account_Name,DOHL6.GL_Account_Name,DOHL5.GL_Account_Name,DOHL4.GL_Account_Name,DOHL3.GL_Account_Name,DOHL2.GL_Account_Name,DOHL1.GL_Account_Name)

  FROM
  
`rax-datamart-dev.corporate_dmart.dim_hier_gl_account` A,
`rax-datamart-dev.corporate_dmart.dim_gl_account` B  
,`rax-datamart-dev.corporate_dmart.dim_gl_account` P 
,`rax-datamart-dev.corporate_dmart.dim_gl_account` DOHL1 
,`rax-datamart-dev.corporate_dmart.dim_gl_account` DOHL2 
,`rax-datamart-dev.corporate_dmart.dim_gl_account` DOHL3
,`rax-datamart-dev.corporate_dmart.dim_gl_account` DOHL4
,`rax-datamart-dev.corporate_dmart.dim_gl_account` DOHL5 
,`rax-datamart-dev.corporate_dmart.dim_gl_account` DOHL6 
,`rax-datamart-dev.corporate_dmart.dim_gl_account` DOHL7 
,`rax-datamart-dev.corporate_dmart.dim_gl_account` DOHL8 	
,`rax-datamart-dev.corporate_dmart.dim_gl_account` DOHL9 	
,`rax-datamart-dev.corporate_dmart.dim_gl_account` DOHL10  

Where
B.Current_Record=1	
and P.Current_Record=1
AND A.GL_Account_Id=B.GL_Account_Id
AND A.Parent_GL_Account_Id=P.GL_Account_Id
AND SUBSTRING(A.Root_To_Leaf_Path,(7 * 0 + 2),6) = DOHL1.GL_Account_Id
AND SUBSTRING(A.Root_To_Leaf_Path,(7 * 1 + 2),6) = DOHL2.GL_Account_Id
AND SUBSTRING(A.Root_To_Leaf_Path,(7 * 2 + 2),6) = DOHL3.GL_Account_Id
AND SUBSTRING(A.Root_To_Leaf_Path,(7 * 3 + 2),6) = DOHL4.GL_Account_Id
AND SUBSTRING(A.Root_To_Leaf_Path,(7 * 4 + 2),6) = DOHL5.GL_Account_Id
AND SUBSTRING(A.Root_To_Leaf_Path,(7 * 5 + 2),6) = DOHL6.GL_Account_Id
AND SUBSTRING(A.Root_To_Leaf_Path,(7 * 6 + 2),6) = DOHL7.GL_Account_Id
AND SUBSTRING(A.Root_To_Leaf_Path,(7 * 7 + 2),6) = DOHL8.GL_Account_Id
AND SUBSTRING(A.Root_To_Leaf_Path,(7 * 8 + 2),6) = DOHL9.GL_Account_Id
AND SUBSTRING(A.Root_To_Leaf_Path,(7 * 9 + 2),6) = DOHL10.GL_Account_Id;


END;
