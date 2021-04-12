CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_three_dw.udsp_insert_dim_hier_gl_account`()
BEGIN  

create or replace temporary table temp_gl_account
as
select PARENT_FLEX_VALUE,FLEX_VALUE 
from `rax-landing-qa.operational_reporting_oracle.xxrs_fnd_flex_value_children_v`;

insert into temp_gl_account (parent_flex_value,flex_value) values ('N/A','P36000');

create or replace temporary table temp_hierarchy_gl_account
as
  
  SELECT
		PA.FLEX_VALUE AS GL_Account_Id
		,PA.PARENT_FLEX_VALUE AS Parent_GL_Account_id
		,CAST(CONCAT('|',PA.FLEX_VALUE) AS STRING) AS RootToLeafPath
		,1 AS OrgLevel
	FROM
		temp_gl_account PA
	WHERE
		PA.FLEX_VALUE = 'P36000'
	
	UNION ALL
	SELECT
		PA.FLEX_VALUE AS GL_Account_Id
		,PA.PARENT_FLEX_VALUE AS Parent_GL_Account_id
		,CAST(CONCAT(RL.RootToLeafPath,'|',PA.FLEX_VALUE)  AS STRING) AS RootToLeafPath
		,RL.OrgLevel + 1 AS OrgLevel
	FROM
		temp_gl_account PA
		INNER JOIN (SELECT
		PA.FLEX_VALUE AS GL_Account_Id
		,PA.PARENT_FLEX_VALUE AS Parent_GL_Account_id
		,CAST(CONCAT('|',PA.FLEX_VALUE) AS string) AS RootToLeafPath
		,1 AS OrgLevel
	FROM
		temp_gl_account PA
	WHERE
		PA.FLEX_VALUE = 'P36000')  RL
		ON RL.GL_Account_Id = PA.PARENT_FLEX_VALUE;
    


delete  `rax-datamart-dev.corporate_dmart.dim_hier_gl_account` where true;

INSERT INTO `rax-datamart-dev.corporate_dmart.dim_hier_gl_account`
           (GL_Account_Id
           ,Parent_GL_Account_Id
           ,Hier_Level_No
           ,Root_To_Leaf_Path
           ,Leaf_Member
           ,Number_Of_Children
           ,GL_Account_Name
           ,Parent_GL_Account_Name
           ,Hier_Level_1
		   ,Hier_Level_1_Name
           ,Hier_Level_2
		   ,Hier_Level_2_Name
           ,Hier_Level_3
		   ,Hier_Level_3_Name
           ,Hier_Level_4
		   ,Hier_Level_4_Name
           ,Hier_Level_5
		   ,Hier_Level_5_Name
           ,Hier_Level_6
		   ,Hier_Level_6_Name
           ,Hier_Level_7
		   ,Hier_Level_7_Name
           ,Hier_Level_8
		   ,Hier_Level_8_Name
           ,Hier_Level_9
		   ,Hier_Level_9_Name
           ,Hier_Level_10
		   ,Hier_Level_10_Name
           ,Record_Created_By
           ,Record_Created_Datetime)

		Select 
			A.GL_Account_Id
           ,A.Parent_GL_Account_Id
		   ,A.OrgLevel
		   ,A.RootToLeafPath
		   ,0
		   ,0
		   ,'' ,'' ,'' ,'' ,'' ,'' ,'' ,'' ,'' ,'' ,''
		   ,'' ,'' ,'' ,'' ,'' ,'' ,'' ,'' ,'' ,'' ,''
		   ,'ETL'
	       ,CURRENT_DATETIME()
	     From 
		 
		 (Select 
			GL_Account_Id
           ,Parent_GL_Account_Id
		  ,OrgLevel
		   ,RootToLeafPath
		   ,RANK() over (partition by gl_account_id order by parent_gl_account_id desc) RK 
		   From	 
			temp_hierarchy_gl_account
		) A
		Where A.RK=1;


END;
