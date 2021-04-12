CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_raw_item_load`()
BEGIN

create or replace table stage_one.raw_item as 
SELECT LTRIM(RTRIM(ifnull(Item_Tag,'Unknown'))) As Item_Tag
	  ,LTRIM(RTRIM(ifnull(I.DESCR,'Unknown'))) As Item_Name
      ,LTRIM(RTRIM(Coalesce(D.DESCR,I.DESCR,'Unknown'))) As Item_Description
	  ,LTRIM(RTRIM(ifnull(ITEM_TYPE,'Unknown'))) As Item_Type
      ,LTRIM(RTRIM(ifnull(ITEM_SUB_TYPE,'Unknown'))) As Item_Sub_Type
	  ,ifnull(Rec_Id,0) As Rec_Id
      ,current_datetime() As Record_Created_Datetime
 FROM `rax-landing-qa`.brm_ods.config_item_types_t I 
 Left Join `rax-landing-qa`.brm_ods.dd_objects_t d  On I.Item_Type=D.NAME;

END;
