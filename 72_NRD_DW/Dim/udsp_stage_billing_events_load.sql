CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_two_dw.udsp_stage_billing_events_load`()
BEGIN



DELETE
FROM  `rax-staging-dev.stage_two_dw.stage_billing_events` TARGET
WHERE TARGET.Event_Type IN(SELECT SOURCE.Event_Type from 
 `rax-staging-dev.stage_one.raw_billing_events` SOURCE
);


insert into  `rax-staging-dev.stage_two_dw.stage_billing_events`
(Event_Type
      ,Event_Type_Name
      ,Event_Type_Short_Name
      ,Event_Type_Description
	  ,Event_Type_Charge
	  ,Event_Type_Is_Recurring
	  ,Event_Type_Nk
	  ,Event_Type_Source_Record_Id,
    Record_Created_By,
    Record_Created_Datetime,
    Record_Updated_By,
    Record_Updated_Datetime,
   Chk_Sum_Md5
)
 SELECT 
	   Event_Type
      ,Event_Type_Name
      ,Event_Type_Short_Name
      ,Event_Type_Description
	  ,Event_Type_Charge
	  ,cast(Event_Is_Recurring as BOOL)
	  ,Evnet_Type_Nk
	  ,Event_Type_Source_Record_Id
	  ,'SSIS_Dim_Billing_Events' As Record_Created_By
	  ,CAST(CURRENT_TIMESTAMP() AS  DATETIME) AS Record_Created_Datetime
	  ,'SSIS_Dim_Billing_Events' As Record_Updated_By
	  ,CAST(CURRENT_TIMESTAMP() AS  DATETIME) AS Record_Updated_Datetime
  ,TO_BASE64(MD5(CONCAT( Event_Type_Name, '|' ,Event_Type_Short_Name,'|' ,Event_Type_Description,'|' ,Event_Type_Charge,'|' ,Event_Is_Recurring,'|' )))  AS Chk_Sum_Md5
  
FROM (
SELECT 
       Event_Type
      ,Event_Type_Name
      ,Event_Type_Short_Name
      ,Event_Type_Description
	  ,Event_Type_Charge
	  , Event_Is_Recurring
	  , Evnet_Type_Nk
	  ,Event_Type_Source_Record_Id
FROM `rax-staging-dev.stage_one.raw_billing_events`   ) SRC
ORDER BY 1;

END;
