CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_raw_billing_events_load`()
BEGIN

create or replace table `rax-staging-dev.stage_one.raw_billing_events`
AS
Select
	LTRIM(RTRIM(IFNULL(Name,'Unknown'))) as Event_Type, 
	CAST(LTRIM(RTRIM(IFNULL(REPLACE(name,'/',' '),'Unknown'))) As STRING) as Event_Type_Name,
	LTRIM(RTRIM(IFNULL(RIGHT(name , STRPOS (REVERSE(name),'/')-1),'Unknown'))) as Event_Type_Short_Name, 
	LTRIM(RTRIM(IFNULL(Descr,'Unknown'))) as Event_Type_Description,
	Case when name like '%cycle%' then 'Recurring' Else 'Non-Recurring' end as Event_Type_Charge,
	Case when name like '%cycle%' then 1 Else 0 end as Event_Is_Recurring,
	LTRIM(RTRIM(IFNULL(Name,'Unknown'))) Evnet_Type_Nk,
	Obj_Id0 As Event_Type_Source_Record_Id,
	CURRENT_DATETIME() AS Record_Created_Datetime
From
	`rax-landing-qa`.brm_ods.dd_objects_t
Where 	LOWER(name) like '/event%'	AND Obj_Id0<>102661 

Order by Obj_Id0;


END;
