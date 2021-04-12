CREATE OR REPLACE PROCEDURE `rax-abo-72-dev.sales.udsp_etl_ub_rt_utility_bookings`()
BEGIN

--------------------------------------------------------------
create or replace temp table  AGG as 
SELECT --INTO    #AGG
    Bill_TMK											   AS Period,
    CASE mod(cast(BILL_TMK as int64),100) WHEN 1 THEN (Bill_TMK-89) else (Bill_TMK-1) END  AS Prior_Period,
    cast(BD.Account_SF_ID as string) as Account_SF_ID,
    BD.Linked_SF_ID,
   cast( BD.Account_Number as string) Account_Number,
    BD.Platform,
    BD.Service_Level,
    BD.Charge_Type,
    BD.Is_Recurring_Revenue			AS Is_Recurring,
    BD.BRM_Is_Recurring_Desc,
    BD.BRM_Is_Recurring,
    BD.Is_Backbill,
    BD.Is_Utility_Booking,
    BD.Utility_Bucket,
    BD.Is_Normalized,
    BD.Currency,
    SUM(BD.Amount_Local)				AS Amount_Local,
    SUM(BD.Amount_USD)				AS Amount_USD,
    SUM(BD.Amount_GBP)				AS Amount_GBP,
    SUM(BD.Normalized_Amount_Local)	AS Normalized_Amount_Local,
    SUM(BD.Normalized_Amount_USD)		AS Normalized_Amount_USD,
    SUM(BD.Normalized_Amount_GBP)		AS Normalized_Amount_GBP,
    BD.Is_Internal_Account
FROM
    (
	SELECT --#Billing_Data
  Account_SF_ID
,Linked_SF_ID
,Core_Account_Number
,Account_Number
,Line_of_Business
,Customer_Name
,Bill_TMK
,Bill_Date
,Bill_Start_Date
,Bill_No
,Min_Event_Created_Date
,Max_Event_Created_Date
,Bill_Earned_Start_Date
,Bill_Earned_End_Date
,Usage_Date_TMK
,Amount_Local
,Normalized_Amount_Local
,Currency
,Amount_USD
,Normalized_Amount_USD
,Amount_GBP
,Normalized_Amount_GBP
,Platform
,Service_Level
,Charge_Type
,Utility_Bucket
,Is_Recurring_Revenue
,BRM_Is_Recurring_Desc
,BRM_Is_Recurring
,Is_Backbill
,Is_Normalized
,GL_ID
,GL_Config_Desc
,Impact_Category
,Product_Code
,Product_Description
,GL_Account
,GL_Account_Desc
,Product_BU
,Product_Type
,Days_in_Usage_Period
,Is_Utility_Booking
,Is_Internal_Account
,Load_dtt
FROM  `rax-abo-72-dev`.sales.ub_brm_data A 
	---------------------
	UNION ALL
	---------------------
SELECT	Account_SF_ID
,Linked_SF_ID
,cast(Core_Account_Number as string) as Core_Account_Number
,Account_Number
,Line_of_Business
,Customer_Name
,Bill_TMK
,Bill_Date
,Bill_Start_Date
,Bill_No
,Min_Event_Created_Date
,Max_Event_Created_Date
,Bill_Earned_Start_Date
,Bill_Earned_End_Date
,Usage_Date_TMK
,Amount_Local
,Normalized_Amount_Local
,Currency
,Amount_USD
,Normalized_Amount_USD
,Amount_GBP
,Normalized_Amount_GBP
,Platform
,Service_Level
,Charge_Type
,Utility_Bucket
,Is_Recurring_Revenue
,BRM_Is_Recurring_Desc
,BRM_Is_Recurring
,Is_Backbill
,Is_Normalized
,GL_ID
,GL_Config_Desc
,Impact_Category
,Product_Code
,Product_Description
,GL_Account
,GL_Account_Desc
,Product_BU
,Product_Type
,Days_in_Usage_Period
,Is_Utility_Booking
,Is_Internal_Account
,Load_dtt

	FROM  `rax-abo-72-dev`.sales.ub_brm_data_cloud B 
	) BD --#Billing_Data BD
GROUP BY
    Bill_TMK,
    BD.Account_SF_ID,
    BD.Linked_SF_ID,
    BD.Account_Number,
    BD.Platform,
    BD.Service_Level,
    BD.Charge_Type,
    BD.Is_Recurring_Revenue,
    BD.BRM_Is_Recurring_Desc,
    BD.BRM_Is_Recurring,
    BD.Is_Backbill,
    BD.Is_Utility_Booking,
    BD.Utility_Bucket,
    BD.Is_Normalized,
    BD.Currency,
    BD.Is_Internal_Account;

create or replace table `rax-abo-72-dev`.sales.ub_rt_utility_bookings as
SELECT 
    Period,
    Prior_Period,
    BD.Account_SF_ID,
    AO.Account_Number							  as Account_Number,
    IfNULL(cast(LK.Account_Number as string),'N/A') as Linked_Account_Number,
    AO.DDI,
    AO.DP_Customer_ID,
    AO.Account_Name,
    AO.Account_OwnerID as Owner_SF_ID,
    AO.Account_OwnerName as Owner_Name,
    AO.Owner_Region,
    AO.Owner_Sub_Region,
    AO.Owner_Segment,
    AO.Owner_Sub_Segment,
    AO.Owner_Group,
    AO.Owner_Sub_Group as Owner_Subgroup,
    AO.Owner_Role,
    AO.Owner_Role_Type,
    AO.Owner_Team,
    AO.Owner_Userrole_ID as Owner_RoleID,
    BD.Platform,
    BD.Service_Level,
    BD.Charge_Type,
    BD.Is_Recurring,
    BD.BRM_Is_Recurring_Desc,
    BD.BRM_Is_Recurring,
    BD.Is_Backbill,
    BD.Is_Utility_Booking,
    BD.Utility_Bucket,
    BD.Is_Normalized,
    BD.Currency,
    BD.Amount_Local,
    BD.Amount_USD,
    BD.Amount_GBP,
    BD.Normalized_Amount_Local,
    BD.Normalized_Amount_USD,
    BD.Normalized_Amount_GBP,
    BD.Is_Internal_Account,
    current_date() as Load_dtt
FROM 
	AGG BD
LEFT JOIN
    `rax-abo-72-dev`.sales.ub_account_ownership AO
ON BD.Account_SF_ID = AO.Account_SF_ID
AND BD.Period = AO.Date_Month_Key
LEFT JOIN
    `rax-abo-72-dev`.sales.ub_account_ownership LK
ON BD.Linked_SF_ID = LK.Account_SF_ID
AND BD.Period = LK.Date_Month_Key
;
--omitted 2/11/2019
--LEFT JOIN
--	Sales.dbo.UB_Bridge_Owner_Hierarchy BOH
--ON AO.Account_OwnerID = BOH.Ownerid
--AND BD.Bill_TMK = BOH.DateMonthSk
------------------------------------------------------------------

END;