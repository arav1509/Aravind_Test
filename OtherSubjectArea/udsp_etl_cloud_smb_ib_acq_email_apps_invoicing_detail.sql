create or replace procedure `rax-abo-72-dev`.sales.udsp_etl_cloud_smb_ib_acq_email_apps_invoicing_detail(v_date date)
begin
---------------------------------------------------------------------------------------------------------------------
DECLARE CurrentMonthYear datetime;
DECLARE CurrentTime_Month string;
DECLARE WorkDays int64;
DECLARE CalDays int64;
---------------------------------------------------------------------------------------------------------------
SET CurrentMonthYear=cast(v_date as datetime);
SET CurrentTime_Month=cast(`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(currentmonthyear) as string);
----------------------------------------------------------------------------------------------------
DELETE FROM  `rax-abo-72-dev`.sales.cloud_smb_ib_acq_email_apps_invoices_detail WHERE Invoice_Time_Month_Key=CurrentTime_Month;
-------------------------------------------------------------------------------------------------------------
INSERT INTO  `rax-abo-72-dev`.sales.cloud_smb_ib_acq_email_apps_invoices_detail
SELECT 
	ACCOUNTID,
	Account_Num,
	Account_Type,
	Account_Sub_Type,
	A.Cloud_Account,
	Email_Core_Account_Num,
	Cloud_Account_Name,
	Email_Account_Type,
	Is_Consolidated_Billing,
	Is_Linked_Account,
	Is_Internal_Account,
	Cloud_Desired_Billing_Date,
	Cloud_Account_Status,
	Cloud_Account_Create_Date,
	OPPORTUNITY_ID,
	Opportunity_Name,
	Opportunity_Owner,
	Opportunity_Owner_ID,
	Opportunity_Owner_Role,
	Opportunity_Owner_Role_Segment,
	Opportunity_Owner_Is_Active,
	Account_Owner,
	Account_Owner_ID,
	Account_Owner_Role,
	Account_Owner_Role_Segment,
	Account_Owner_Is_Active,
	Split_Category,
	Split_Percentage,
	Cloud_Username,
	Name,
	Category,
	Final_Opportunity_Type,
	Close_Date,
	cast(Activation_Date as datetime) as Activation_Date,
	Record_Type,
	StageName,
	On_Demand_Reconciled,
	first_month_paid,
	Additional_Sales_Rep,
	Additional_Sales_Rep_ID,
	Solution_Engineer_ID,
	Additional_Solution_Engineer_ID,
	Max_Lead_Generator_ID,
	Lead_Date_Passed,
	Reseller_Partner_Account,
	Partner_Role,
	Channel,
	Commission_Referral_Type,
	Invoice_Amount				AS cloud_servers_fees,
	ServiceID,
	Service_Name,
	Invoice_Date,
	Invoice_Time_Month_Key,
	current_date()					AS record_insert_date,
	Activation_File_Type, Territory, LPID, PLATFORM, PLATFORM_SUB_CATEGORY, BUCKET_INFLUENCE, BUCKET_SOURCE, LEADSOURCE, FOCUS_AREA, Partner_Role_Role, Partner_Role_Name, Partner_Role_StatusX, MARKETING_SOURCED
FROM 
	`rax-abo-72-dev`.sales.cloud_smb_ib_acq_email_apps_invoices_insert A 
JOIN
	`rax-abo-72-dev`.sales.email_apps_service_line_item B 
ON A.Cloud_Account = B.Cloud_Account
AND cast(`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(Paid_Date)  as string)= B.Invoice_Time_Month_Key
WHERE
	cast(`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(Paid_Date) as string)=CurrentTime_Month;
	
end;	