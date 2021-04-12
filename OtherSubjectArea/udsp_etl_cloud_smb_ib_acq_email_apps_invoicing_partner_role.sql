
create or replace procedure `rax-abo-72-dev`.sales.udsp_etl_cloud_smb_ib_acq_email_apps_invoicing_partner_role(v_date date)
begin

---------------------------------------------------------------------------------------------------------------------
DECLARE CurrentMonthYear  datetime;
DECLARE CurrentTime_Month string;
DECLARE WorkDays int64;
DECLARE CalDays int64;
---------------------------------------------------------------------------------------------------------------
SET CurrentMonthYear=cast(v_date as datetime);
SET CurrentTime_Month=cast(`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(CurrentMonthYear) as string);
---------------------------------------------------------------------------------------------------------------

DELETE FROM `rax-abo-72-dev`.sales.cloud_smb_ib_acq_email_apps_invoices_partner_role 
WHERE cast(`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(Close_Date) as string)=CurrentTime_Month;

-------------------------------------------------------------------------------------------------------------
INSERT INTO `rax-abo-72-dev`.sales.cloud_smb_ib_acq_email_apps_invoices_partner_role
SELECT 
	A.ACCOUNTID,
	A.Account_Num,
	A.Account_Type,
	A.Account_Sub_Type,
	A.Cloud_Account,
	A.Email_Core_Account_Num,
	A.Cloud_Account_Name,
	A.Email_Account_Type,
	A.Is_Consolidated_Billing,
	A.Is_Linked_Account,
	A.Is_Internal_Account,
	A.Cloud_Desired_Billing_Date,
	A.Cloud_Account_Status,
	A.Cloud_Account_Create_Date,
	A.OPPORTUNITY_ID,
	A.Opportunity_Name,
	A.Opportunity_Owner,
	A.Opportunity_Owner_ID,
	A.Opportunity_Owner_Role,
	A.Opportunity_Owner_Role_Segment,
	A.Opportunity_Owner_Is_Active,
	A.Account_Owner,
	A.Account_Owner_ID,
	A.Account_Owner_Role,
	A.Account_Owner_Role_Segment,
	A.Account_Owner_Is_Active,
	A.Split_Category,
	A.Split_Percentage,
	A.Cloud_Username,
	A.Name,
	A.Category,
	A.Final_Opportunity_Type,
	A.Close_Date,
	cast(A.Activation_Date as datetime) as Activation_Date,
	A.Record_Type,
	A.StageName,
	A.On_Demand_Reconciled,
	A.first_month_paid,
	A.Additional_Sales_Rep,
	A.Additional_Sales_Rep_ID,
	A.Solution_Engineer_ID,
	A.Additional_Solution_Engineer_ID,
	A.Max_Lead_Generator_ID,
	A.Lead_Date_Passed,
	A.Reseller_Partner_Account,
	A.Partner_Role,
	A.Channel,
	A.Commission_Referral_Type,
	C.RV_Account				AS RV_Partner_Account,
	D.Namex						AS RV_Partner_Account_Name,
	C.Rolex						AS RV_Partner_Account_Role,
	A.cloud_servers_fees,
	Paid,
	Paid_Date,
	Adjusted,
	cast(Adjusted_Date as datetime) Adjusted_Date,
	'True'						AS Approved,
	'Approved'					AS Status,
	cast(current_date()	as date)				AS record_insert_date,
	Activation_File_Type,
	A.Territory, 
	A.LPID, 
	A.PLATFORM,
	A.PLATFORM_SUB_CATEGORY,
	A.BUCKET_INFLUENCE,
	A.BUCKET_SOURCE,
	A.LEADSOURCE,
	A.FOCUS_AREA,
	A.Partner_Role_Role,
	A.Partner_Role_Name,
	A.Partner_Role_StatusX,
	A.Marketing_SourceD


from
	`rax-abo-72-dev`.sales.cloud_smb_ib_acq_email_apps_invoices_insert a  
left join
	`rax-abo-72-dev`.sales.cloud_opportunity_daily_snapshot b 
on a.opportunity_id = b.opportunity_id

join
	`rax-landing-qa`.salesforce_ods.qpartner_role c 
on b.master_opportunity_id = c.opportunity
join
	`rax-landing-qa`.salesforce_ods.qrvpe__rvaccount d  
on c.rv_account= d.id
WHERE  cast(`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(A.Close_Date) as string)=CurrentTime_Month;

end;
