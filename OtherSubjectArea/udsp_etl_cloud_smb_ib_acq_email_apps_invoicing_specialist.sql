
create or replace procedure `rax-abo-72-dev`.sales.udsp_etl_cloud_smb_ib_acq_email_apps_invoicing_specialist(v_date date)
---------------------------------------------------------------------------------------------------------------------
DECLARE CurrentMonthYear datetime;
DECLARE CurrentTime_Month string;
DECLARE WorkDays int64;
DECLARE CalDays int64;
---------------------------------------------------------------------------------------------------------------
SET CurrentMonthYear=cast(v_Date as datetime);
SET CurrentTime_Month=cast(`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(currentmonthyear) as string);

-------------------------------------------------------------------------------------------------------------
DELETE FROM `rax-abo-72-dev`.sales.cloud_smb_ib_acq_email_apps_invoices_specialist WHERE cast(`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(Close_Date) as string)=CurrentTime_Month
;
-------------------------------------------------------------------------------------------------------------
INSERT INTO `rax-abo-72-dev`.sales.cloud_smb_ib_acq_email_apps_invoices_specialist
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
	C.Additional_Sales_Team		AS Additional_Sales_Rep,
	D.NAMEX						AS Additional_Sales_Rep_ID,
	C.Additional_Team_Type,
	A.Solution_Engineer_ID,
	A.Additional_Solution_Engineer_ID,
	A.Max_Lead_Generator_ID,
	A.Lead_Date_Passed,
	A.Reseller_Partner_Account,
	A.Partner_Role,
	A.Channel,
	A.Commission_Referral_Type,
	A.cloud_servers_fees,
	Paid,
	Paid_Date,
	Adjusted,
	cast(Adjusted_Date as datetime) as Adjusted_Date,
	'True'						AS Approved,
	'Approved'					AS Status,
	cast(current_date() as date)					AS record_insert_date,
	Activation_File_Type,
	A.Territory, 
	A.LPID, 
	A.PLATFORM, A.PLATFORM_SUB_CATEGORY, A.BUCKET_INFLUENCE, A.BUCKET_SOURCE, A.LEADSOURCE, A.FOCUS_AREA, A.Partner_Role_Role, A.Partner_Role_Name, A.Partner_Role_StatusX, a.MARKETING_SOURCED

from
	`rax-abo-72-dev`.sales.cloud_smb_ib_acq_email_apps_invoices_insert a  
join
	`rax-abo-72-dev`.sales.cloud_opportunity_daily_snapshot b 
on a.opportunity_id = b.opportunity_id

join
	`rax-landing-qa`.salesforce_ods.qspecialist c 
on b.master_opportunity_id = c.opportunity

left join
	`rax-landing-qa`.salesforce_ods.quser d  
on c.additional_sales_team= d.id
WHERE 
	cast(`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(currentmonthyear) as string)=CurrentTime_Month;

end;
