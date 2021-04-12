create or replace procedure `rax-abo-72-dev`.sales.udsp_etl_cloud_smb_ib_acq_email_apps_invoicing_insert(v_date date)
begin
---------------------------------------------------------------------------------------------------------------------
DECLARE CurrentMonthYear date;
DECLARE CurrentTime_Month int64;
DECLARE WorkDays int64;
DECLARE CalDays int64;
---------------------------------------------------------------------------------------------------------------
SET CurrentMonthYear=cast(current_date() as date );
SET CurrentTime_Month=`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(CurrentMonthYear);
---------------------------------------------------------------------------------------------------------------
----SMB ACQ SA3
INSERT INTO
	`rax-abo-72-dev`.sales.cloud_smb_ib_acq_email_apps_invoices_insert
SELECT 
	ACCOUNTID,Account_Num, Account_Type, Account_Sub_Type, A.Cloud_Account,A.Email_Core_Account_Num, Cloud_Account_Name, Email_Account_Type,Is_Consolidated_Billing,Is_Linked_Account,Is_Internal_Account,Cloud_Desired_Billing_Date,A.Cloud_Account_Status,Cloud_Account_Create_Date,OPPORTUNITY_ID,Opportunity_Name, Opportunity_Owner,Opportunity_Owner_ID,Opportunity_Owner_Role,Opportunity_Owner_Role_Segment,Opportunity_Owner_Is_Active,Account_Owner,Account_Owner_ID,Account_Owner_Role,Account_Owner_Role_Segment,Account_Owner_Is_Active,Split_Category,Split_Percentage,Cloud_Username, concat(CAST(FORMAT_DATETIME("%B", DATETIME(CurrentMonthYear)) as string) ,' ',CAST(extract(year from CurrentMonthYear) as string), ' ', 'Email Apps Ist Invoice' ,' - ' , A.Cloud_Account)  AS Name, Category,Final_Opportunity_Type, CAST(`rax-abo-72-dev`.bq_functions.udfdatepart(CurrentMonthYear) as datetime) AS Close_Date, '1900-01-01' AS Activation_Date, '0125000000052evAAA' As Record_Type,StageName, 'TRUE' AS On_Demand_Reconciled,1 AS first_month_paid, Additional_Sales_Rep,Additional_Sales_Rep_ID, Solution_Engineer_ID,Additional_Solution_Engineer_ID, Max_Lead_Generator_ID, CAST(`rax-abo-72-dev`.bq_functions.udfdatepart(CurrentMonthYear) as datetime) AS Lead_Date_Passed,Partner_Account AS Reseller_Partner_Account,Partner_Role, Channel,Commission_Referral_Type, trunc(CAST(First_Invoice_Amount AS numeric),2) AS cloud_servers_fees, 1 AS Paid,  CAST(`rax-abo-72-dev`.bq_functions.udfdatepart(CurrentMonthYear) as datetime) AS Paid_Date,0 AS Adjusted, '1900-01-01' AS Adjusted_Date, `rax-abo-72-dev`.bq_functions.udfdatepart(current_date()) As record_insert_date, 'SMB_ACQ_Email_Apps' AS  Activation_File_Type, Territory, LPID, PLATFORM, PLATFORM_SUB_CATEGORY, BUCKET_INFLUENCE, BUCKET_SOURCE, LEADSOURCE, FOCUS_AREA, Partner_Role_Role, Partner_Role_Name, Partner_Role_StatusX, MARKETING_SOURCED
FROM 
	`rax-abo-72-dev`.sales.cloud_smb_acq_email_apps_all A 
WHERE
    First_Invoice_Month=CurrentTime_Month
AND lower(Email_Account_Type) <> 'rackspace indirect'
AND Mail_1st_Month_Invoice=1
AND lower(CLOUD_Account_Status) <>'closed'
AND First_Invoice_Amount<>0
AND	NOT EXISTS (
			SELECT 
				Cloud_Account
			FROM 
				`rax-abo-72-dev`.sales.cloud_smb_ib_acq_email_apps_invoices_insert X
			WHERE 
				 X.Cloud_Account = A.Cloud_Account 
			AND `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(Close_Date)=CurrentTime_Month
			);
-------------------
--SMB ACQ Rackspace
INSERT INTO
	`rax-abo-72-dev`.sales.cloud_smb_ib_acq_email_apps_invoices_insert
SELECT 
	ACCOUNTID,Account_Num, Account_Type, Account_Sub_Type, A.Cloud_Account,A.Email_Core_Account_Num,Cloud_Account_Name, Email_Account_Type,Is_Consolidated_Billing,Is_Linked_Account,Is_Internal_Account,Cloud_Desired_Billing_Date,A.Cloud_Account_Status,Cloud_Account_Create_Date,OPPORTUNITY_ID,Opportunity_Name,Opportunity_Owner,Opportunity_Owner_ID,Opportunity_Owner_Role,Opportunity_Owner_Role_Segment,Opportunity_Owner_Is_Active,Account_Owner,Account_Owner_ID,Account_Owner_Role,Account_Owner_Role_Segment,Account_Owner_Is_Active,Split_Category,Split_Percentage,Cloud_Username, concat(CAST(FORMAT_DATETIME("%B", DATETIME(CurrentMonthYear)) as string) ,' ',CAST(extract(year from CurrentMonthYear) as string), ' ', 'Email Apps Ist Invoice' ,' - ' , A.Cloud_Account)  AS Name,Category,Final_Opportunity_Type, CAST(`rax-abo-72-dev`.bq_functions.udfdatepart(CurrentMonthYear) as datetime) AS Close_Date, '1900-01-01' AS Activation_Date, '0125000000052evAAA' As Record_Type,StageName, 'TRUE' AS On_Demand_Reconciled,1 AS first_month_paid, Additional_Sales_Rep,Additional_Sales_Rep_ID, Solution_Engineer_ID,Additional_Solution_Engineer_ID, Max_Lead_Generator_ID, CAST(`rax-abo-72-dev`.bq_functions.udfdatepart(CurrentMonthYear) as datetime) AS Lead_Date_Passed,Partner_Account AS Reseller_Partner_Account,Partner_Role, Channel,Commission_Referral_Type, trunc(CAST(First_Invoice_Amount AS  numeric),2) AS cloud_servers_fees, 1 AS Paid,  CAST(`rax-abo-72-dev`.bq_functions.udfdatepart(CurrentMonthYear) as datetime) AS Paid_Date,0 AS Adjusted, '1900-01-01' AS Adjusted_Date, `rax-abo-72-dev`.bq_functions.udfdatepart(current_date()) As record_insert_date, 'SMB_ACQ_Email_Apps' AS  Activation_File_Type, Territory, LPID, PLATFORM, PLATFORM_SUB_CATEGORY, BUCKET_INFLUENCE, BUCKET_SOURCE, LEADSOURCE, FOCUS_AREA, Partner_Role_Role, Partner_Role_Name, Partner_Role_StatusX, MARKETING_SOURCED
FROM 
	`rax-abo-72-dev`.sales.cloud_smb_acq_email_apps_all A 
WHERE
    First_Invoice_Month=CurrentTime_Month
AND lower(Email_Account_Type) = 'rackspace indirect'
AND Mail_1st_Month_Invoice=1
AND lower(CLOUD_Account_Status) <>'closed'
AND IfNULL(First_Invoice_Amount,0)<>0
AND	NOT EXISTS (
			SELECT 
				Cloud_Account
			FROM 
				`rax-abo-72-dev`.sales.cloud_smb_ib_acq_email_apps_invoices_insert X
			WHERE 
				 X.Cloud_Account = A.Cloud_Account 
			AND `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(Close_Date)=CurrentTime_Month
			);
-------------------------------------------------------------------------------------------------------------
--SMB IB SA3
INSERT INTO
	`rax-abo-72-dev`.sales.cloud_smb_ib_acq_email_apps_invoices_insert
SELECT 
	ACCOUNTID,Account_Num, Account_Type, Account_Sub_Type, A.Cloud_Account,A.Email_Core_Account_Num, Cloud_Account_Name, Email_Account_Type,Is_Consolidated_Billing,Is_Linked_Account,Is_Internal_Account,Cloud_Desired_Billing_Date,A.Cloud_Account_Status,Cloud_Account_Create_Date,OPPORTUNITY_ID,Opportunity_Name,Opportunity_Owner,Opportunity_Owner_ID,Opportunity_Owner_Role,Opportunity_Owner_Role_Segment,Opportunity_Owner_Is_Active,Account_Owner,Account_Owner_ID,Account_Owner_Role,Account_Owner_Role_Segment,Account_Owner_Is_Active,Split_Category,Split_Percentage,Cloud_Username,concat(CAST(FORMAT_DATETIME("%B", DATETIME(CurrentMonthYear)) as string) ,' ',CAST(extract(year from CurrentMonthYear) as string), ' ', 'Email Apps Ist Invoice' ,' - ' , A.Cloud_Account)  AS Name, Category,Final_Opportunity_Type, CAST(`rax-abo-72-dev`.bq_functions.udfdatepart(CurrentMonthYear) as datetime) AS Close_Date, '1900-01-01' AS Activation_Date, '0125000000052evAAA' As Record_Type,StageName, 'TRUE' AS On_Demand_Reconciled,1 AS first_month_paid,  Additional_Sales_Rep,Additional_Sales_Rep_ID,Solution_Engineer_ID,Additional_Solution_Engineer_ID, Max_Lead_Generator_ID, CAST(`rax-abo-72-dev`.bq_functions.udfdatepart(CurrentMonthYear) as datetime) AS Lead_Date_Passed,Partner_Account AS Reseller_Partner_Account,Partner_Role, Channel,Commission_Referral_Type, trunc(CAST(First_Invoice_Amount AS numeric),2) AS cloud_servers_fees, 1 AS Paid,  CAST(`rax-abo-72-dev`.bq_functions.udfdatepart(CurrentMonthYear) as datetime) AS Paid_Date,0 AS Adjusted, '1900-01-01' AS Adjusted_Date, `rax-abo-72-dev`.bq_functions.udfdatepart(current_date()) As record_insert_date, 'SMB_IB_Email_Apps' AS  Activation_File_Type, Territory, LPID, PLATFORM, PLATFORM_SUB_CATEGORY, BUCKET_INFLUENCE, BUCKET_SOURCE, LEADSOURCE, FOCUS_AREA, Partner_Role_Role, Partner_Role_Name, Partner_Role_StatusX, MARKETING_SOURCED
FROM 
	`rax-abo-72-dev`.sales.cloud_smb_ib_email_apps_all A 
WHERE
    First_Invoice_Month=CurrentTime_Month
AND lower(Email_Account_Type) <> 'rackspace indirect'
AND Mail_1st_Month_Invoice=1
AND lower(CLOUD_Account_Status) <>'closed'
AND ifnull(First_Invoice_Amount,0)<>0
AND	NOT EXISTS (
			SELECT 
				Cloud_Account
			FROM 
				`rax-abo-72-dev`.sales.cloud_smb_ib_acq_email_apps_invoices_insert X
			WHERE 
				 X.Cloud_Account = A.Cloud_Account 
			AND `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(Close_Date)=CurrentTime_Month
			);
-------------------
--SMB IB Rackspace
INSERT INTO
	`rax-abo-72-dev`.sales.cloud_smb_ib_acq_email_apps_invoices_insert
SELECT 
	ACCOUNTID,Account_Num, Account_Type, Account_Sub_Type, A.Cloud_Account,A.Email_Core_Account_Num,Cloud_Account_Name, Email_Account_Type,Is_Consolidated_Billing,Is_Linked_Account,Is_Internal_Account,Cloud_Desired_Billing_Date,A.Cloud_Account_Status,Cloud_Account_Create_Date,OPPORTUNITY_ID,Opportunity_Name,Opportunity_Owner,Opportunity_Owner_ID,Opportunity_Owner_Role,Opportunity_Owner_Role_Segment,Opportunity_Owner_Is_Active,Account_Owner,Account_Owner_ID,Account_Owner_Role,Account_Owner_Role_Segment,Account_Owner_Is_Active,Split_Category,Split_Percentage,Cloud_Username, concat(CAST(FORMAT_DATETIME("%B", DATETIME(CurrentMonthYear)) as string) ,' ',CAST(extract(year from CurrentMonthYear) as string), ' ', 'Email Apps Ist Invoice' ,' - ' , A.Cloud_Account)  AS Name, Category,Final_Opportunity_Type, CAST(`rax-abo-72-dev`.bq_functions.udfdatepart(CurrentMonthYear) as datetime) AS Close_Date, '1900-01-01' AS Activation_Date, '0125000000052evAAA' As Record_Type,StageName, 'TRUE' AS On_Demand_Reconciled,1 AS first_month_paid, Additional_Sales_Rep,Additional_Sales_Rep_ID, Solution_Engineer_ID,Additional_Solution_Engineer_ID, Max_Lead_Generator_ID, CAST(`rax-abo-72-dev`.bq_functions.udfdatepart(CurrentMonthYear) as datetime) AS Lead_Date_Passed,Partner_Account AS Reseller_Partner_Account,Partner_Role, Channel,Commission_Referral_Type, trunc(CAST(First_Invoice_Amount AS numeric),2) AS cloud_servers_fees, 1 AS Paid,  CAST(`rax-abo-72-dev`.bq_functions.udfdatepart(CurrentMonthYear) as datetime) AS Paid_Date,0 AS Adjusted, '1900-01-01' AS Adjusted_Date, `rax-abo-72-dev`.bq_functions.udfdatepart(current_date()) As record_insert_date, 'SMB_IB_Email_Apps' AS  Activation_File_Type, Territory, LPID, PLATFORM, PLATFORM_SUB_CATEGORY, BUCKET_INFLUENCE, BUCKET_SOURCE, LEADSOURCE, FOCUS_AREA, Partner_Role_Role, Partner_Role_Name, Partner_Role_StatusX, MARKETING_SOURCED
FROM 
	`rax-abo-72-dev`.sales.cloud_smb_ib_email_apps_all A 
WHERE
    First_Invoice_Month=CurrentTime_Month
AND Email_Account_Type = 'Rackspace Indirect'
AND Mail_1st_Month_Invoice=1
AND CLOUD_Account_Status <>'Closed'
AND ifnull(First_Invoice_Amount,0)<>0
AND	NOT EXISTS (
			SELECT 
				Cloud_Account
			FROM 
				`rax-abo-72-dev`.sales.cloud_smb_ib_acq_email_apps_invoices_insert X
			WHERE 
				 X.Cloud_Account = A.Cloud_Account 
			AND `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(Close_Date)=CurrentTime_Month
			)	;
---------------------------------------------------------------------------------------------------------------
end;
