create or replace procedure `rax-abo-72-dev`.sales.udsp_etl_cloud_smb_ib_acq_email_apps_invoice_audit(v_date date)
begin
---------------------------------------------------------------------------------------------------------------------
DECLARE CurrentMonthYear  datetime;
DECLARE CurrentTime_Month string;
DECLARE WorkDays int64;
DECLARE CalDays int64;

---------------------------------------------------------------------------------------------------------------
SET CurrentMonthYear=cast(v_date as datetime);
SET CurrentTime_Month=cast(`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(CurrentMonthYear) as string);


create or replace temp table 	SMB_SA3Invoices as
SELECT
	A.*,
	ifnull(First_Invoice_Amount,0)						AS New_Invoice_Total
--INTO	#SMB_SA3Invoices
FROM
	`rax-abo-72-dev`.sales.cloud_smb_ib_acq_email_apps_invoices_insert A
INNER JOIN
(
SELECT
	A.ACT_AccountID,
	First_Invoice_Month				AS First_Invoice_Month,
	TotalPrice						AS First_Invoice_Amount
FROM
(
SELECT DISTINCT
	ACT_AccountID,
	MIN(Invoiced_Date_Time_Month)	AS  First_Invoice_Month	
FROM
	`rax-abo-72-dev`.sales.salesforce_cloud_billing_info A  
WHERE
	upper(Invoice_Source) IN ('SA3')
GROUP BY
	ACT_AccountID
)A
INNER JOIN
(
SELECT DISTINCT
	ACT_AccountID,
	Invoiced_Date_Time_Month	AS Invoiced_Date_Time_Month,
	SUM(TotalPrice)				AS TotalPrice
FROM
	`rax-abo-72-dev`.sales.salesforce_cloud_billing_info A  
WHERE
	upper(Invoice_Source) IN ('SA3')
GROUP BY
	ACT_AccountID,
	Invoiced_Date_Time_Month
)B	
ON A.ACT_AccountID=B.ACT_AccountID
AND A.First_Invoice_Month=B.Invoiced_Date_Time_Month
) B
ON A.Cloud_Account=B.ACT_AccountID
WHERE
	 ifnull(B.First_Invoice_Amount,0)<>0
AND lower(Email_Account_Type) <> 'rackspace indirect'
AND cast(`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(Paid_Date) as string)=CurrentTime_Month
;
---------------------------------------------------------------------------------------------------------------

create or replace temp table 	SMB_Upgrades as
SELECT 
	ACCOUNTID,
	Account_Num,
	Account_Type,
	Account_Sub_Type,
	Cloud_Account,
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
	'0'					AS Master_Opportunity_ID,
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
	cast(activation_date as datetime) as activation_date,
	Record_Type,
	StageName,
	On_Demand_Reconciled,
	first_month_paid,
	Solution_Engineer_ID,
	Additional_Solution_Engineer_ID,
	Max_Lead_Generator_ID,
	Lead_Date_Passed,
	Reseller_Partner_Account,
	Partner_Role,
	Channel,
	Commission_Referral_Type,
	cloud_servers_fees						AS Old_Invoice, 
	New_Invoice_Total						AS New_Invoice, 
	New_Invoice_Total-cloud_servers_fees	AS Adjustment,
	0										AS IS_Upgrade, 
	CAST(`rax-abo-72-dev`.bq_functions.udfdatepart(CurrentMonthYear) as datetime)					
											AS Upgrade_Date,
	`rax-abo-72-dev`.bq_functions.udfdatepart(current_date())				AS record_insert_date,
	Activation_File_Type, Territory, LPID, PLATFORM, PLATFORM_SUB_CATEGORY, BUCKET_INFLUENCE, BUCKET_SOURCE, LEADSOURCE, FOCUS_AREA, Partner_Role_Role, Partner_Role_Name, Partner_Role_StatusX, MARKETING_SOURCED
--INTO	#SMB_Upgrades--
FROM SMB_SA3Invoices A
WHERE
	cloud_servers_fees<>New_Invoice_Total	;
---------------------------------------------------------------------------------------------------------------	
create or replace temp table 	SMB_OracleInvoices as
SELECT
	A.*,
	ifnull(First_Invoice_Amount,0)						AS New_Invoice_Total
--INTO	#SMB_OracleInvoices
FROM
	`rax-abo-72-dev`.sales.cloud_smb_ib_acq_email_apps_invoices_insert A
INNER JOIN
(
SELECT
	A.ACT_AccountID,
	First_Invoice_Month				AS First_Invoice_Month,
	TotalPrice						AS First_Invoice_Amount
FROM
(
SELECT DISTINCT
	ACT_AccountID,
	MIN(Invoiced_Date_Time_Month)	AS  First_Invoice_Month	
FROM
	`rax-abo-72-dev`.sales.salesforce_cloud_billing_info A  
WHERE
	lower(Invoice_Source) IN ('Oracle')
GROUP BY
	ACT_AccountID
)A
INNER JOIN
(
SELECT DISTINCT
	ACT_AccountID,
	Invoiced_Date_Time_Month	AS Invoiced_Date_Time_Month,
	SUM(TotalPrice)				AS TotalPrice
FROM
	`rax-abo-72-dev`.sales.salesforce_cloud_billing_info A  
WHERE
	Invoice_Source IN ('Oracle')
GROUP BY
	ACT_AccountID,
	Invoiced_Date_Time_Month
)B	
ON A.ACT_AccountID=B.ACT_AccountID
AND A.First_Invoice_Month=B.Invoiced_Date_Time_Month
) B
ON A.Email_Core_Account_Num=B.ACT_AccountID
WHERE
	 ifnull(B.First_Invoice_Amount,0)<>0
AND lower(Email_Account_Type) = 'rackspace indirect'
AND cast(`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(Paid_Date) as string)=CurrentTime_Month;
---------------------------------------------------------------------------------------------------------------


INSERT INTO SMB_Upgrades
SELECT 
	ACCOUNTID,
	Account_Num,
	Account_Type,
	Account_Sub_Type,
	Cloud_Account,
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
	'0'					AS Master_Opportunity_ID,
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
	cast(activation_date as datetime),
	Record_Type,
	StageName,
	On_Demand_Reconciled,
	first_month_paid,
	Solution_Engineer_ID,
	Additional_Solution_Engineer_ID,
	Max_Lead_Generator_ID,
	Lead_Date_Passed,
	Reseller_Partner_Account,
	Partner_Role,
	Channel,
	Commission_Referral_Type,
	cloud_servers_fees						AS Old_Invoice, 
	New_Invoice_Total						AS New_Invoice, 
	New_Invoice_Total-cloud_servers_fees	AS Adjustment,
	0										AS IS_Upgrade, 
	CAST(`rax-abo-72-dev`.bq_functions.udfdatepart(CurrentMonthYear) as datetime)					
											AS Upgrade_Date,
	`rax-abo-72-dev`.bq_functions.udfdatepart(current_date())				AS record_insert_date,
	Activation_File_Type, Territory, LPID, PLATFORM, PLATFORM_SUB_CATEGORY, BUCKET_INFLUENCE, BUCKET_SOURCE, LEADSOURCE, FOCUS_AREA, Partner_Role_Role, Partner_Role_Name, Partner_Role_StatusX, MARKETING_SOURCED
FROM SMB_OracleInvoices A
WHERE
	cloud_servers_fees<>New_Invoice_Total	;
--------------------------------------------------------------
DELETE FROM   `rax-abo-72-dev`.sales.cloud_smb_ib_acq_email_apps_invoices_audit c
WHERE EXISTS
		 (
			SELECT 
				Cloud_Account
			FROM 
				SMB_Upgrades X
			WHERE 
				 X.Cloud_Account = c.Cloud_Account 
			AND cast(`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(Upgrade_Date) as string)=CurrentTime_Month
			)	;
--------------------------------------------------------------
INSERT INTO `rax-abo-72-dev`.sales.cloud_smb_ib_acq_email_apps_invoices_audit 
SELECT
*
FROM
	SMB_Upgrades A
WHERE
	 NOT EXISTS
		 (
			SELECT 
				Cloud_Account
			FROM 
				`rax-abo-72-dev`.sales.cloud_smb_ib_acq_email_apps_invoices_audit  X
			WHERE 
				 X.Cloud_Account = A.Cloud_Account 
			AND cast(`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(Upgrade_Date) as string)=CurrentTime_Month
			)	;
---------------------------------------------------------------------------------------------------------------
UPDATE `rax-abo-72-dev`.sales.cloud_smb_ib_acq_email_apps_invoices_audit c
SET
	c.Master_Opportunity_ID=B.Master_OPPORTUNITY_ID				
FROM
	`rax-abo-72-dev`.sales.cloud_smb_ib_acq_email_apps_invoices_audit A
INNER JOIN
	`rax-abo-72-dev`.sales.on_demand_reconciled_accounts B  
ON A.Cloud_Account =B.ddi
AND `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(A.Upgrade_Date)=`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(B.Close_date)
WHERE
	 cast(`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(A.Upgrade_Date) as string)= CurrentTime_Month
AND lower(B.On_demand_reconciled) ='true' ;
---------------------------------------------------------------------------------------------------------------
UPDATE `rax-abo-72-dev`.sales.cloud_smb_ib_acq_email_apps_invoices_audit c
SET
	c.Category=(CASE 
					WHEN (Adjustment + Old_Invoice)=0 						THEN 'Delete'
					WHEN New_Invoice < Old_Invoice AND New_Invoice >0		THEN 'Downgrade'
					WHEN New_Invoice> Old_Invoice							THEN 'Upgrade'
					ELSE
						' '
			  END)
WHERE
	cast(`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(C.Upgrade_Date) as string)= CurrentTime_Month;
--------------------------------------------------------------
UPDATE `rax-abo-72-dev`.sales.cloud_smb_ib_acq_email_apps_invoices_audit
SET
	Is_Upgrade=1
WHERE
	cast(`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(Upgrade_Date) as string)= CurrentTime_Month
AND lower(Category)='upgrade';
----------------------------------------------------------
UPDATE `rax-abo-72-dev`.sales.cloud_smb_ib_acq_email_apps_invoices_insert c
SET
	c.Cloud_Servers_Fees=B.New_Invoice,
	c.Adjusted=1,
	c.Adjusted_Date=CAST(B.record_insert_date AS STRING)
FROM
	`rax-abo-72-dev`.sales.cloud_smb_ib_acq_email_apps_invoices_insert a
INNER JOIN
	`rax-abo-72-dev`.sales.cloud_smb_ib_acq_email_apps_invoices_audit B
ON `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(A.Close_Date)=`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(B.Close_Date)
AND A.Cloud_Account=B.Cloud_Account
WHERE
	cast(`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(B.Upgrade_Date) as string)= CurrentTime_Month
;

end;
