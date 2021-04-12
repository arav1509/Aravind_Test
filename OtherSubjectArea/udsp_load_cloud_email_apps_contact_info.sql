CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.sales.udsp_load_cloud_email_apps_contact_info()
begin

create or replace table `rax-abo-72-dev`.sales.cloud_email_apps_contact_info as 
SELECT DISTINCT 
	Account_Key,
	A.Account,
	Account_Start_Date,
	Account_End_Date,
	Account_Billing_Date,	
	Account_Tenure,	
	Rackspace_Account,
	Account_Name,
	Account_Status,
	Account_Status_Online_ID,
	Account_Type,
	Parent_Account,
	Parent_Account_Name,
	Account_Manager,
	Account_Source,
	Primary_Contact_First_Name,
	Primary_Contact_Last_Name,
	Contact_Type					AS Primary_Contact_Type,
	Primary_Contact_Phone			AS Phone_No,
	Primary_Contact_Email			AS E_Mail,
	address,
	city,
	state,
	zip,
	country,
	Domain,
	Domain_Internal_Flag,	
	current_date()						As Load_Date	
FROM `rax-abo-72-dev`.mailtrust.email_apps_core_primary_contact_info  A ;
end;