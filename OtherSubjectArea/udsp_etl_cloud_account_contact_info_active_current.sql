CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.slicehost.udsp_etl_cloud_account_contact_info_active_current()
begin


create or replace table `rax-abo-72-dev`.slicehost.cloud_account_contact_info_active_current as 
SELECT
	 Account_ID ,
	 Account_Number ,
	Account_Tenure,
	 SliceHost_CustomerID ,
	 AccountName ,
	 Account_Status ,
	 Account_Status_ID ,
	 Account_Status_Online_ID ,
	 Account_Type ,
	 Account_Type_ID ,
	 Account_SLAType ,
	 Account_SLATypeID ,
	 Account_Created_Date ,
	 ContractDate ,
	Consolidated_Billing,
	 FirstName ,
	 LastName ,
	 UserName ,
	 Email ,
	 Phone ,
	 Street ,
	 City ,
	 State ,
	 PostalCode ,
	 Country ,
	 CountryCode ,
	Internal_Flag					AS Accounting_Internal_Flag,
	Domain_Internal_Flag,
	(
		SELECT
			Max(Account_Created_Date) 
		FROM
			`rax-abo-72-dev`.slicehost.cloud_account_contact_info_current
	)								 As As_of_Date						
FROM 
	`rax-abo-72-dev`.slicehost.cloud_account_contact_info_current
WHERE
	Account_Status_Online_ID=1
AND Internal_Flag<>1
AND  Account_Type_ID =3;

end;
