CREATE OR REPLACE PROCEDURE `rax-abo-72-dev.slicehost.udsp_etl_cloud_account_contact_info_current`()
begin
---------------------------------------------------------------------------------------------------------------	

DECLARE v_CURRENT_TMK int64;
SET v_CURRENT_TMK=`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(current_date());
---------------------------------------------------------------------------------------------------------------	
create or replace temp table start_up as 
SELECT  
    CAST(replace(replace(ACCOUNT_NO,'020-',''),'021-','') as int64)					AS account_number,
    CASE 
	    WHEN 
	    cast(current_date() as date) between cast(DATETIME_ADD(CAST('1970-01-01 00:00:00' AS datetime), interval cast(max(PURCHASE_START_T) as int64) second ) as  datetime)
	    and cast(DATETIME_ADD(CAST('1970-01-01 00:00:00' AS datetime), interval cast(max(PURCHASE_END_T) as int64) second) as date)
	    THEN 
		    1 
	    ELSE 
		    0 
    END AS Is_Startup,
    DATETIME_ADD(CAST('1970-01-01 00:00:00' AS datetime), interval cast(max(PURCHASE_START_T) as int64) second) AS Startup_Start_Date,
    DATETIME_ADD(CAST('1970-01-01 00:00:00' AS datetime), interval cast(max(PURCHASE_END_T) as int64) second ) AS Startup_End_Date

FROM (
SELECT  
    a.ACCOUNT_NO,
    p.PURCHASE_START_T,
    p.PURCHASE_END_T
FROM 
    `rax-landing-qa`.brm_ods.deal_t d 
INNER JOIN 
    `rax-landing-qa`.brm_ods.purchased_product_t p 
ON d.POID_ID0 = p.DEAL_OBJ_ID0
INNER JOIN 
    `rax-landing-qa`.brm_ods.account_t a 
ON p.ACCOUNT_OBJ_ID0 = a.POID_ID0
WHERE 
    lower(d.DESCR) like '%startup%')
GROUP BY 
   CAST(replace(replace(ACCOUNT_NO,'020-',''),'021-','') as int64)
	;
	
create or replace temp table  rcn_Temp as	
SELECT * --INTO	#rcn
FROM(
SELECT  
    A.account_Number,
    customer_Number			 AS RCN,
    A.account_Source_System_name
FROM
	`rax-datamart-dev`.corporate_dmart.dim_account  A 
WHERE
	customer_Number IS NOT NULL
AND	A.current_Record = 1
AND lower(account_Source_System_name)='hostingmatrix')
;

create or replace temp table  gcn as
SELECT * --into	#gcn
FROM (
SELECT   
    A.account_Number,
    GCN,
    account_Type
FROM
	`rax-datamart-dev`.corporate_dmart.gcn_match  A 
WHERE
	GCN IS NOT NULL
AND lower(account_Type)='hostingmatrix'
);
---------
create or replace temp table  rackconnect as
SELECT * --into	#rackconnect
FROM (
SELECT   
    A.account_Number
FROM
	`rax-datamart-dev`.corporate_dmart.vw_sku_assignment  A 
where 
    cast(time_month_key as int64) = `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(current_date())
and ( lower(sku_name) like '%rackconnect%' or lower(sku_description) like '%rackconnect%')
and lower(device_online_status) = 'online'
group by 
    account_number);

create or replace temp table  USERS as	
SELECT *  --into	#USERS
FROM (
SELECT 
	A.ID					AS User_ID,
	A.NAMEX				AS User,
	USERROLEID			AS User_Role_ID,
	ISACTIVE				AS User_IS_ACTIVE,
	C.NAMEX				AS User_Role,
	COMPANYNAME			AS Company_Name, 
	DIVISION				AS Division, 
	DEPARTMENT			AS Department, 
	TITLE				AS Title, 
	ISACTIVE				AS User_Active, 
	USERROLEID			AS Role_ID,
	MANAGERID				AS Manager_ID, 
	CREATEDDATE			AS Created_Date, 
	ISMANAGER				AS IsManager,
	USERNAME				AS Username,
	Region				AS Region,
	GROUPX				AS `Group`,
	SUB_GROUP				AS Sub_Group,
	DEFAULTCURRENCYISOCODE   AS Default_Currency_ISO_Code,
	EMPLOYEENUMBER			AS EMPLOYEE_NUMBER,
	EMPLOYEENUMBER			AS EMPLOYEENUMBER	
FROM
	`rax-landing-qa`.salesforce_ods.quser A  
LEFT OUTER JOIN
	`rax-landing-qa`.salesforce_ods.quserrole C 
ON A.USERROLEID= C.ID
);

create or replace temp table  QAccount as	
sELECT  --INTO	#QAccount	
	A.ID								   AS SF_Account_ID,
	A.ACCOUNT_NUMBER					   AS SF_Core_Account_Number,
	ddi								   AS SF_DDI,
	LTRIM(RTRIM(A.NAMEX))				   AS SF_Account_Name,
	TYPEX							   AS SF_Account_Type, 
	SUB_TYPE							   AS SF_Account_Sub_Type,
	AM.User							   AS SF_Account_Manager,
	ACCOUNT_MANAGER					   AS SF_Account_Manager_ID,
	AM.User_Role						   AS SF_Account_Manager_Role,	
	AM.Group						   AS SF_Account_Manager_Group,
	AM.Sub_Group						   AS SF_Account_Manager_Sub_Group,	
	Acctowner.User					   AS SF_Account_Owner,
	A.OWNERID							   AS SF_Account_Owner_ID,
	Acctowner.EMPLOYEENUMBER				   AS SF_Account_Owner_Employee_Number,
	Acctowner.User_Role				   AS SF_Account_Owner_Role,
	Acctowner.Group					   AS SF_Account_Owner_Group,
	Acctowner.Sub_Group				   AS SF_Account_Owner_Sub_Group,
	'N/A'							   AS SF_GM, 	
	'N/A'							   AS SF_Director, 
	'N/A'							   AS SF_VP, 
	'N/A'							   AS SF_Manager, 
	'N/A'							   AS SF_Business_Unit,  
	'N/A'							   AS SF_Region,
	'N/A'							   AS SF_Segment,
	'N/A'							   AS SF_Sub_Segment,
	'N/A'							   AS SF_Team,
	'N/A'							   AS SF_Hierachy_Attribute_Key,
	'N/A'							   AS Hierarchy_Reporting_Group,
	LASTMODIFIEDDATE					   AS LASTMODIFIEDDATE

FROM
  (Select
	  A.ID,
	  A.NAMEX,
	  A.TYPEX,
	  ACC_Owner            AS OWNERID,
	  A.ACCOUNT_NUMBER,
	  A.SUB_TYPE,    
	  A.ACCOUNT_MANAGER,    
	  ddi,
	  A.LASTMODIFIEDDATE 
 FROM
 `rax-landing-qa`.salesforce_ods.qaccounts  A                                                         
 WHERE  
	  A.DELETE_FLAG='N' 
 -- AND A.TYPEX IN ('Cloud Customer','Former Cloud Customer')
 AND DDI IS NOT NULL
) A	
LEFT OUTER JOIN
	USERS Acctowner
ON A.OWNERID =Acctowner.User_ID
LEFT OUTER JOIN
	USERS Am
ON A.ACCOUNT_MANAGER=AM.User_ID;
-----------------------------------------------------------------------------------------------------------------------

create or replace temp table  QAccount_DDI as	
SELECT--INTO    #QAccount_DDI
    SF_Account_ID,
    SF_Core_Account_Number,
    A.SF_DDI,
    SF_Account_Name,
    SF_Account_Type, 
    SF_Account_Sub_Type,
    SF_Account_Manager,
    SF_Account_Manager_ID,
    SF_Account_Manager_Role,	
    SF_Account_Manager_Group,
    SF_Account_Manager_Sub_Group,	
    SF_Account_Owner,
    SF_Account_Owner_ID,
    SF_Account_Owner_Employee_Number,
    SF_Account_Owner_Role,
    SF_Account_Owner_Group,
    SF_Account_Owner_Sub_Group,
    SF_GM, 	
    SF_Director, 
    SF_VP, 
    SF_Manager, 
    SF_Business_Unit,  
    SF_Region,
    SF_Segment,
    SF_Sub_Segment,
    SF_Team,
    SF_Hierachy_Attribute_Key,
    Hierarchy_Reporting_Group
FROM QAccount A
INNER JOIN
(
SELECT
    SF_DDI,
    MAX(LASTMODIFIEDDATE) AS MAX_LASTMODIFIEDDATE
FROM QAccount
GROUP BY
    SF_DDI
)B
 ON LASTMODIFIEDDATE=B.MAX_LASTMODIFIEDDATE
AND A.SF_DDI=B.SF_DDI;


create or replace temp table  Linked_Accounts as	
SELECT --into	#Linked_Accounts
    CAST(' ' as string)					 AS ACCOUNTID,
	Entity_number1								 AS Core_Account,
	CAST(entity_number2	as string)			 AS DDI,
	Match_Time								 AS creation_date,
	Entity_number1								 AS Account_Num	

FROM (
SELECT 
	Entity_number1,
	entity_number2,
	Match_Time
FROM
	`rax-datamart-dev`.corporate_dmart.vw_entity_match  B  
WHERE  
	lower(entity_source1)='salesforce'
and lower(entity_source2) ='hostingmatrix'
and lower(status) ='verified'
);


create or replace temp table Keizan_XX as 
SELECT --into	#Keizan_XX
    Keizan_Cloud_DDI,
    Keizan_Cloud_Segment,
    Keizan_Cloud_Region,
    Keizan_Cloud_Sub_Region,
    CASE WHEN `rax-staging-dev.bq_functions.udf_is_numeric`(Keizan_Core_Account)=0 then '0' else Keizan_Core_Account end as Keizan_Core_Account,
    Keizan_Cloud_BU,
    Keizan_Cloud_Team,
    Keizan_Cloud_Sub_Team,
    cast('1900-01-01' as datetime)			AS Keizan_Match_Date,
    Keizan_Cloud_AM,
    Keizan_Cloud_Onboarding_Specialist,
    Keizan_Cloud_Advisor,
    Keizan_Cloud_BDC,
    Keizan_Cloud_Tech_Lead, 
    Keizan_Cloud_Sales_Associate, 
    Keizan_Cloud_Launch_Manager,    
    Keizan_Cloud_TAM,
    Keizan_Cloud_Secondary_TAM
FROM (
SELECT 
    cast(ddi as string)			aS Keizan_Cloud_DDI,
    max("group")						AS Keizan_Cloud_Segment,
    max(region)						AS Keizan_Cloud_Region,
    max(subregion)						AS Keizan_Cloud_Sub_Region,
    cast(core as string)				AS Keizan_Core_Account,
    max(segment)						AS Keizan_Cloud_BU,
    max(team)						AS Keizan_Cloud_Team,
    max(sub_team)						AS Keizan_Cloud_Sub_Team,
    max(am)							AS Keizan_Cloud_AM,
    max(onboardingspecialist)			     AS Keizan_Cloud_Onboarding_Specialist,
    max(cloudadvisor)				     AS Keizan_Cloud_Advisor,
    max(businessdevconsultant)		     AS Keizan_Cloud_BDC,
    max(techlead)					     AS Keizan_Cloud_Tech_Lead, 
    max(salesassociate)				     AS Keizan_Cloud_Sales_Associate, 
    max(launchmanager)				     AS Keizan_Cloud_Launch_Manager,    
    max(tam)						     AS Keizan_Cloud_TAM,
    max(secondarytam_faws)				AS Keizan_Cloud_Secondary_TAM
FRom
`rax-landing-qa`.keizan_ods.teams  B  
GROUP BY
	ddi,
	core);


UPDATE Keizan_XX x
SET
	x.Keizan_Match_Date=creation_date
FROM
	Keizan_XX A
INNER JOIN
	Linked_Accounts B
ON A.Keizan_Core_Account=cast(B.Core_Account as string)
where true;


-----------------------------------------------------------------------------------------------------------------------


create or replace table `rax-abo-72-dev`.slicehost.keizan_stage as 
SELECT
    Keizan_Cloud_DDI,
    Keizan_Core_Account,
    Keizan_Cloud_BU,
    Keizan_Cloud_Region, 
    Keizan_Cloud_Sub_Region,
    Keizan_Cloud_Segment,
    Keizan_Cloud_Team,
    Keizan_Cloud_Sub_Team,
    Keizan_Match_Date,
    Keizan_Cloud_AM,
    Keizan_Cloud_Onboarding_Specialist,
    Keizan_Cloud_Advisor,
    Keizan_Cloud_BDC,
    Keizan_Cloud_Tech_Lead, 
    Keizan_Cloud_Sales_Associate, 
    Keizan_Cloud_Launch_Manager,    
    Keizan_Cloud_TAM,
    Keizan_Cloud_Secondary_TAM
FROM
(	
SELECT
	Keizan_Cloud_DDI		AS Min_DDI,
	MIN(Keizan_Match_Date)	AS Min_creation_date
 FROM Keizan_XX
GROUP BY
	Keizan_Cloud_DDI
)A
 INNER JOIN Keizan_XX B
ON A.Min_DDI=B.Keizan_Cloud_DDI
AND A.Min_creation_date=Keizan_Match_Date;

create or replace temp table team_all as   
SELECT *-- INTO    #team_all
FROM
  (SELECT* FROM
  `rax-landing-qa`.ss_db_ods.team_all sslteam 
WHERE 
    cast(deleted_at as date) = cast('1970-01-01' as date)
AND (subsegment IS NOT NULL 
and country IS NOT NULL 
and subsegment <> ' ' 
and country <> ' ')
);

create or replace temp table Keizan_ALL as  
SELECT  --INTO	#Keizan_ALL 
    Keizan_Cloud_DDI,
    Keizan_Core_Account,
    Keizan_Match_Date,
    ifnull(SSLteam.country ,B.Country)					    AS Keizan_Country,
    ifnull(Keizan_Cloud_Region,B.Region)				    AS Keizan_Region,
    ifnull(Keizan_Cloud_Sub_Region,B.Sub_Region)			    AS Keizan_Sub_Region,
    ifnull(Keizan_Cloud_BU,B.Team_Business_Unit)			    AS Keizan_Business_Unit,
    ifnull(Keizan_Cloud_Segment,B.Team_Business_Segment)	    AS Keizan_Segment,
    ifnull(B.Team_Reporting_Segment, 'Other')			    AS Keizan_Reporting_Segment,
    ifnull(SSLteam.subsegment,B.Team_Business_Sub_Segment)    AS Keizan_Sub_Segment,
    Keizan_Cloud_Sub_Team							    AS Keizan_Cloud_Sub_Team,
    A.Keizan_Cloud_Team								    AS Keizan_Cloud_Team,
    A.Keizan_Cloud_AM								    AS Keizan_Cloud_AM,
    A.Keizan_Cloud_Onboarding_Specialist				    AS Keizan_Cloud_Onboarding_Specialist,
    A.Keizan_Cloud_Advisor							    AS Keizan_Cloud_Advisor,
    A.Keizan_Cloud_BDC								    AS Keizan_Cloud_BDC,
    A.Keizan_Cloud_Tech_Lead							    AS Keizan_Cloud_Tech_Lead, 
    A.Keizan_Cloud_Tech_Lead							    AS Keizan_Cloud_Sales_Associate, 
    A.Keizan_Cloud_Launch_Manager						    AS Keizan_Cloud_Launch_Manager,    
    A.Keizan_Cloud_TAM,
    A.Keizan_Cloud_Secondary_TAM			
FROM
	`rax-abo-72-dev`.slicehost.keizan_stage A
LEFT OUTER JOIN
	`rax-abo-72-dev`.report_tables.dim_support_team_hierarchy B 
ON A.Keizan_Cloud_Team=B.Team_Name
LEFT OUTER JOIN
    team_all	 SSLteam
On A.Keizan_Cloud_Team=SSLteam.name;

create or replace temp table Max_invoiced as 
SELECT --INTO	#Max_invoiced
	Account								AS ACT_AccountID,
	CAST(MIN(invoice_Date) as date)	AS Min_Invoice_Date,
	CAST(MAX(invoice_Date) as date)	AS Max_Invoice_Date
FROM
	`rax-abo-72-dev`.slicehost.cloud_invoice_detail 
GROUP BY
	Account;
	
create or replace temp table tempSLA as 	
SELECT --into	#tempSLA
	substr(account_no, strpos(account_no,'-')+1,64)							AS ACCOUNT_ID,
	GL_SEGMENT																		AS GL_SEGMENT,
	CASE WHEN (pd_VALUE =  'TRUE' OR  upper(pd_VALUE) like '%MANAGED%') THEN 1 ELSE 0 END	AS MGD_FLAG,
	concat(NAME , '--' , pd_VALUE , ifnull(concat(' ', sla_VALUE), ''))								AS SLA_NAME,
	DATETIME_ADD(cast('1970-01-01' as datetime), interval cast(profile_Effective_T as int64) second)										AS SLA_Effective_Date
FROM (
SELECT 
    a.account_no,
    GL_SEGMENT,
    PD.VALUE		    AS pd_VALUE,
    PD.NAME,		    
    sla.VALUE		    AS sla_VALUE,
    A.Effective_T	    AS Act_Effective_T,
    p.Effective_T	    AS profile_Effective_T		
From 
   `rax-landing-qa`.brm_ods.account_t A   
INNER JOIN
   `rax-landing-qa`.brm_ods.profile_t p 
on a.poid_id0 = p.Account_Obj_Id0
LEFT OUTER JOIN
   `rax-landing-qa`.brm_ods.profile_acct_extrating_data_t PD   
on PD.OBJ_ID0 = p.POID_ID0
LEFT OUTER JOIN 
    `rax-landing-qa`.brm_ods.profile_acct_extrating_data_t sla  
ON  p.POID_ID0 = sla.OBJ_ID0 
and upper(p.NAME) = 'MANAGED_FLAG'
AND upper(sla.NAME)='SERVICE_TYPE'
where 
    upper(PD.NAME)='MANAGED'
and upper(p.NAME )= 'MANAGED_FLAG'
AND lower(a.GL_SEGMENT) IN ('.cloud.us')
);

create or replace temp table dedicated as 
SELECT --INTO	#dedicated
    number,
    id,
    parentAccountNumber,	
    parentAccountId,
    CAST (rcn as string) AS rcn

FROM (
SELECT
    *
FROM
    `rax-landing-qa`.cms_ods.customer_account A 
WHERE
    upper(type)='MANAGED_HOSTING');
	
create or replace temp table Customer_Account as 	
SELECT --INTO   #Customer_Account
    number, 
    id, 
    CAST('N/A' as string)				   AS Account_Number,
    parentAccountNumber,
    parentAccountId, 
    name, 
    type, 
    status, 
    CAST(rcn as string)					  AS rcn,
    createdDate, 
    tier

FROM (
SELECT
    number, 
    A.id, 
    parentAccountNumber,
    parentAccountId, 
    name, 
    A.type, 
    A.status, 
    rcn,
    createdDate, 
    tier  
FROM
     `rax-landing-qa`.cms_ods.customer_account A 
WHERE
   `rax-staging-dev.bq_functions.udf_is_numeric`(A.number)=1 AND
upper(A.TYPE) IN ('CLOUD','SITES_ENDUSER') )  ---RV 06/26/2019  modified the where clause
where
 CAST(number as int64) < 10000000
 ;
----------------------------------------------------------------------------------------------------


create or replace temp table ConcatenationDemo as 
SELECT --INTO      #ConcatenationDemo
    Account,
    contactNumber,
    Number

FROM (
SELECT  
    A.number			   AS Account,
    phone.contactNumber,
    phone.Number
FROM 
      `rax-landing-qa`.cms_ods.customer_account A  
LEFT OUTER JOIN
    `rax-landing-qa`.cms_ods.contact_roles B  
ON A.number= B.customerAccountNumber
AND upper(B.CUSTOMERACCOUNTTYPE) IN ('CLOUD','SITES_ENDUSER') -- TO GET ONLY CLOUD FEEDS
AND value='PRIMARY'
AND A.type=customerAccountType
LEFT OUTER JOIN
   `rax-landing-qa`.cms_ods.contact_phonenumbers phone  
ON B.contactNumber= phone.contactNumber
WHERE 
    `rax-staging-dev.bq_functions.udf_is_numeric`(A.number)=1 
AND upper(A.TYPE) IN ('CLOUD','SITES_ENDUSER') -- TO GET ONLY CLOUD FEEDS

) WHERE CAST(Account as int64) < 10000000
;

create or replace temp table Contact_PhoneNumbers as 
SELECT --INTO    #Contact_PhoneNumbers
    Account,
    contactNumber, 
	STRING_AGG (Number,'') as Phone

FROM  ConcatenationDemo AS x
GROUP BY 
    Account,contactNumber;
    
	
create or replace temp table Cloud_Account_Contact_Info as 
SELECT --INTO	#Cloud_Account_Contact_Info
    ID									  AS Account_ID, 
    CAST('N/A' as string)				  AS Account_Number,
    contactNumber							  AS contactNumber,
    CAST(rcn as string)					  AS RCN,
    AccountName							  AS AccountName, 		
    status								  AS Account_Status,
    createdDate							  AS Account_Created_Date,
    extract (day from cast(createdDate as date))						  AS DesiredBillingDate,
    FirstName,
    LastName, 
    username								  AS UserName,
    CAST(REPLACE(substr(SHD_EmailID,5,length(SHD_EmailID)),'-','') as int64)	  AS SHD_EmailID,
    ifnull(Address,'N/A')					  AS Email, 
    RTRIM(Street)							  AS Street,
    RTRIM(City)							  AS City, 
    RTRIM(ifnull(State,'Unknown'))				  AS State,
    RTRIM(zipcode)							  AS PostalCode, 
    RTRIM(Country)							  AS Country,
    code								  AS CountryCode,	
    current_date()								  AS Refresh_Date

FROM (
SELECT  
    A.ID, 
    B.contactNumber,
    A.Number								  AS Account_Number,
    A.rcn,
    A.Name								  AS AccountName, 		
    A.status,
    A.createdDate,
    PSN_Name.FirstName,
    PSN_Name.LastName, 
    PSN_Name.username,
    B.contactNumber						  AS SHD_EmailID,
    C.Address, 
    SHD_Address.Street,
    SHD_Address.City, 
    SHD_Address.State,
    SHD_Address.zipcode, 
    SHD_Country.Name						  AS Country,
    SHD_Country.code
FROM 
	`rax-landing-qa`.cms_ods.customer_account A  
LEFT OUTER JOIN
    `rax-landing-qa`.cms_ods.contact_roles B  
ON A.number= B.customerAccountNumber
AND upper(B.CUSTOMERACCOUNTTYPE) IN ('CLOUD','SITES_ENDUSER') -- TO GET ONLY CLOUD FEEDS
AND value='PRIMARY'
LEFT OUTER JOIN
    `rax-landing-qa`.cms_ods.customer_contact PSN_Name  
ON B.contactNumber= PSN_Name.contactNumber
LEFT OUTER JOIN
     `rax-landing-qa`.cms_ods.contact_emailaddress C  
ON B.contactNumber= C.contactNumber
AND C.primary is true
LEFT OUTER JOIN
     `rax-landing-qa`.cms_ods.contact_addresses SHD_Address  
ON B.contactNumber= SHD_Address.contactNumber
AND SHD_Address.primary is true
LEFT OUTER JOIN
     `rax-landing-qa`.cms_ods.countries SHD_Country  
ON SHD_Address.country= SHD_Country.code
WHERE  `rax-staging-dev.bq_functions.udf_is_numeric`(A.number)=1 AND
       upper(A.type)='CLOUD'
AND    upper(A.TYPE) IN ('CLOUD','SITES_ENDUSER') -- TO GET ONLY CLOUD FEEDS
) WHERE CAST(Account_Number as int64) < 10000000
;

create or replace temp table SSDB_Server_Level as 
SELECT --INTO	#SSDB_Server_Level
	DDI,
	service_level,
	service_type

FROM 
	(
SELECT  
	A.number		AS DDI,
	service_level,
	service_type
FROM 
	`rax-landing-qa`.ss_db_ods.account_all A
WHERE
	 upper(A.type)='CLOUD'
);


UPDATE Cloud_Account_Contact_Info ca
SET
  ca.Account_Number=B.number
FROM Cloud_Account_Contact_Info A
INNER JOIN dedicated B
ON A.rcn=B.rcn  
where true;

create or replace temp table Cloud_Account_Contact_Info_Current_All as
SELECT  --INTO	#Cloud_Account_Contact_Info_Current_All
    CAST(concat(CAST(A.Account_ID as string),'','Cloud_Hosting_US') as string) AS Cloud_Account_Key,
    A.Account_ID							  AS Account_ID, 
    Account_Number,
    CAST(rcn as string)					  AS CMS_RCN,
    0				   					  AS SliceHost_CustomerID,
    AccountName, 		
    0									  AS Account_Tenure,	
    Account_Status,
    actstatus.ID                                    AS Account_Status_ID,
    CASE	
    WHEN 
	    actstatus.Online<>1
    THEN
	    'Offline'
    ELSE
	    'Online'	
    END									  AS Account_Status_Online,
    actstatus.Online						  AS Account_Status_Online_ID,
    CAST('Cloud US' as string)			AS Account_Type,
	CAST('Cloud Customer' as string)	AS Account_Customer_Type,
    3									  AS Account_Type_ID,
    'N/A'				   					  AS Account_SLAType,
    0									  AS Account_SLATypeID,
    'Infrastructure'						  AS Account_Service_Level,
    'legacy'								  AS Account_Service_Level_Type,
    0									  AS Account_Service_Level_ID,
    ifnull(MGD_FLAG,0)									   AS Is_Managed_Flag,
    ifnull(SLA_BRM_NAME,'unassigned')						   AS Account_SLA_Type_BRM_Desc,	    
    ifnull(SLAD.SLA_NAME,'unassigned')						   AS Account_SLA_Name_BRM,
    ifnull(SLAD.SLA_Type,'unassigned')						   AS Account_SLA_Type_BRM,   
	0														   AS Account_SLA_Name_BRM_Is_Managed ,  
    CAST(ifnull(SLA_Effective_Date,'1900-01-01') as datetime)		   AS Account_SLA_Type_BRM_Effective_Date,				    
    CAST(Account_Created_Date	as datetime)					   AS Account_Created_Date,
    CAST(ifnull(Max_Invoice_Date,Account_Created_Date)as datetime)  AS Account_End_Date,
    CAST(ifnull(Min_Invoice_Date,'1900-01-01')as datetime)		   AS Account_First_Billed_Date,
    CAST(ifnull(Max_Invoice_Date,'1900-01-01')as datetime)		   AS Account_Last_Billed_Date,
    extract(day from cast(A.Account_Created_Date as date)	)						   AS DesiredBillingDate,
     0												   AS Consolidated_Billing,
    '1900-01-01'											   AS Consolidated_Create_Date,
    0												   AS Managed_Flag,
    FirstName,
    LastName, 
    username								  AS UserName,
    SHD_EmailID,
    Email, 
	ifnull(substr(Email,strpos(Email,'@')+1,length(Email)),'N/A') 
										  AS Domain,		
    0									  AS Internal_Flag,
    0									  AS Domain_Internal_Flag,
    Phone, 
    Street,
    City, 
    State,
    PostalCode, 
    Country,
    CountryCode,	
    current_date()								  AS Refresh_Date

FROM  Cloud_Account_Contact_Info A  
LEFT OUTER JOIN Contact_PhoneNumbers p  
ON A.contactNumber= p.contactNumber
AND A.Account_ID=Account
LEFT OUTER JOIN
   `rax-abo-72-dev`.slicehost.act_val_accountstatus  actstatus
On A.Account_Status=actstatus.Name
LEFT OUTER JOIN Max_invoiced B
ON A.Account_ID =cast(B.ACT_AccountID as string)
LEFT OUTER JOIN tempSLA SLA
ON A.Account_ID =SLA.ACCOUNT_ID
LEFT OUTER JOIN
	`rax-abo-72-dev`.cloud_usage.dim_cloud_sla SLAD 
ON SLA.GL_SEGMENT=SLAD.GL_SEGMENT
AND SLA.SLA_NAME=SLAD.SLA_BRM_NAME
;


UPDATE Cloud_Account_Contact_Info_Current_All c
SET
	c.Account_Customer_Type='Former Cloud Customer'
FROM Cloud_Account_Contact_Info_Current_All A
WHERE
	lower(A.Account_Status)='closed';

-------------------------------------------------------------------------------------------------------------
UPDATE Cloud_Account_Contact_Info_Current_All c
 SET
	c.Account_Customer_Type=CASE
							WHEN
								A.Account_Last_Billed_Date >= cast(DATE_ADD(Current_date(), interval -31 DAY) as date)
							AND A.Account_Last_Billed_Date <=  current_date()
							AND A.Account_Status not in('Closed', 'Close')
							THEN  
								'Cloud Customer'
							ELSE
								'Former Cloud Customer'
							END
FROM Cloud_Account_Contact_Info_Current_All A 
where true;

-------------------------------------------------------------------------------------------------------------
UPDATE Cloud_Account_Contact_Info_Current_All c
SET 
	c.Account_Tenure=
	ifnull(Datediff(dd,A.Account_Created_Date,
			CASE WHEN DATEDIFF(D,ifnull(Account_Last_Billed_Date,Account_End_Date),current_date())>31 
			THEN current_date() 
			ELSE Account_End_Date END),0)
	--ifnull(ifnull(Account_Last_Billed_Date,CASE WHEN Account_End_Date='1900-01-01 00:00:00.000' THEn current_date() else Account_End_Date End),current_date())),0)
FROM Cloud_Account_Contact_Info_Current_All A
where true;
----------------------------------------------------------------------------------------------------
UPDATE  Cloud_Account_Contact_Info_Current_All c
SET	
	c.Domain_Internal_Flag=1			
FROM Cloud_Account_Contact_Info_Current_All A
WHERE
  (lower(A.Domain) like '%@rackspace%.co%' 
OR lower(A.Domain) like '%@rackspace%'
OR lower(A.Domain) like '%@racksapce%'
OR lower(A.Domain) like '%@lists.rackspace%'
OR lower(A.Domain) like '%@mailtrust%' 
OR lower(A.Domain) like '%@mosso%' 
OR lower(A.Domain) like '%@jungledisk%' 
OR lower(A.Domain) like '%@slicehost%'
OR lower(A.Domain) like '%@cloudkick%'
OR lower(A.Domain) like '%@ackspace%'
OR lower(A.Domain) like '%@test.com%'
OR lower(A.Domain) like '%@test.co.uk%'
OR lower(A.Accountname) like '%rackspace%'
OR lower(A.Accountname) like '%datapipe%'
);

UPDATE  Cloud_Account_Contact_Info_Current_All c
SET
    c.Account_Service_Level=ifnull(service_level,'infrastructure'), 
    c.Account_Service_Level_Type=ifnull(service_type, 'legacy') 
FROM Cloud_Account_Contact_Info_Current_All A
INNER JOIN 
	SSDB_Server_Level B
on a.Account_ID = B.DDI
where true;  
----------------------------------------------------------------------------------------------------   
UPDATE  Cloud_Account_Contact_Info_Current_All c
SET
	c.Account_Service_Level_ID=1
FROM
    Cloud_Account_Contact_Info_Current_All A
WHERE
    lower(A.Account_Service_Level)='managed';
	
----------------------------------------------------------------------------------------------------
UPDATE  Cloud_Account_Contact_Info_Current_All c
SET
	Account_SLA_Name_BRM_Is_Managed=1
FROM Cloud_Account_Contact_Info_Current_All A
WHERE
    lower(A.Account_SLA_Name_BRM)='managed';
----------------------------------------------------------------------------------------------------   
UPDATE  Cloud_Account_Contact_Info_Current_All c
SET
    c.Account_SLA_Name_BRM_Is_Managed=0,
    c.Account_SLA_Type_BRM_Desc='unassigned',
    c.Account_SLA_Name_BRM='unassigned',
    c.Account_SLA_Type_BRM='unassigned' 
FROM Cloud_Account_Contact_Info_Current_All A
WHERE
    A.Account_Type_ID=2;

create or replace temp table  MAX_SHD_EmailID as 
SELECT--INTO    #MAX_SHD_EmailID
	Account_ID,
	MAX(SHD_EmailID) AS MAX_SHD_EmailID

FROM Cloud_Account_Contact_Info_Current_All A
GROUP BY
	Account_ID;

create or replace temp table  Cloud_Account_Contact_Info_Current_temp as	
SELECT  --INTO    #Cloud_Account_Contact_Info_Current
    Cloud_Account_Key,
    A.Account_ID, 
    CAST(CMS_RCN as string)		AS CMS_RCN,
     CAST('N/A' AS string)		AS RCN,
    CAST('N/A' AS string)		AS GCN,
    Account_Number,
    SliceHost_CustomerID,
    AccountName, 		
    Account_Tenure,	
    Account_Status,
    Account_Status_ID,
    Account_Status_Online,
    Account_Status_Online_ID,
    Account_Type,
    Account_Type_ID,
	Account_Customer_Type,
    Account_SLAType,
    Account_SLATypeID,
    Account_Service_Level_Type,
    Account_Service_Level,
    Account_Service_Level_ID,
    Is_Managed_Flag,
    Account_SLA_Type_BRM_Desc,	    
    Account_SLA_Name_BRM,
    Account_SLA_Type_BRM,  
    Account_SLA_Name_BRM_Is_Managed,   
    Account_SLA_Type_BRM_Effective_Date,				    
    Account_Created_Date,
    Account_End_Date,
    Account_First_Billed_Date,
    Account_Last_Billed_Date,
    '1900-01-01'					 AS HMDB_Last_Billed_Date,
    DesiredBillingDate,
     '1900-01-01'				 AS ContractDate,
    Consolidated_Billing,
    Consolidated_Create_Date,
    Managed_Flag,
    FirstName,
    LastName, 
    UserName,
    SHD_EmailID,
    Email, 
    CAST(Domain as string)	 AS Domain,	
    Internal_Flag,
    Domain_Internal_Flag,
    Phone, 
    Street,
    City, 
    State,
    PostalCode, 
    Country,
    CountryCode,	
    Refresh_Date

FROM Cloud_Account_Contact_Info_Current_All A
INNER JOIN MAX_SHD_EmailID B
ON A.Account_ID=B.Account_ID
AND ifnull(A.SHD_EmailID,0)=ifnull(B.MAX_SHD_EmailID,0);


UPDATE Cloud_Account_Contact_Info_Current_temp c
SET
   c.RCN=RCN.RCN
FROM Cloud_Account_Contact_Info_Current_temp A
INNER JOIN
    rcn_Temp	  AS RCN
ON A.Account_ID= RCN.account_Number
where true;
--------------------------------------------------------------------------------------------------- 

create or replace temp table  Billing as	
SELECT--INTO      #Billing
    Account,
    contactNumber,
    Number

FROM (
SELECT  
    A.number			   AS Account,
    phone.contactNumber,
    phone.Number
FROM 
      `rax-landing-qa`.cms_ods.customer_account A  
LEFT OUTER JOIN
    `rax-landing-qa`.cms_ods.contact_roles B  
ON A.number= B.customerAccountNumber
AND upper(B.CUSTOMERACCOUNTTYPE) IN ('CLOUD','SITES_ENDUSER') -- TO GET ONLY CLOUD FEEDS
AND value='BILLING'
LEFT OUTER JOIN
  `rax-landing-qa`.cms_ods.contact_phonenumbers phone  
ON B.contactNumber= phone.contactNumber
WHERE
     `rax-staging-dev.bq_functions.udf_is_numeric`(A.number)=1 
AND  upper(A.TYPE) IN ('CLOUD','SITES_ENDUSER') -- TO GET ONLY CLOUD FEEDS
) 
WHERE CAST(Account as int64) < 10000000
;
 
 create or replace temp table  Contact_BillingPhoneNumbers as
SELECT --INTO   #Contact_BillingPhoneNumbers
    Account,
    contactNumber, 
	STRING_AGG(Number,', ') as Billing_Phone
FROM  Billing AS x
GROUP BY 
    Account,contactNumber;

 create or replace temp table  Billing_Contact_Info_ALL as
SELECT -- INTO	#Billing_Contact_Info_ALL
    ID									  AS Account_ID, 
    contactNumber,
    AccountName							  AS Billing_AccountName, 	
    FirstName								  AS Billing_FirstName,
    LastName								  AS Billing_LastName, 
    CAST(REPLACE(substr(SHD_EmailID,5,length(SHD_EmailID)),'-','') as int64)	  
										  AS SHD_EmailID,	
    ifnull(Address,'N/A')					  AS Billing_Email, 
    ifnull(substr(Address,strpos(Address,'@')+1,length(Address)),'N/A') 
										  AS Billing_Domain,	
    0									  AS Billing_Internal_Flag,
    0									  AS Billing_Domain_Internal_Flag,
    RTRIM(Street)							  AS Billing_Street,
    RTRIM(City)							  AS Billing_City, 
    RTRIM(ifnull(State,'Unknown'))				  AS Billing_State,
    RTRIM(zipcode)							  AS Billing_PostalCode, 
    RTRIM(Country)							  AS Billing_Country,
    code								  AS Billing_CountryCode	

FROM (
SELECT 
    A.ID, 
    B.contactNumber,
    A.Number								  AS Account_Number,
    A.rcn,
    A.Name								  AS AccountName, 		
    A.createdDate,
    PSN_Name.FirstName,
    PSN_Name.LastName, 
    PSN_Name.username,
    B.contactNumber						  AS SHD_EmailID,
    C.Address, 
    SHD_Address.Street,
    SHD_Address.City, 
    SHD_Address.State,
    SHD_Address.zipcode, 
    SHD_Country.Name						  AS Country,
    SHD_Country.code
FROM 
	`rax-landing-qa`.cms_ods.customer_account A  
LEFT OUTER JOIN
    `rax-landing-qa`.cms_ods.contact_roles B  
ON A.number= B.customerAccountNumber
AND upper(B.CUSTOMERACCOUNTTYPE) IN ('CLOUD','SITES_ENDUSER') -- TO GET ONLY CLOUD FEEDS
AND value='BILLING'
LEFT OUTER JOIN
    `rax-landing-qa`.cms_ods.customer_contact PSN_Name  
ON B.contactNumber= PSN_Name.contactNumber
LEFT OUTER JOIN
    `rax-landing-qa`.cms_ods.contact_emailaddress C  
ON B.contactNumber= C.contactNumber
AND C.primary is true
LEFT OUTER JOIN
    `rax-landing-qa`.cms_ods.contact_addresses SHD_Address  
ON B.contactNumber= SHD_Address.contactNumber
AND SHD_Address.primary is true
LEFT OUTER JOIN
    `rax-landing-qa`.cms_ods.countries SHD_Country  
ON SHD_Address.country= SHD_Country.code
WHERE  
    `rax-staging-dev.bq_functions.udf_is_numeric`(A.number)=1 
AND upper(A.type)='CLOUD'
AND upper(A.TYPE) IN ('CLOUD','SITES_ENDUSER') -- TO GET ONLY CLOUD FEEDS
) 
    WHERE CAST(Account_Number as int64) < 10000000
	;


UPDATE  Billing_Contact_Info_ALL b
SET	
	 b.Billing_Internal_Flag=1,
     b.Billing_Domain_Internal_Flag=1
FROM Billing_Contact_Info_ALL A
WHERE
  (lower(A.Billing_Email) LIKE '%@rackspace%.co%' 
OR lower(A.Billing_Email) LIKE '%@rackspace%'
OR lower(A.Billing_Email) LIKE '%@racksapce%'
OR lower(A.Billing_Email) LIKE '%@lists.rackspace%'
OR lower(A.Billing_Email) LIKE '%@mailtrust%' 
OR lower(A.Billing_Email) LIKE '%@mosso%' 
OR lower(A.Billing_Email) LIKE '%@jungledisk%' 
OR lower(A.Billing_Email) LIKE '%@slicehost%'
OR lower(A.Billing_Email) LIKE '%@cloudkick%'
OR lower(A.Billing_Email) like '%@ackspace%'
OR lower(A.Billing_Email) like '%@test.com%'
OR lower(A.Billing_Email) like '%@test.co.uk%'
OR lower(A.Billing_AccountName) like '%rackspace%'
OR lower(A.Billing_AccountName) like '%datapipe%'
)
AND A.Billing_Domain_Internal_Flag=0
;

----------------------------------------------------------------------------------------------------
create or replace temp table MAX_Billing_SHD_EmailID as 
SELECT--INTO    #MAX_Billing_SHD_EmailID
	Account_ID,
	MAX(SHD_EmailID) AS MAX_SHD_EmailID

FROM Billing_Contact_Info_ALL A
GROUP BY
	Account_ID;
----------------------------------------------------------------------------------------------------

create or replace temp table Billing_Contact_Info as 
SELECT  --INTO    #Billing_Contact_Info
     A.Account_ID, 
     Billing_AccountName, 
	Billing_FirstName,
	Billing_LastName, 
	Billing_Email, 
	CAST(Billing_Domain as string) AS Billing_Domain,
	Billing_Internal_Flag,
	Billing_Domain_Internal_Flag,
	Billing_Phone, 
	Billing_Street,
	Billing_City, 
	Billing_State,
	Billing_PostalCode, 
	Billing_Country,
	Billing_CountryCode

FROM Billing_Contact_Info_ALL A
LEFT OUTER JOIN
   Contact_BillingPhoneNumbers phone
ON A.contactNumber= phone.contactNumber
INNER JOIN
    MAX_Billing_SHD_EmailID B
ON A.Account_ID = B.Account_ID --added 8/3/2015
AND ifnull(A.SHD_EmailID,0)=ifnull(B.MAX_SHD_EmailID,0)
;

create or replace temp table CMS_Consolidated as 
SELECT   --INTO    #CMS_Consolidated
    ACCOUNT_ID
    ,LINE_OF_BUSINESS
    ,ACCOUNT_NUMBER
    ,Consolidation_Account
    ,Consolidation_date
    ,Is_Consolidation_Account

FROM 
    `rax-abo-72-dev`.slicehost.brm_cloud_account_profile 
WHERE
    Is_Consolidation_Account=1
AND upper(LINE_OF_BUSINESS)='US_CLOUD'
AND upper(BRM_ACCOUNT_NO) not like '%INCORRECT%'
;
create or replace temp table CMS_Account_Attributes as 
SELECT   --INTO    #CMS_Account_Attributes
    ACCOUNT_ID
    ,bdom
    ,LINE_OF_BUSINESS
    ,ACCOUNT_NUMBER
     ,Is_Racker_Account
    ,Is_Internal_Account
FROM 
    `rax-abo-72-dev`.slicehost.brm_cloud_account_profile 
WHERE
    upper(LINE_OF_BUSINESS)='US_CLOUD'
AND upper(BRM_ACCOUNT_NO) not like '%INCORRECT%';

create or replace table `rax-abo-72-dev`.slicehost.cloud_account_contact_info_current as 
SELECT  
    CAST(0 as int64)										   AS DW_Account_Key,
    Cloud_Account_Key, 
    A.Account_ID, 
    CMS_RCN,
    RCN,
    GCN,
    SF_Account_ID										   AS SF_Account_ID,
    SF_Account_Sub_Type,
    SF_Account_Sub_Type 									   AS SF_Account_Sub_Type_UWBL,
    SF_Account_ID										   AS QA_Account_ID,
    ifnull(Keizan_Core_Account,'-1')						   AS Keizan_Core_Account,
    ifnull(Keizan_Match_Date,'1900-01-01')					   AS Keizan_Match_Date,
    CAST('-1'	as string)								   AS Legally_Linked_Core_Account,
    CAST('1900-01-01'	as datetime)							   AS Legally_Linked_Date,
    Account_Number, 
    SliceHost_CustomerID, 
    AccountName, 
    Account_Tenure, 
    Account_Status, 
    Account_Status_ID,
    Account_Status_Online, 
    Account_Status_Online_ID, 
    Account_Type, 
    Account_Type_ID, 
    Account_Customer_Type,
    Account_SLAType, 
    Account_SLATypeID, 
    Account_Service_Level,
    Account_Service_Level_Type,
    Account_Service_Level_ID, 
    Is_Managed_Flag, 
    Account_SLA_Type_BRM_Desc, 
    Account_SLA_Name_BRM, 
    Account_SLA_Type_BRM, 
    Account_SLA_Name_BRM_Is_Managed,
    Account_SLA_Type_BRM_Effective_Date, 
    Account_Created_Date, 
    DesiredBillingDate										AS Account_Desired_Billed_Date,
    0													AS Account_BDOM_BRM,
    Account_First_Billed_Date, 
    Account_Last_Billed_Date,
    HMDB_Last_Billed_Date,
    Account_End_Date, 
	CAST('1900-01-01' as datetime)								AS Account_Startup_Start_Date,
	CAST('1900-01-01' as datetime)								AS Account_Startup_End_Date,
    ifnull(Keizan_Region,'Other')								AS Keizan_Region,
    ifnull(Keizan_Sub_Region,'Other')							AS Keizan_Sub_Region,		
    ifnull(Keizan_Business_Unit,'Other')						AS Keizan_Business_Unit,
    ifnull(Keizan_Segment,'Other')								AS Keizan_Segment,
    ifnull(Keizan_Reporting_Segment,'Other')						AS Keizan_Reporting_Segment,
    ifnull(Keizan_Sub_Segment,'Other')							AS Keizan_Sub_Segment,
    ifnull(Keizan_Cloud_Team,'Other')							AS Keizan_Cloud_Team,
    ifnull(Keizan_Cloud_Sub_Team,'Other')						AS Keizan_Sub_Team,		
    ifnull(Keizan_Cloud_AM,'Other')							AS Keizan_Cloud_AM,
    ifnull(Keizan_Cloud_Onboarding_Specialist,'Other')				AS Keizan_Cloud_Onboarding_Specialist,
    ifnull(Keizan_Cloud_Advisor,'Other')						AS Keizan_Cloud_Advisor,
    ifnull(Keizan_Cloud_BDC,'Other')							AS Keizan_Cloud_BDC,
    ifnull(Keizan_Cloud_Tech_Lead,'Other')						AS Keizan_Cloud_Tech_Lead, 
    ifnull(Keizan_Cloud_Tech_Lead,'Other')						AS Keizan_Cloud_Sales_Associate, 
    ifnull(Keizan_Cloud_Launch_Manager,'Other')					AS Keizan_Cloud_Launch_Manager,    
    ifnull(Keizan_Cloud_TAM,'Other')							AS Keizan_Cloud_TAM,
    ifnull(Keizan_Cloud_Secondary_TAM,'Other')					AS Keizan_Cloud_Secondary_TAM,
    ifnull(ifnull(SF_Business_Unit,SF_Account_Owner_Group),'Other')	AS SF_Business_Unit,
    ifnull(SF_GM,'Other')									AS SF_GM,
    ifnull(SF_VP,'Other')									AS SF_VP,
    ifnull(SF_Director,'Other')	 							AS SF_Director,
    ifnull(SF_Manager,'Other')	 							AS SF_Manager,
    ifnull(SF_Segment,'Other')								AS SF_Segment,
    ifnull(SF_Sub_Segment,'Other')		 						AS SF_Sub_Segment,
    ifnull(SF_Team,'Other')								     AS SF_Team,
    ifnull(SF_Account_Manager,'Other')							AS SF_Account_Manager,
    ifnull(SF_Account_Owner,'Other')							AS SF_Account_Owner,
    ifnull(SF_Account_Owner_Group,'Other')						AS SF_Account_Owner_Group,
    ifnull(SF_Account_Owner_Sub_Group,'Other')				     AS SF_Account_Owner_Sub_Group,
    ifnull(SF_Account_Owner_Employee_Number,'Other')				AS SF_Account_Owner_EmployeeNumber,
    ifnull(SF_Hierachy_Attribute_Key,'Other')					AS SF_Hierachy_Attribute_Key,
    CAST(CASE 
    WHEN 
    (SF_Core_Account_Number IS NOT NULL OR SF_Core_Account_Number<>'')
    THEN
    'Q_Account'
    ELSE
	   'HMDB'
    END	as string)									AS SF_Account_Source,
    0				   									AS ON_Net_Revenue_Plan,
    DesiredBillingDate, 
    ContractDate, 
    ifnull(Keizan_Core_Account,'-1')							 AS Consolidated_Account,
    CASE
    WHEN 
    Keizan_Core_Account IS NOT NULL 
    THEN
    1
    ELSE
    0
    END													AS Consolidated_Billing,
    ifnull(Keizan_Match_Date,'1900-01-01')						AS Consolidated_Create_Date, 
    CASE
    WHEN 
    Keizan_Core_Account IS NOT NULL 
    THEN
    1
    ELSE
    0
    END													AS Keizan_Linked_Account,
    0													AS Is_Legally_Linked_Account,
    0													AS Is_RackConnect_Linked,
    FirstName, 
    LastName, 
    UserName, 
    Email, 
    Domain, 
    Phone, 
    Street, 
    City, 
    State, 
    PostalCode, 
    Country, 
    CountryCode, 
    Billing_FirstName,
    Billing_LastName, 
    Billing_Email, 
    Billing_Domain,		
    Billing_Internal_Flag,
    Billing_Domain_Internal_Flag,
    Billing_Phone, 
    Billing_Street,
    Billing_City, 
    Billing_State,
    Billing_PostalCode, 
    Billing_Country,
    Billing_CountryCode,
    0						 AS Internal_Flag,
    CASE
    WHEN
	    (Domain_Internal_Flag+Billing_Internal_Flag)<> 0
    THEN
	    1
    ELSE
	    0
    END						 AS Domain_Internal_Flag, 
    0						 AS BRM_Internal_Account,
    0						 AS BRM_Racker_Account,
    Refresh_Date, 
    current_date()					 AS Load_Date

FROM Cloud_Account_Contact_Info_Current_temp A
LEFT OUTER JOIN
	Keizan_ALL BB
ON A.Account_ID=BB.Keizan_Cloud_DDI
LEFT OUTER JOIN
    QAccount_DDI SCI 
ON CAST(A.Account_ID as string)=SF_DDI	
LEFT OUTER JOIN
    Billing_Contact_Info BI
ON A.Account_ID= BI.Account_ID

;

create or replace temp table Cloud_US_Dim_account as
SELECT     * --INTO	#Cloud_US_Dim_account
FROM (
SELECT  
    A.Account_Key,
    A.account_Number,
    A.account_Source_System_name
FROM
	`rax-datamart-dev`.corporate_dmart.dim_account  A 
WHERE
	A.current_Record = 1
AND upper(account_Source_System_name) In ('HOSTINGMATRIX','CMS')
);


UPDATE `rax-abo-72-dev`.slicehost.cloud_account_contact_info_current c
SET
	c.DW_Account_Key=Account_Key
FROM 
	`rax-abo-72-dev`.slicehost.cloud_account_contact_info_current A
INNER JOIN
	Cloud_US_Dim_account B
ON A.Account_ID= B.account_number
where true;
----------------------------------------------------------------------------------------------------
UPDATE `rax-abo-72-dev`.slicehost.cloud_account_contact_info_current d
SET
	d.DW_Account_Key=Account_Key
FROM 
	`rax-abo-72-dev`.slicehost.cloud_account_contact_info_current A
INNER JOIN
	Cloud_US_Dim_account B
ON A.Account_ID= B.account_number
where true;
--------------------------------------------------------------------------------------------------

UPDATE `rax-abo-72-dev`.slicehost.cloud_account_contact_info_current c
SET
	c.Account_Type='Cloud US Startup',
	c.Account_Startup_Start_Date=Startup_Start_Date,
	c.Account_Startup_End_Date=Startup_End_Date
FROM  `rax-abo-72-dev`.slicehost.cloud_account_contact_info_current A
INNER JOIN
	start_up B
ON A.Account_ID= cast(B.account_number as string)
where true;
--------------------------------------------------------------------------------------------------
Update  `rax-abo-72-dev`.slicehost.cloud_account_contact_info_current c
SET
    c.Is_RackConnect_Linked=1
FROM `rax-abo-72-dev`.slicehost.cloud_account_contact_info_current  A 
 INNER JOIN
	rackconnect B
ON cast(Keizan_Core_Account as string)=cast(B.account_number as string)
where true
;
----------------------------------------------------------------------------------------------------
UPDATE `rax-abo-72-dev`.slicehost.cloud_account_contact_info_current c
SET
	 c.Is_Legally_Linked_Account=1,
     c.Legally_Linked_Date=creation_date,
     c.Legally_Linked_Core_Account=Core_Account
FROM `rax-abo-72-dev`.slicehost.cloud_account_contact_info_current A
INNER JOIN
	Linked_Accounts B
ON A.Account_ID=B.DDI
where true;
----------------------------------------------------------------------------------------------------
UPDATE `rax-abo-72-dev`.slicehost.cloud_account_contact_info_current c
SET
	 c.Is_Legally_Linked_Account=1,
     c.Legally_Linked_Date=creation_date,
     c.Legally_Linked_Core_Account=Core_Account
FROM `rax-abo-72-dev`.slicehost.cloud_account_contact_info_current A
INNER JOIN
	Linked_Accounts B
ON A.Account_ID=B.DDI
where true;
----------------------------------------------------------------------------------------------------
UPDATE `rax-abo-72-dev`.slicehost.cloud_account_contact_info_current c
SET
	c.Keizan_Core_Account=-1
FROM `rax-abo-72-dev`.slicehost.cloud_account_contact_info_current A
WHERE
	A.Keizan_Core_Account = 0
;
----------------------------------------------------------------------------------------------------
UPDATE `rax-abo-72-dev`.slicehost.cloud_account_contact_info_current c
SET
	c.Account_Customer_Type='Cloud Customer'
FROM `rax-abo-72-dev`.slicehost.cloud_account_contact_info_current A
WHERE
	A.Account_Created_Date  >= cast(DATE_ADD(current_date(), interval -31 day) as date)
AND A.Account_Last_Billed_Date='1900-01-01'
AND lower(A.Account_Status) not in('Closed', 'Close');
----------------------------------------------------------------------------------------------------
UPDATE `rax-abo-72-dev`.slicehost.cloud_account_contact_info_current  c
SET
    c.Keizan_Core_Account=B.Consolidation_Account,
    c.Keizan_Linked_Account=1,
    c.Keizan_Match_Date=Consolidation_date,  
    c.Consolidated_Account=B.Consolidation_Account,
    c.Consolidated_Billing=1,
    c.Consolidated_Create_Date=Consolidation_date
FROM
    `rax-abo-72-dev`.slicehost.cloud_account_contact_info_current  A
INNER JOIN
    CMS_Consolidated B
On A.Account_ID= B.ACCOUNT_NUMBER
WHERE
    A.Keizan_Core_Account='-1';
----------------------------------------------------------------------------------------------------
UPDATE `rax-abo-72-dev`.slicehost.cloud_account_contact_info_current c
SET
    c.Account_BDOM_BRM=B.BDOM,
    c.BRM_Internal_Account=Is_Internal_Account, 
    c.BRM_Racker_Account=Is_Racker_Account
FROM
    `rax-abo-72-dev`.slicehost.cloud_account_contact_info_current  A
INNER JOIN
    CMS_Account_Attributes B
On A.Account_ID= B.ACCOUNT_NUMBER
where true;
----------------------------------------------------------------------------------------------------
UPDATE `rax-abo-72-dev`.slicehost.cloud_account_contact_info_current  c
SET
	c.Is_Managed_Flag=0
FROM
	`rax-abo-72-dev`.slicehost.cloud_account_contact_info_current  A
WHERE
	lower(A.Account_SLA_Name_BRM) <> 'managed';
----------------------------------------------------------------------------------------------------
UPDATE  `rax-abo-72-dev`.slicehost.cloud_account_contact_info_current c
SET	
	c.Domain_Internal_Flag=1
FROM
	`rax-abo-72-dev`.slicehost.cloud_account_contact_info_current  A
WHERE
  (lower(A.Email) LIKE '%@rackspace%.co%' 
OR lower(A.Email) LIKE '%@rackspace%'
OR lower(A.Email) LIKE '%@racksapce%'
OR lower(A.Email) LIKE '%@lists.rackspace%'
OR lower(A.Email) LIKE '%@mailtrust%' 
OR lower(A.Email) LIKE '%@mosso%' 
OR lower(A.Email) LIKE '%@jungledisk%' 
OR lower(A.Email) LIKE '%@slicehost%'
OR lower(A.Email) LIKE '%@cloudkick%'
OR lower(A.Email) like '%@ackspace%'
OR lower(A.Email) like '%@test.com%'
OR lower(A.Email) like '%@test.co.uk%'
OR lower(A.AccountName) like '%rackspace%'
OR lower(A.AccountName) like '%datapipe%'
)
AND A.Domain_Internal_Flag=0;

----------------------------------------------------------------------------------------------------
UPDATE  `rax-abo-72-dev`.slicehost.cloud_account_contact_info_current  c
SET	
	c.Billing_Domain_Internal_Flag=1,
	c.Billing_Internal_Flag=1
FROM
	`rax-abo-72-dev`.slicehost.cloud_account_contact_info_current  A
WHERE
  (lower(A.Billing_Email) LIKE '%@rackspace%.co%' 
OR lower(A.Billing_Email) LIKE '%@rackspace%'
OR lower(A.Billing_Email) LIKE '%@racksapce%'
OR lower(A.Billing_Email) LIKE '%@lists.rackspace%'
OR lower(A.Billing_Email) LIKE '%@mailtrust%' 
OR lower(A.Billing_Email) LIKE '%@mosso%' 
OR lower(A.Billing_Email) LIKE '%@jungledisk%' 
OR lower(A.Billing_Email) LIKE '%@slicehost%'
OR lower(A.Billing_Email) LIKE '%@cloudkick%'
OR lower(A.Billing_Email) like '%@ackspace%'
OR lower(A.Billing_Email) like '%@test.com%'
OR lower(A.Billing_Email) like '%@test.co.uk%'
OR lower(A.AccountName) like '%rackspace%'
OR lower(A.AccountName) like '%datapipe%'
)
AND A.Billing_Domain_Internal_Flag=0;
----------------------------------------------------------------------------------------------------
UPDATE `rax-abo-72-dev`.slicehost.cloud_account_contact_info_current
SET  
    Domain_Internal_Flag=1
FROM  `rax-abo-72-dev`.slicehost.cloud_account_contact_info_current A 
where 
    (A.Domain_Internal_Flag+Billing_Internal_Flag)<> 0
AND A.Domain_Internal_Flag=0;
----------------------------------------------------------------------------------------------------
UPDATE `rax-abo-72-dev`.slicehost.cloud_account_contact_info_current c
SET  
    c.Internal_Flag=1
FROM `rax-abo-72-dev`.slicehost.cloud_account_contact_info_current A
where 
    concat(A.Domain_Internal_Flag,A.Billing_Domain_Internal_Flag ,A.BRM_Internal_Account,A.Internal_Flag) <>0
AND A.Internal_Flag=0;
----------------------------------------------------------------------------------------------------
UPDATE `rax-abo-72-dev`.slicehost.cloud_account_contact_info_current c
SET  
    c.Internal_Flag=0
FROM  `rax-abo-72-dev`.slicehost.cloud_account_contact_info_current A 
where 
   (lower(a.accountname) like '%- rackspace%' and  lower(a.accountname) not like '%support%')
and lower(a.domain) not like '%rackspace%'
AND lower(A.Account_Number)<> 'n/a';
----------------------------------------------------------------------------------------------------
UPDATE `rax-abo-72-dev`.slicehost.cloud_account_contact_info_current
SET  
    c.Internal_Flag=0
FROM  `rax-abo-72-dev`.slicehost.cloud_account_contact_info_current A 
where 
   (lower(a.accountname) like '%- rackspace%' and  lower(a.accountname) not like '%support%')
and lower(a.billing_domain) not like '%rackspace%'
and lower(a.account_number)<> 'n/a'
;
----------------------------------------------------------------------------------------------------
UPDATE `rax-abo-72-dev`.slicehost.cloud_account_contact_info_current c
SET  
    c.SF_Account_Sub_Type_UWBL='Internal'
FROM `rax-abo-72-dev`.slicehost.cloud_account_contact_info_current A 
where 
    A.Internal_Flag=1
AND ifnull(lower(A.SF_Account_Sub_Type), 'unknown')<>'internal';
----------------------------------------------------------------------------------------------------
UPDATE `rax-abo-72-dev`.slicehost.cloud_account_contact_info_current c
SET  
    c.SF_Account_Sub_Type_UWBL='Internal'
FROM 
    `rax-abo-72-dev`.slicehost.cloud_account_contact_info_current A 
where 
    A.Internal_Flag=1
AND ifnull(lower(A.SF_Account_Sub_Type), 'unknown')<>'internal';
----------------------------------------------------------------------------------------------------
UPDATE `rax-abo-72-dev`.slicehost.cloud_account_contact_info_current C
SET
	C.Is_Managed_Flag=0
FROM
	`rax-abo-72-dev`.slicehost.cloud_account_contact_info_current A
WHERE
	lower(A.Account_SLA_Name_BRM) <> 'managed';


end;