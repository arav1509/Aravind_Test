CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.report_tables.udsp_etl_dim_contact_info_current()
begin

create or replace temp table Core_Contact as
SELECT  DISTINCT --INTO    #Core_Contact
    A.ID							    AS CONT_ContactID,
    ifnull(AccountNumber, 'N/A')		    AS Core_Account_number,
    ifnull(CAST(F.ID as string), 'N/A')    AS Core_Account_ID,
    FirstName,
    LastName
FROM
     `rax-landing-qa`.core_ods.cont_contact A  
LEFT JOIN
    `rax-landing-qa`.core_ods.cont_person  E 
on A.CONT_PersonID=E.ID
LEFT JOIN
    `rax-landing-qa`.core_ods.acct_xref_account_contact_accountrole  B 
ON A.ID =B.CONT_ContactID
LEFT OUTER JOIN
   `rax-landing-qa`.core_ods.acct_account F 
ON B.ACCT_AccountID=F.ID;
-----------------------------------------------------------------------------------

create or replace temp table contact_raw as
SELECT
    Contact_KEY, CAST('None' AS string) AS Core_Contact_SSO_NK, Contact_NK, Contact_Account_ID, CAST('N/A' AS string)  AS Core_Account, Contact_Last_Name, Contact_First_Name, Contact_Salutation, A.Contact_Full_Name, Contact_Other_Street, Contact_Other_City, Contact_Other_State, Contact_Other_Postal_Code, Contact_Other_Country, Contact_Mailing_Street, Contact_Mailing_City, Contact_Mailing_State, Contact_Mailing_Postal_Code, Contact_Mailing_Country, Contact_Phone, Contact_FAX, Contact_Mobile_Phone, Contact_Home_Phone, Contact_Other_Phone, Contact_Assistant_Phone, Contact_Reports_To_ID, Contact_Email, Contact_Title, Contact_Department, Contact_Assistant_Name, Contact_Lead_Source, Contact_Birthdate, Contact_Description, Contact_Currency_ISO_Code, Contact_Owner_ID, Contact_Has_Opted_Out_Of_Email, Contact_Do_Not_Call, Contact_Effective_Start_Datetime, Contact_Created_By_ID, Contact_Effective_End_Datetime, Contact_Last_Modified_By, Contact_System_Mod_Stamp, Contact_Last_Activity_Date, Contact_Last_CU_Request_Date, Contact_Last_CU_Update_Date, Contact_Do_Not_Mail, Contact_Inactive, Contact_CORE_Contact_ID, Contact_Contact_Title, Contact_Disc_Profile, Contact_Language_Preference, Contact_Hobbies, Contact_Secret_Question, Contact_Secret_Answer, Contact_Email5, Contact_Email2, Contact_Email3, Contact_Email4, Contact_Phone2, Contact_Phone3, Contact_Phone4, Contact_Phone5, Contact_Record_Created_Datetime, Contact_Record_Created_By, Contact_Record_Updated_Datetime, Contact_Record_Updated_By, Contact_Source_Name, 0 AS Contact_Current_Record, Contact_Deleted_Date_Key, Contact_SSO, Contact_Type, Contact_Supervisor_Email, Contact_Phone_Extension, Contact_Employee_Type, Contact_Work_Shift, Contact_Location, Contact_Supervisor, Contact_Survey_Black_List, Contact_Gender, Contact_Ethnicity, Contact_Exponenthrid, Contact_Created_Date, Contact_PreferredName, Contact_Company, Contact_BusinessCategory, Contact_Support_Team
--INTO    #contact_raw
FROM
    `rax-datamart-dev`.corporate_dmart.dim_contact A 
WHERE
    lower(Contact_Type) in ('internal', 'unknown')
and lower(Contact_Source_Name) in ('core','edirectory')
AND cast(Contact_Record_Created_Datetime as datetime) > cast('1900-01-01 00:00:00.000' as datetime);
-----------------------------------------------------------------------------------

create or replace temp table distinct_contacts as
SELECT DISTINCT --INTO    #distinct_contacts
    Contact_Full_Name,
    Contact_Email,
    Contact_CORE_Contact_ID
FROM contact_raw A
WHERE
    lower(A.Contact_Email) not in ('n/a','unknown','not applicable') 
AND Contact_CORE_Contact_ID <> '0';
----------------------------------------------------------------------
UPDATE contact_raw c
SET
    c.Contact_Email=B.Contact_Email
FROM contact_raw A
INNER JOIN distinct_contacts B
ON A.Contact_CORE_Contact_ID=B.Contact_CORE_Contact_ID
WHERE
     lower(A.Contact_Email) in ('n/a','unknown','not applicable') ;
----------------------------------------------------------------------
UPDATE contact_raw c
SET
    c.Contact_CORE_Contact_ID=B.Contact_CORE_Contact_ID
FROM contact_raw A
INNER JOIN distinct_contacts B
ON A.Contact_Email=B.Contact_Email
WHERE
    A.Contact_CORE_Contact_ID='0';
----------------------------------------------------------------------
UPDATE contact_raw c
SET
    c.Contact_CORE_Contact_ID=B.Contact_CORE_Contact_ID,
    c.Contact_Email=B.Contact_Email
FROM
    contact_raw A
INNER JOIN
    distinct_contacts B
ON A.Contact_Full_Name=B.Contact_Full_Name
WHERE
   (A.Contact_CORE_Contact_ID ='0' and  lower(A.Contact_Email) IN ('N/A','Unknown','Not Applicable'));
----------------------------------------------------------------------
create or replace temp table update_contact_by_Current_Email as
SELECT--INTO   #update_contact_by_Current_Email
    MAX_Contact_Key,
    A.Contact_Full_Name,
    A.Contact_Email,
    Contact_CORE_Contact_ID,
    Contact_SSO
FROM contact_raw A
INNER JOIN
(
SELECT DISTINCT 
    MAX(Contact_KEY) As MAX_Contact_Key,
    Contact_Email
FROM contact_raw 
WHERE
    lower(Contact_Email) not in ('n/a','unknown','not applicable') 
GROUP BY
    Contact_Email
)B
ON A.Contact_Email=B.Contact_Email
AND A.Contact_Key=B.MAX_Contact_Key;
----------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------
UPDATE contact_raw c
SET
    c.Contact_SSO=B.Contact_SSO
FROM
    contact_raw A
INNER JOIN
    update_contact_by_Current_Email B
ON A.Contact_Email=B.Contact_Email
WHERE
     A.Contact_SSO<>B.Contact_SSO;
     
create or replace temp table contact_SSO as
SELECT--INTO   #contact_SSO 
    A.Contact_Full_Name,
    Contact_SSO
FROM contact_raw A
INNER JOIN
(
SELECT DISTINCT 
    MAX(Contact_KEY) As MAX_Contact_Key,
    Contact_Full_Name
FROM contact_raw 
WHERE
    lower(Contact_SSO) not in ('n/a','unknown','not applicable') --and contact_email not in ('n/a','unknown'))
GROUP BY
    Contact_Full_Name
)B
ON A.Contact_Full_Name=B.Contact_Full_Name
AND A.Contact_Key=B.MAX_Contact_Key;
----------------------------------------------------------------------

UPDATE contact_raw c
SET
    c.Contact_SSO=B.Contact_SSO
FROM
    contact_raw A
INNER JOIN
    contact_SSO B
ON A.Contact_Full_Name=B.Contact_Full_Name
WHERE
   A.Contact_SSO <> B.Contact_SSO;
---------------------------------------------------------------------- 
UPDATE contact_raw c
SET
    c.Contact_SSO=B.Contact_SSO
FROM
    contact_raw A
INNER JOIN
(
SELECT DISTINCT 
    Contact_SSO,
    Contact_Email
FROM   
    contact_raw B
WHERE
    (lower(Contact_SSO)  not in ('n/a','unknown','not applicable') AND lower(Contact_Email) not in ('n/a','unknown','not applicable'))
) B
ON A.Contact_Email=B.Contact_Email
WHERE
 lower( A.Contact_SSO) in ('n/a','unknown','not applicable') 
 ;
----------------------------------------------------------------------------------------------
UPDATE contact_raw c
SET 
    c.Core_Contact_SSO_NK=
				    ifnull(
					    (CASE 
						  WHEN
							 ifnull(LOWER(A.Contact_SSO),'n/a') in ('n/a','unknown','not applicable')
						  THEN 
							 A.Contact_CORE_Contact_ID
						  ELSE
							 A.Contact_SSO
					   END
					   ),    
					    (CASE 
					WHEN
					    ifnull(A.Contact_CORE_Contact_ID,'0') ='0' 
					THEN 
					    A.Contact_SSO
					ELSE
					    A.Contact_CORE_Contact_ID
					END					
					   )
					   )

FROM contact_raw A
where true;  
------------------------------------------------------------------ 
UPDATE contact_raw C
SET
    C.Core_Account=B.Core_Account_number
FROM contact_raw A
INNER JOIN
    Core_Contact B
ON A.Contact_CORE_Contact_ID=CAST(B.CONT_ContactID AS STRING)
WHERE TRUE;
---------------------------------------------------------------------- 
CREATE OR REPLACE TEMP TABLE contact AS
SELECT
    Contact_KEY, Core_Contact_SSO_NK, Contact_NK, Contact_Account_ID, Core_Account, Contact_Last_Name, Contact_First_Name, Contact_Salutation, A.Contact_Full_Name, Contact_Other_Street, Contact_Other_City, Contact_Other_State, Contact_Other_Postal_Code, Contact_Other_Country, Contact_Mailing_Street, Contact_Mailing_City, Contact_Mailing_State, Contact_Mailing_Postal_Code, Contact_Mailing_Country, Contact_Phone, Contact_FAX, Contact_Mobile_Phone, Contact_Home_Phone, Contact_Other_Phone, Contact_Assistant_Phone, Contact_Reports_To_ID, Contact_Email, Contact_Title, Contact_Department, Contact_Assistant_Name, Contact_Lead_Source, Contact_Birthdate, Contact_Description, Contact_Currency_ISO_Code, Contact_Owner_ID, Contact_Has_Opted_Out_Of_Email, Contact_Do_Not_Call, Contact_Effective_Start_Datetime, Contact_Created_By_ID, Contact_Effective_End_Datetime, Contact_Last_Modified_By, Contact_System_Mod_Stamp, Contact_Last_Activity_Date, Contact_Last_CU_Request_Date, Contact_Last_CU_Update_Date, Contact_Do_Not_Mail, Contact_Inactive, Contact_CORE_Contact_ID, Contact_Contact_Title, Contact_Disc_Profile, Contact_Language_Preference, Contact_Hobbies, Contact_Secret_Question, Contact_Secret_Answer, Contact_Email5, Contact_Email2, Contact_Email3, Contact_Email4, Contact_Phone2, Contact_Phone3, Contact_Phone4, Contact_Phone5, Contact_Record_Created_Datetime, Contact_Record_Created_By, Contact_Record_Updated_Datetime, Contact_Record_Updated_By, Contact_Source_Name, Contact_Current_Record, Contact_Deleted_Date_Key, Contact_SSO, Contact_Type, Contact_Supervisor_Email, Contact_Phone_Extension, Contact_Employee_Type, Contact_Work_Shift, Contact_Location, Contact_Supervisor, Contact_Survey_Black_List, Contact_Gender, Contact_Ethnicity, Contact_Exponenthrid, Contact_Created_Date, Contact_PreferredName, Contact_Company, Contact_BusinessCategory, Contact_Support_Team
--INTO   #contact
FROM contact_raw A 
inner join
(
SELECT
    Contact_Full_Name,
    MAX(Contact_KEY) AS  MAX_Contact_KEY
FROM contact_raw
GROUP BY
    Contact_Full_Name
)B   
ON A.Contact_Full_Name=B.Contact_Full_Name
AND A.Contact_KEY=B.MAX_Contact_KEY;
------------------------------------------------------------------ 
CREATE OR REPLACE TABLE `rax-abo-72-dev`.report_tables.dim_contact_info_current AS
SELECT
     Contact_KEY, A.Core_Contact_SSO_NK, Contact_NK, Contact_Account_ID, Core_Account, Contact_Last_Name, Contact_First_Name, Contact_Salutation, Contact_Full_Name, Contact_Other_Street, Contact_Other_City, Contact_Other_State, Contact_Other_Postal_Code, Contact_Other_Country, Contact_Mailing_Street, Contact_Mailing_City, Contact_Mailing_State, Contact_Mailing_Postal_Code, Contact_Mailing_Country, Contact_Phone, Contact_FAX, Contact_Mobile_Phone, Contact_Home_Phone, Contact_Other_Phone, Contact_Assistant_Phone, Contact_Reports_To_ID, Contact_Email, Contact_Title, Contact_Department, Contact_Assistant_Name, Contact_Lead_Source, Contact_Birthdate, Contact_Description, Contact_Currency_ISO_Code, Contact_Owner_ID, Contact_Has_Opted_Out_Of_Email, Contact_Do_Not_Call, Contact_Effective_Start_Datetime, Contact_Created_By_ID, Contact_Effective_End_Datetime, Contact_Last_Modified_By, Contact_System_Mod_Stamp, Contact_Last_Activity_Date, Contact_Last_CU_Request_Date, Contact_Last_CU_Update_Date, Contact_Do_Not_Mail, Contact_Inactive, Contact_CORE_Contact_ID, Contact_Contact_Title, Contact_Disc_Profile, Contact_Language_Preference, Contact_Hobbies, Contact_Secret_Question, Contact_Secret_Answer, Contact_Email5, Contact_Email2, Contact_Email3, Contact_Email4, Contact_Phone2, Contact_Phone3, Contact_Phone4, Contact_Phone5, Contact_Record_Created_Datetime, Contact_Record_Created_By, Contact_Record_Updated_Datetime, Contact_Record_Updated_By, Contact_Source_Name, Contact_Current_Record, Contact_Deleted_Date_Key, Contact_SSO, Contact_Type, Contact_Supervisor_Email, Contact_Phone_Extension, Contact_Employee_Type, Contact_Work_Shift, Contact_Location, Contact_Supervisor, Contact_Survey_Black_List, Contact_Gender, Contact_Ethnicity, Contact_Exponenthrid, Contact_Created_Date, Contact_PreferredName, Contact_Company, Contact_BusinessCategory, Contact_Support_Team
FROM
(
SELECT 
    MAX(Contact_KEY)	   AS MAX_Contact_KEY,
    Core_Contact_SSO_NK
FROM contact
GROUP BY
    Core_Contact_SSO_NK
 ) A
INNER JOIN
 contact B
ON A.MAX_Contact_KEY=B.Contact_KEY
AND A.Core_Contact_SSO_NK=B.Core_Contact_SSO_NK;
---------------------------------------------------------------------------------------------  
CREATE OR REPLACE TEMP TABLE MAX_Contact_ID AS
SELECT --INTO    #MAX_Contact_ID
    MAX(Contact_KEY)	   AS MAX_Contact_KEY,
    A.Contact_CORE_Contact_ID
FROM
    `rax-abo-72-dev`.report_tables.dim_contact_info_current A
WHERE
    ifnull(A.Contact_CORE_Contact_ID, 'N/A') NOT IN ('N/A','0')
GROUP BY
    A.Contact_CORE_Contact_ID;
------------------------------------------------------------------------
UPDATE  `rax-abo-72-dev`.report_tables.dim_contact_info_current D
SET
    D.Contact_Current_Record=1
FROM
    `rax-abo-72-dev`.report_tables.dim_contact_info_current A
INNER JOIN
    MAX_Contact_ID B
ON A.Contact_KEY=B.MAX_Contact_KEY
AND A.Contact_CORE_Contact_ID=B.Contact_CORE_Contact_ID
where true;
------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE MAX_SSO AS
SELECT --INTO   #MAX_SSO
    MAX(Contact_KEY)	   AS MAX_Contact_KEY,
    A.Contact_SSO
FROM
   `rax-abo-72-dev`.report_tables.dim_contact_info_current A
WHERE
    ifnull(Contact_SSO,'N/A') NOt IN ('N/A','Unknown','Not Applicable')
AND ifnull(A.Contact_CORE_Contact_ID, 'N/A') IN ('N/A','0')
GROUP BY
    A.Contact_SSO;
------------------------------------------------------------------------
UPDATE  `rax-abo-72-dev`.report_tables.dim_contact_info_current D
SET
    D.Contact_Current_Record=1
FROM
    `rax-abo-72-dev`.report_tables.dim_contact_info_current A
INNER JOIN
    MAX_SSO  B
ON A.Contact_KEY=B.MAX_Contact_KEY
AND A.Contact_SSO=B.Contact_SSO
where true;
---------------------------------------------------------------------------------------------  
CREATE OR REPLACE TEMP TABLE dupes_SSO AS
SELECT--INTO    #dupes_SSO
    Contact_SSO,
    COUNT(Contact_Full_Name) AS SSO_Count
FROM `rax-abo-72-dev`.report_tables.dim_contact_info_current
Group By
    Contact_SSO
HAVING
    COUNT(Contact_Full_Name) >1;
---------------------------------------------------------------------------------------------  
DELETE FROM `rax-abo-72-dev`.report_tables.dim_contact_info_current
WHERE
    Contact_Current_Record=0
AND ifnull(Contact_CORE_Contact_ID, 'N/A') NOT IN ('N/A','0')
;
end;

