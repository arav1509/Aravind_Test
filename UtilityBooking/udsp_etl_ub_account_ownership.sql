CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.sales.udsp_etl_ub_account_ownership()

BEGIN

-----------------------------------------------------------------
DELETE FROM `rax-abo-72-dev`.sales.ub_account_ownership
WHERE Date_Month_Key = `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(current_date());
-----------------------------------------------------------------

create or replace temp table  ub_account_ownership_inc as
SELECT --INTO #ub_account_ownership_inc
cast(FAS.State_Key as string) as State_Key,
FAS.Account_SF_ID,
A.Account_Number,
A.Account_Name,
A.Account_DDI as DDI,
--A.Account_Number_Datapipe,
--A.Account_Id_Datapipe,
A.Account_Customer_Id_Datapipe as DP_Customer_ID,
FAS.Reporting_Month_Key as Date_Month_Key,
A.Account_Type,
Ifnull(B.Owner_ID,U.Owner_ID) as Account_OwnerID,
U.Owner_Name as Account_OwnerName,
U.Owner_Region,
U.Owner_Group,
U.Owner_Sub_group,
U.Owner_Role,
U.Owner_Userrole_Id,
U.Owner_Is_Active,
A.Account_Status,
FAS.Account_Deleted as Delete_Flag,
U.Owner_Sub_Region,
U.Owner_Segment,
U.Owner_Sub_Segment,
U.Owner_Role_Type,
U.Owner_Team
FROM 
	(
		SELECT * --INTO #FAS
		FROM (
		SELECT
			Account_SF_Id,
			Account_Key,
			Owner_Key,
			Account_Deleted,
			Reporting_Month_Key,
			Account_State_start_Datetime_Utc,
			Account_State_end_Datetime_Utc,
			Chk_Sum_MD5 as State_Key
		FROM 
			`rax-datamart-dev`.dwh_db.fact_sf_rpt_account_state FAS
		JOIN
			`rax-staging-dev`.stage_three_dwmaint.sales_monthly_load_busines_day_start_stop SH
		ON FAS.Account_State_start_Datetime_Utc <= SH.Reporting_Load_DateTime_Utc
		AND FAS.Account_State_end_Datetime_Utc > SH.Reporting_Load_DateTime_Utc
		WHERE 
			FAS.Account_Deleted = 0
		AND upper(SH.Region) = 'US'
		AND Reporting_Month_Key=`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(DATE_TRUNC(CURRENT_DATE(), year))
		)
	) FAS--#FAS FAS 
JOIN
	(
		SELECT * --INTO #Acc
		FROM (
		SELECT
			Account_Key,
			Account_SF_ID,
			Account_Number,
			Account_Name,
			Account_DDI,
			Account_Customer_id_Datapipe,
			Account_Type,
			Account_Owner_Id,
			Account_Status,
			Current_Record
		FROM 
			`rax-datamart-dev`.dwh_db.dim_sf_account
		)
	) A --#Acc A 
ON FAS.Account_Key = A.Account_Key

JOIN
	(
		SELECT * --INTO #Own
		FROM (
		SELECT
			Owner_Key,
			Owner_id,
			Owner_Name,
			Owner_Region,
			Owner_Sub_Region,
			Owner_Segment,
			Owner_Sub_Segment,
			Owner_Group,
			Owner_Sub_group,
			Owner_Role,
			Owner_Role_Type,
			Owner_Team,
			Owner_Userrole_Id,
			Owner_Is_Active,
			Current_Record
		FROM 
			`rax-datamart-dev`.dwh_db.dim_sf_user A
		)
	) U --#Own U 
ON FAS.Owner_Key = U.Owner_Key
LEFT JOIN
	(
		SELECT  * --INTO #Bridge
		FROM (
		SELECT
			Owner_Id,
			Bridge_Owner_id
		FROM 
			`rax-datamart-dev`.dwh_db.xref_qv_owner_bridge A
		WHERE
			upper(Bridge_Owner_Id) <> 'N/A'
		--AND Owner_Id <> Bridge_Owner_Id
		)
	) B --#Bridge B 
ON U.Owner_ID = B.Bridge_Owner_ID;

--Added 2/19/2019
UPDATE ub_account_ownership_inc ub
SET
--A.Account_Number = B.Account_Number,
ub.Account_Name = B.Account_Name,
ub.DDI = B.Account_DDI,
ub.DP_Customer_ID = B.Account_Customer_Id_Datapipe

FROM
	ub_account_ownership_inc A
JOIN 
	(
		SELECT * --INTO #Acc
		FROM (
		SELECT
			Account_Key,
			Account_SF_ID,
			Account_Number,
			Account_Name,
			Account_DDI,
			Account_Customer_id_Datapipe,
			Account_Type,
			Account_Owner_Id,
			Account_Status,
			Current_Record
		FROM 
			`rax-datamart-dev`.dwh_db.dim_sf_account
		)
	) B --#Acc B
ON A.Account_SF_ID =B.Account_SF_ID
AND B.Current_Record = 1
where true;

------------------------------------------------------------
UPDATE ub_account_ownership_inc ub
SET
ub.Account_OwnerName = B.Owner_Name
FROM
	ub_account_ownership_inc A
JOIN 
	(
		SELECT * --INTO #Own
		FROM (
		SELECT
			Owner_Key,
			Owner_id,
			Owner_Name,
			Owner_Region,
			Owner_Sub_Region,
			Owner_Segment,
			Owner_Sub_Segment,
			Owner_Group,
			Owner_Sub_group,
			Owner_Role,
			Owner_Role_Type,
			Owner_Team,
			Owner_Userrole_Id,
			Owner_Is_Active,
			Current_Record
		FROM 
			`rax-datamart-dev`.dwh_db.dim_sf_user A
		)
	) B --#Own B
ON A.Account_OwnerID = B.Owner_id
AND B.Current_Record = 1
where A.Account_OwnerName <> B.Owner_Name
;
--------------------------------------------------------------
INSERT INTO `rax-abo-72-dev`.sales.ub_account_ownership
SELECT
	*
FROM
	ub_account_ownership_inc;
--------------------------------------------------------------
END;
