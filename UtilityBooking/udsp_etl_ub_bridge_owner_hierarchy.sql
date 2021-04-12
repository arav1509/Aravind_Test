CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.sales.udsp_etl_ub_bridge_owner_hierarchy()
BEGIN

create or replace temp table  Hierarchy as 
SELECT --INTO #Hierarchy
	BridgeOwner.Date_Month_Key AS DateMonthSk,
	Own.owner_id AS Ownerid,
	Own.owner_name AS OwnerName,
	Own.owner_region AS OwnerRegion,
	Own.owner_Sub_Region AS OwnerSubRegion,
	Own.owner_segment AS OwnerSegment,
	Own.owner_sub_segment AS OwnerSubSegment,
	Own.owner_group AS OwnerGroup,
	Own.Owner_Sub_group AS OwnerSubGroup,
	Own.Owner_Userrole_Id AS OwnerUserroleId,
	Own.owner_role AS OwnerRole,
	Own.Owner_Role_Type AS OwnerRoleType,
	Own.Owner_Team as OwnerTeam,
	Own.owner_manager AS OwnerManager,
	Own.Owner_Manager_Id AS OwnerManagerId,
	Ancestor.owner_id AS Ancestorid,
	Ancestor.owner_name AS AncestorName,
	Ancestor.Owner_Manager_Id AS AncestorManagerId,
	Ancestor.owner_manager AS AncestorManager,
	Own.Owner_Is_Misc_User AS OwnerIsMiscUser,
	Own.Owner_Is_Active AS OwnerIsActive,
	BridgeOwner.Record_Updated_Datetime    AS BridgeOwnerUpdateDate

FROM
	(
		SELECT * --INTO #Bridge
		FROM (
		SELECT
			Owner_Key,
			Ancestor_Key,
			Date_Month_Key,
			Record_Updated_Datetime
		FROM
			`rax-datamart-dev`.dwh_db.fact_sf_bridgeownerhierarchy
		)
	)BridgeOwner --#Bridge BridgeOwner 
LEFT JOIN 
	(
		SELECT * --INTO #Own 
		FROM (
		SELECT
			Owner_key,
			ifnull(B.Owner_Id,A.Owner_id) as Owner_id,
			Owner_name,
			Owner_region,
			Owner_Sub_Region,
			Owner_Segment,
			Owner_Sub_Segment,
			Owner_group,
			Owner_Sub_group,
			Owner_Userrole_Id,
			Owner_role,
			Owner_role_Type,
			Owner_Team,
			Owner_manager,
			Owner_Manager_Id,
			Owner_Is_Misc_User,
			Owner_Is_Active,
			Current_Record
		FROM 
			`rax-datamart-dev`.dwh_db.dim_sf_user A 
		LEFT JOIN
			`rax-datamart-dev`.dwh_db.xref_qv_owner_bridge B 
		ON A.Owner_id = B.Bridge_Owner_Id
		WHERE upper(B.Bridge_Owner_Id) <> 'N/A'
		--AND B.Owner_Id<>Bridge_Owner_Id
		)
	)Own --#Own Own  
ON Own.owner_key = BridgeOwner.owner_key
LEFT JOIN 
	(
		SELECT * --INTO #Own 
		FROM (
		SELECT
			Owner_key,
			ifnull(B.Owner_Id,A.Owner_id) as Owner_id,
			Owner_name,
			Owner_region,
			Owner_Sub_Region,
			Owner_Segment,
			Owner_Sub_Segment,
			Owner_group,
			Owner_Sub_group,
			Owner_Userrole_Id,
			Owner_role,
			Owner_role_Type,
			Owner_Team,
			Owner_manager,
			Owner_Manager_Id,
			Owner_Is_Misc_User,
			Owner_Is_Active,
			Current_Record
		FROM 
			`rax-datamart-dev`.dwh_db.dim_sf_user A 
		LEFT JOIN
			`rax-datamart-dev`.dwh_db.xref_qv_owner_bridge B 
		ON A.Owner_id = B.Bridge_Owner_Id
		WHERE upper(B.Bridge_Owner_Id) <> 'N/A'
		--AND B.Owner_Id<>Bridge_Owner_Id
		)
	) Ancestor --#Own Ancestor  
ON Ancestor.owner_key = BridgeOwner.Ancestor_key
;


create or replace temp table QHierarchy as 
SELECT DISTINCT --INTO #QHierarchy
	A.Date_month_key as DateMonthSK,
	A.Owner_id as AncestorId,
	B.Owner_Name as AncestorName,
	A.Owner_manager_id as AncestorManagerId,
	C.Owner_Name as AncestorManager
FROM 
	(
		SELECT * --INTO #Quota
		FROM (
		SELECT
			Date_Month_Key,
			Owner_Key,
			Owner_id,
			Manager_Key,
			Owner_Manager_id
		FROM
			`rax-datamart-dev`.dwh_db.fact_sf_quota_history
		)
	) A -- #Quota A 
JOIN 
	(
		SELECT * --INTO #Own 
		FROM (
		SELECT
			Owner_key,
			ifnull(B.Owner_Id,A.Owner_id) as Owner_id,
			Owner_name,
			Owner_region,
			Owner_Sub_Region,
			Owner_Segment,
			Owner_Sub_Segment,
			Owner_group,
			Owner_Sub_group,
			Owner_Userrole_Id,
			Owner_role,
			Owner_role_Type,
			Owner_Team,
			Owner_manager,
			Owner_Manager_Id,
			Owner_Is_Misc_User,
			Owner_Is_Active,
			Current_Record
		FROM 
			`rax-datamart-dev`.dwh_db.dim_sf_user A 
		LEFT JOIN
			`rax-datamart-dev`.dwh_db.xref_qv_owner_bridge B 
		ON A.Owner_id = B.Bridge_Owner_Id
		WHERE upper(B.Bridge_Owner_Id) <> 'N/A'
		--AND B.Owner_Id<>Bridge_Owner_Id
		)
	) B --#Own B 
ON A.Owner_Key = B.Owner_Key
JOIN 
	(
		SELECT * --INTO #Own 
		FROM (
		SELECT
			Owner_key,
			ifnull(B.Owner_Id,A.Owner_id) as Owner_id,
			Owner_name,
			Owner_region,
			Owner_Sub_Region,
			Owner_Segment,
			Owner_Sub_Segment,
			Owner_group,
			Owner_Sub_group,
			Owner_Userrole_Id,
			Owner_role,
			Owner_role_Type,
			Owner_Team,
			Owner_manager,
			Owner_Manager_Id,
			Owner_Is_Misc_User,
			Owner_Is_Active,
			Current_Record
		FROM 
			`rax-datamart-dev`.dwh_db.dim_sf_user A 
		LEFT JOIN
			`rax-datamart-dev`.dwh_db.xref_qv_owner_bridge B 
		ON A.Owner_id = B.Bridge_Owner_Id
		WHERE upper(B.Bridge_Owner_Id) <> 'N/A'
		--AND B.Owner_Id<>Bridge_Owner_Id
		)
	) c --#Own c 
ON A.Manager_Key = C.Owner_Key
WHERE
	upper(C.Owner_Name) <> 'N/A'
	;

-- Data Duplicate Error

UPDATE  Hierarchy h
SET 
	h.AncestorManagerID = B.AncestorManagerID,
	h.AncestorManager = B.AncestorManager
FROM
	Hierarchy A 
JOIN 
	QHierarchy B
ON A.DateMonthSk = B.DateMonthSk and A.Ancestorid = B.Ancestorid
where A.AncestorManagerId <> B.AncestorManagerID
;

--------------------------------------------------------------
create or replace table `rax-abo-72-dev`.sales.ub_bridge_owner_hierarchy as 
SELECT
DateMonthSk,
Ownerid,
OwnerName,
OwnerRegion,
OwnerSubRegion,
OwnerSegment,
OwnerSubSegment,
OwnerGroup,
OwnerSubGroup,
OwnerUserroleId,
OwnerRole,
OwnerRoleType,
OwnerTeam,
OwnerManager,
OwnerManagerId,
Ancestorid,
AncestorName,
AncestorManagerId,
AncestorManager,
OwnerIsMiscUser,
OwnerIsActive,
BridgeOwnerUpdateDate
FROM
	Hierarchy;

UPDATE `rax-abo-72-dev`.sales.ub_bridge_owner_hierarchy ub
SET
ub.OwnerName = B.Owner_Name
FROM
	`rax-abo-72-dev`.sales.ub_bridge_owner_hierarchy A
JOIN 
	(
		SELECT * --INTO #Own 
		FROM (
		SELECT
			Owner_key,
			ifnull(B.Owner_Id,A.Owner_id) as Owner_id,
			Owner_name,
			Owner_region,
			Owner_Sub_Region,
			Owner_Segment,
			Owner_Sub_Segment,
			Owner_group,
			Owner_Sub_group,
			Owner_Userrole_Id,
			Owner_role,
			Owner_role_Type,
			Owner_Team,
			Owner_manager,
			Owner_Manager_Id,
			Owner_Is_Misc_User,
			Owner_Is_Active,
			Current_Record
		FROM 
			`rax-datamart-dev`.dwh_db.dim_sf_user A 
		LEFT JOIN
			`rax-datamart-dev`.dwh_db.xref_qv_owner_bridge B 
		ON A.Owner_id = B.Bridge_Owner_Id
		WHERE upper(B.Bridge_Owner_Id) <> 'N/A'
		--AND B.Owner_Id<>Bridge_Owner_Id
		)
	) B --#Own B
ON A.Ownerid = B.Owner_id
AND B.Current_Record = 1
where true;
-------------------------------------------------------------
UPDATE `rax-abo-72-dev`.sales.ub_bridge_owner_hierarch ub
SET
ub.OwnerManager = B.Owner_Name
FROM
	`rax-abo-72-dev`.sales.ub_bridge_owner_hierarchy A
JOIN 
	(
		SELECT * --INTO #Own 
		FROM (
		SELECT
			Owner_key,
			ifnull(B.Owner_Id,A.Owner_id) as Owner_id,
			Owner_name,
			Owner_region,
			Owner_Sub_Region,
			Owner_Segment,
			Owner_Sub_Segment,
			Owner_group,
			Owner_Sub_group,
			Owner_Userrole_Id,
			Owner_role,
			Owner_role_Type,
			Owner_Team,
			Owner_manager,
			Owner_Manager_Id,
			Owner_Is_Misc_User,
			Owner_Is_Active,
			Current_Record
		FROM 
			`rax-datamart-dev`.dwh_db.dim_sf_user A 
		LEFT JOIN
			`rax-datamart-dev`.dwh_db.xref_qv_owner_bridge B 
		ON A.Owner_id = B.Bridge_Owner_Id
		WHERE upper(B.Bridge_Owner_Id) <> 'N/A'
		--AND B.Owner_Id<>Bridge_Owner_Id
		)
	) B --#Own B
ON OwnerManagerId = B.Owner_id
AND B.Current_Record = 1
where true;

-------------------------------------------------------------
UPDATE `rax-abo-72-dev`.sales.ub_bridge_owner_hierarchy ub
SET
ub.AncestorName = B.Owner_Name
FROM
	`rax-abo-72-dev`.sales.ub_bridge_owner_hierarchy A
JOIN 
	(
		SELECT * --INTO #Own 
		FROM (
		SELECT
			Owner_key,
			ifnull(B.Owner_Id,A.Owner_id) as Owner_id,
			Owner_name,
			Owner_region,
			Owner_Sub_Region,
			Owner_Segment,
			Owner_Sub_Segment,
			Owner_group,
			Owner_Sub_group,
			Owner_Userrole_Id,
			Owner_role,
			Owner_role_Type,
			Owner_Team,
			Owner_manager,
			Owner_Manager_Id,
			Owner_Is_Misc_User,
			Owner_Is_Active,
			Current_Record
		FROM 
			`rax-datamart-dev`.dwh_db.dim_sf_user A 
		LEFT JOIN
			`rax-datamart-dev`.dwh_db.xref_qv_owner_bridge B 
		ON A.Owner_id = B.Bridge_Owner_Id
		WHERE upper(B.Bridge_Owner_Id) <> 'N/A'
		--AND B.Owner_Id<>Bridge_Owner_Id
		)
	) B --#Own B
ON A.Ancestorid = B.Owner_id
AND B.Current_Record = 1
where true;
-------------------------------------------------------------
UPDATE `rax-abo-72-dev`.sales.ub_bridge_owner_hierarchy ub
SET
ub.AncestorManager = B.Owner_Name
FROM
	`rax-abo-72-dev`.sales.ub_bridge_owner_hierarchy A
JOIN 
	(
		SELECT * --INTO #Own 
		FROM (
		SELECT
			Owner_key,
			ifnull(B.Owner_Id,A.Owner_id) as Owner_id,
			Owner_name,
			Owner_region,
			Owner_Sub_Region,
			Owner_Segment,
			Owner_Sub_Segment,
			Owner_group,
			Owner_Sub_group,
			Owner_Userrole_Id,
			Owner_role,
			Owner_role_Type,
			Owner_Team,
			Owner_manager,
			Owner_Manager_Id,
			Owner_Is_Misc_User,
			Owner_Is_Active,
			Current_Record
		FROM 
			`rax-datamart-dev`.dwh_db.dim_sf_user A 
		LEFT JOIN
			`rax-datamart-dev`.dwh_db.xref_qv_owner_bridge B 
		ON A.Owner_id = B.Bridge_Owner_Id
		WHERE upper(B.Bridge_Owner_Id) <> 'N/A'
		--AND B.Owner_Id<>Bridge_Owner_Id
		)
	) B --#Own B
ON A.AncestorManagerId = B.Owner_id
AND B.Current_Record = 1
where true;


end;
