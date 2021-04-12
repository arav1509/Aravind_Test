CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.sales.udsp_etl_partner_accounts_all(V_Date date)
-------------------------------------------------------------------------------------------------------------------
begin


DECLARE CurrentMonthYear date;
DECLARE CurrentTime_Month int64;
DECLARE PreviousTime_Month int64;
DECLARE WorkDays int64;
DECLARE CalDays int64;

-----------------------------------------------------------------------------------------------------------------------------------------------------------
SET CurrentMonthYear=V_Date;
SET CurrentTime_Month=`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(CurrentMonthYear);
SET PreviousTime_Month = `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(DATE_ADD(CurrentMonthYear, INTERVAL -1 MONTH));
-------------------------------------------------------------------------------------------------------------------
DELETE FROM `rax-abo-72-dev`.sales.partner_accounts_all where Time_Month_Key = CurrentTime_Month;
-------------------------------------------------------------------------------------------------------------------


--Revenue Tickets
SELECT --INTO #OLD_REV_TIX
	opportunity_number as SF_Opportunity_Number,        
	opportunity_number,
	ifnull(Days_free,0) as Days_free,                   
	24 as Term,                                         
	cast(device_number as string) as Device_Number,
	Core_Online_Date as Online_Date                          
FROM
	`rax-landing-qa`.raptor_ods.work_items WK 
JOIN 
    `rax-landing-qa`.raptor_ods.revenue_tickets RTD 
	ON  wk.workable_item_id = rtd.id
	AND lower(rtd.workflow_state) = 'complete'                
JOIN
	`rax-landing-qa`.raptor_ods.devices d 
ON wk.id = d.work_item_id
and lower(wk.workable_item_type) = 'revenueticket'
WHERE
	Opportunity_Number is not null
and device_number is not null;
-------------------------------------------------------------------------------
create or replace temp table  SF_REVTIX as 
SELECT --INTO #SF_REVTIX 
	Opportunity_Id as SF_Opportunity_ID,
	Opportunity_Id,
	ifnull(A.Free_Days,0)	as Free_days,
	ifnull(A.Contract_Length,0)  as Term,
	cast(ifnull(B.Device_Number,'0') as string) as Device_Number,
	cast('2050-01-01' as date) as OnlineDate,
	A.Namex as Billing_Name
FROM
	`rax-landing-qa`.salesforce_ods.qopportunity A  
JOIN
	`rax-landing-qa`.salesforce_ods.qquote_line B  --?
ON A.ID = B.Opportunity_Quote
WHERE
	A.DELETE_FLAG<>'Y'
--AND A.ON_DEMAND_RECONCILED='false'
--AND A.Typex='Revenue Ticket'
AND lower(A.STAGENAME) in ('closed won', 'validation pending', 'contract kickback')
;
--AND B.Device_number is not null
delete from SF_REVTIX where if(SAFE_CAST(device_number AS FLOAT64) is null,'FALSE', 'TRUE') = 'FALSE' ;

----------------------------------------------------------------------------------------------
create or replace temp table Contracts as
SELECT DISTINCT--INTO #Contracts
	CASE WHEN CHO.renewal_id like 'ren_%' THEN RIGHT(CHO.renewal_id, length(CHO.renewal_id) - 4)
		 ELSE CHO.Opportunity_id END as SF_Opportunity_Number,                                      
	CHO.Opportunity_id as Opportunity_Number,
	ifnull(CHO.Days_Free,0) as Days_Free,                                                            
	Case WHEN cast(CHO.coterminous as datetime) is not null THEN DATETIME_DIFF( CAST(CHO.contract_Signed AS datetime), CAST(CHO.Coterminous AS datetime), MONTH)
	ELSE CHO.contract_length END as Term,                                                                             
	cast(D.Device_Number as string ) AS Device_Number,                                          
	CASE WHEN CHO.contract_type_id = 5 THEN CHO.contract_signed 
	ELSE D.Core_Online_Date END as Online_Date                                                                                                                                                         
FROM    
	`rax-landing-qa`.raptor_ods.work_items WK  
JOIN 
   `rax-landing-qa`.raptor_ods.contracts CHO  
ON  wk.workable_item_id = cho.id
AND lower(cho.workflow_state) = 'complete'
JOIN
	`rax-landing-qa`.raptor_ods.devices d 
ON wk.id = d.work_item_id
AND lower(wk.workable_item_type) = 'contract'

--------------
UNION ALL
--------------
--Acount Level Products
SELECT DISTINCT
	CASE WHEN lower(CHO.renewal_id) like 'ren_%' THEN RIGHT(CHO.renewal_id, length(CHO.renewal_id) - 4)
		 ELSE CHO.Opportunity_id END as SF_Opportunity_Number,                                        
		                                                                                             
	CHO.Opportunity_id as Opportunity_Number,
	ifnull(CHO.Days_Free,0) as Days_Free,                                                            
	CHO.contract_length,                                                                            
	cast(0 as string ) AS Device_Number,                                          
	CHO.contract_signed as Online_Date 
FROM 
	`rax-landing-qa`.raptor_ods.work_items WK  
JOIN 
    `rax-landing-qa`.raptor_ods.contracts CHO  
ON  wk.workable_item_id = cho.id
AND lower(cho.workflow_state) = 'complete'
JOIN
	`rax-landing-qa`.raptor_ods.products p 
ON wk.id = p.work_item_id
AND lower(wk.workable_item_type) = 'contract'
;

create or replace temp table TIX as 
SELECT --INTO #TIX
	Opportunity_ID as SF_Opportunity_Number,
	Opportunity_ID,
	0 as Days_Free,
	24 as Term,
	ifnull(cast( Device_Number as string)  ,'0') as Device_Number,		
	ifnull(ifnull(X.ONLINE_DATE, DATE_COMPLETED),A.Online_Date) as Online_Date
FROM(select * from(select Opportunity_ID, device_number, online_date, date_completed, row_number() over (partition by oracle_key, device_number order by transaction_num desc) as row from
	`rax-abo-72-dev`.report_tables.revenue_materialization_revenue_tickets RTD  
LEFT JOIN
	`rax-landing-qa`.ebs_ods.raw_xxrs_sc_history_tbl O 
ON RTD.Ticket_Number = O.Opportunity_Num
AND ifnull(cast(RTD.Device_Number as string),'0') = ifnull(cast( O.device_num as string ) ,'0')  
AND RTD.Product_Name = O.Product_Name 
WHERE
	Opportunity_ID is not null
and device_number is not null
and lower(O.Product_Name) <> 'virtualization'
and RTD.device_number <> '0')x
where row = 1)x
LEFT JOIN
	`rax-landing-qa`.ebs_ods.raw_xxrs_sc_device_product_tbl A  
ON Device_Number = A.Device_Num
--------------
UNION ALL
--------------
SELECT
	Opportunity_ID,
	Opportunity_ID,
	0,
	24,
	cast(x.Computer_number as string),
	ifnull(ONLINE_DATE, DATE_COMPLETED)
FROM
	`rax-abo-72-dev`.report_tables.revenue_materialization_revenue_tickets RTD  
JOIN
	`rax-landing-qa`.ebs_ods.raw_xxrs_sc_history_tbl O  
ON RTD.Ticket_Number = O.Opportunity_Num
AND ifnull(cast(RTD.Device_Number as string),'0') = ifnull(cast( O.device_num as string ) ,'0')   -- modified to string and added aliases_jcm08.03.16
AND RTD.Product_Name = O.Product_Name
JOIN 
	(SELECT 
		f.ReferenceNumber,
		a.Subject,
		b.computer_number
	FROM 
		`rax-landing-qa`.core_ods.revn_revenue a 
	JOIN  
		`rax-landing-qa`.core_ods.revn_xref_revenue_server b 
	ON a.REVN_RevenueID = b.REVN_RevenueID
	JOIN 
		`rax-landing-qa`.core_ods.tckt_ticket f 
	ON a.TCKT_TicketID = f.TCKT_TicketID)x
	ON O.Opportunity_Num = x.ReferenceNumber
	WHERE 
		lower(O.Product_Name) = 'virtualization'
		;
-------------------------------------------------------------------------------------------------------------------

INSERT INTO 	Contracts
SELECT
	*
FROM
	(
		SELECT --INTO #OLD_REV_TIX
			opportunity_number as SF_Opportunity_Number,        
			opportunity_number,
			ifnull(Days_free,0) as Days_free,                   
			24 as Term,                                         
			cast(device_number as string) as Device_Number,
			Core_Online_Date as Online_Date                          
		FROM
			`rax-landing-qa`.raptor_ods.work_items WK 
		JOIN 
			`rax-landing-qa`.raptor_ods.revenue_tickets RTD 
			ON  wk.workable_item_id = rtd.id
			AND lower(rtd.workflow_state) = 'complete'                
		JOIN
			`rax-landing-qa`.raptor_ods.devices d 
		ON wk.id = d.work_item_id
		and lower(wk.workable_item_type) = 'revenueticket'
		WHERE
			Opportunity_Number is not null
		and device_number is not null
	) A --#OLD_REV_TIX A 
WHERE NOT EXISTS 
(SELECT * FROM Contracts B 
WHERE A.Opportunity_Number = B.Opportunity_Number
AND A.Device_Number = B.Device_Number);
----------------------------------------------------
DELETE from SF_REVTIX
where ( Device_Number, Billing_Name) in(
select ( A.Device_Number, A.Billing_Name)
FROM SF_REVTIX A
JOIN Contracts B
ON A.Device_Number = B.Device_Number
AND A.Billing_Name = B.SF_Opportunity_Number);
----------------------------------------------------
INSERT INTO
	 Contracts 
SELECT
	SF_Opportunity_ID,
	Opportunity_ID,
	cast(Free_Days as int64) as Free_Days,
	cast(Term as int64) as Term,
	cast(Device_Number as string ) as Device_Number ,
	OnlineDate
FROM
	SF_REVTIX A 
WHERE NOT EXISTS 
(SELECT * FROM Contracts B 
WHERE A.Opportunity_Id = B.Opportunity_Number
AND A.Device_Number = B.Device_Number)
;
-------------------------------------------------------------------------------------------------------------------
INSERT INTO 
	Contracts 
SELECT
	*
FROM
	TIX A 
WHERE NOT EXISTS 
(SELECT * FROM Contracts B 
WHERE A.Opportunity_Id = B.Opportunity_Number
AND A.Device_Number = B.Device_Number);

-------------------------------------------------------------------------------------------------------------------
create or replace temp table ORACLE AS 
SELECT DISTINCT --INTO #ORACLE
	B.Opportunity_Num,
	Cast(A.Device_Num as string) as Device_Num,
	A.Contract_SNID,
	A.EC_END_DATE,
	A.ONLINE_DATE,
	B.Contract_TERM,
	ifnull(B.Free_Time,0) as Free_Time			
FROM 
	`rax-landing-qa`.ebs_ods.raw_xxrs_sc_device_product_tbl A  
LEFT JOIN
	`rax-landing-qa`.ebs_ods.xxrs_sc_contract_tbl B 
ON a.Contract_SNID = b.Contract_SNID
;

create or replace temp table Partner_Accounts_All_Temp as
SELECT   --INTO 	Partner_Accounts_All_Temp
	OPPORTUNITY_ID,
	Master_OPPORTUNITY_ID,
	ACCOUNTID,
	Account_Num,
	Core_Account_Number,
	Account_Name,
	Account_Type,
	Account_Sub_Type,
	Account_Owner,
	Account_Owner_Is_Active,
	Account_Owner_Role,
	Account_Owner_Role_Segment,
	Account_Owner_Group,
	Account_Owner_Sub_Group,
	Opportunity_Owner,
	Opportunity_Owner_Is_Active,
	Opportunity_Owner_Role,
	Opportunity_Owner_Role_Segment,
	Opportunity_Owner_Group,
	Opportunity_Owner_Sub_Group,
	q.Email_Account_Num,
	Opportunity_Type,
	q.DDI,
	cast( '0'		as STRING) AS Device_Number,		-- modified format to string_jcm08.03.16
	cast('9999-01-01' as datetime)				AS Device_End_Date,
	Opp_ISDELETED,
	Billing_Name,
	Category,
	24											AS Term,
	0											AS Free_Days,
	Q.Final_Opportunity_Type,
	Final_Opportunity_Type_Group,
	Opportunity_Sub_Type,
	Close_Date,
	`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(Close_Date)		AS Close_Date_Time_Month_Key,
	CurrentTime_Month							AS Time_Month_Key,
	StageName,
	ON_DEMAND_RECONCILED,
	Is_Linked_Account,
	Is_Consolidated_Billing,
	cast(Is_Internal_Account as int64) as Is_Internal_Account,
	Cloud_Desired_Billing_Date				AS Account_Desired_Billing_Date,
	Cloud_Last_Billing_Date					AS Account_Last_Billing_Date,
	Cloud_Account_Status						AS Account_Status,
	Cloud_Account_Create_Date					AS Account_Create_Date,
	Cloud_Account_End_Date					AS Account_End_Date,
	Cloud_Account_Tenure						AS Account_Tenure,
	Partner_Account,
	Partner_Role,
	CASE WHEN  LOWER(Commissions_Role) like 'pay co%' 
			or LOWER(Commissions_Role) like '%credit%' 
		 THEN 1 
		 WHEN LOWER(Partner_Contract_Type) in ('aus reseller agreement','us reseller agreement','emea reseller agreement','apac reseller agreement','hk reseller agreement','latam reseller agreement') 
			and LOWER(Commissions_Role) not like '%pay co%' 
		 THEN 1 ELSE 0 END						AS Pay_Commissions,
	Partner_Account_Name,
	Partner_Account_Type,
	Partner_Account_Sub_Type,
	Partner_Contract_Signed_Date,
	Partner_Account_RSA_ID,
	Partner_Account_RV_EXT_ID,
	Partner_Account_RSA_or_RV,
	Partner_Account_Owner,
	Partner_Account_Owner_Role,
	Partner_Contract_Type,
	Commissions_Role,	
	A.Oracle_Vendor_ID,
	A.Tier_Level,
	A.Company_Number								AS Points,
	Refresh_Date,
	'Cloud Partner'								AS Source,
	null						AS Partner_Type,
	A.Partner_Type AS US_Partner_Type,
	A.PartnerAssociated,
	A.Partner_Divested

FROM 
	`rax-abo-72-dev`.sales.cloud_opportunity_daily_snapshot Q  
JOIN 
	`rax-landing-qa`.salesforce_ods.qaccount A  
ON Q.Partner_account = A.Id
LEFT JOIN
	`rax-abo-72-dev`.sales.dim_final_opportunity_type F 
ON Q.Final_Opportunity_Type = F.Final_Opportunity_Type
WHERE 
	Q.Partner_Account is not null and Q.Partner_Account <> 'N/A' and Q.Partner_Account <> ' '
--and Q.Delete_flag <> 'Y'
and lower(stagename) = 'closed won'
and ( lower(Final_Opportunity_Type_Grouping) in ('cloud','mail','cloud_affiliate') 
		OR ( lower(Q.Final_Opportunity_Type) = 'dedicated/private cloud' AND q.DDI is not null)
	 )
----------------------------------------
UNION ALL
----------------------------------------
SELECT DISTINCT
	OPPORTUNITY_ID,
	Master_OPPORTUNITY_ID,
	ACCOUNTID,
	Account_Num,
	Core_Account,
	Q.Account_Name,
	Q.Account_Type,
	Q.Account_Sub_Type,
	Account_Owner,
	Account_Owner_ISACTIVE,
	Account_Owner_Role,
	Account_Owner_Role_Segment,
	Account_Owner_Group,
	Account_Owner_Sub_Group,
	Opportunity_Owner,
	Opportunity_Owner_ISACTIVE,
	Opportunity_Owner_Role,
	Opportunity_Owner_Role_Segment,
	Opportunity_Owner_Group,
	Opportunity_Owner_Sub_Group,
	q.Email_Account_Num,
	Opportunity_Type,
	'99999999' as DDI,
	ifnull(cast( C.Device_Number as STRING ) ,'0')				AS Device_Number,		-- modified to string_08.03.16jcm
	CASE WHEN 
	Partner_Contract_Type = 'VC/PE Strategic Agreement' 
	or A.Program_Type = 'VC/PE Program' 
	--or Primary_Business_Model = 'Master Agent/Agent'
	or ifnull(cast( C.Device_Number as STRING ) ,'0') = '0	'		-- modified to string_08.03.16jcm
	then cast('9999-01-01' as datetime)
		 ELSE DATE_ADD(CAST(ifnull(EC_END_DATE,DATE_ADD(CAST(ifnull(C.ONLINE_DATE,E.ONLINE_DATE) AS DATE),interval ifnull((CASE WHEN TERM is null or CAST(TERM AS INT64)<6 THEN CAST(Q.Contract_Term AS INT64) ELSE CAST(TERM AS INT64) END),24) month)) AS DATE), interval ifnull(CAST(Free_Time AS INT64),ifnull(CAST(Days_Free AS INT64),0)) day)
		 END																					AS Device_END_DATE,
	Opp_ISDELETED,
	Billing_Name,
	Category,
	ifnull(ifnull(CASE WHEN TERM <12 THEN Q.Contract_Term ELSE TERM END,Q.Contract_Term),24)		as Contract_Term,
	ifnull(Free_Time,ifnull(Days_Free,0))																	as Free_Days,
	Q.Final_Opportunity_Type,
	Final_Opportunity_Type_Group,
	Opportunity_Sub_Type,
	Close_Date,
	cast(Close_Date_Time_Month_Key as int64) as Close_Date_Time_Month_Key,
	CurrentTime_Month																			AS Time_Month_Key,
	StageName,
	ON_DEMAND_RECONCILED,
	B.Linked_Flag																				as Is_Linked_Account,
	0																							as Is_Consolidated_Billing,
	cast(B.Internal_Flag as int64)	as Is_Internal_Account,
	EXTRACT(DAY FROM B.Account_Last_Invoiced_Date)															as Account_Desired_Billing_Date,
	B.Account_Last_Invoiced_Date																as Account_Last_Billing_Date,
	B.Account_Status																			as Account_Status,
	B.Account_Created_Date																		as Account_Create_Date,
	CAST('1900-01-01'	as datetime)																as Account_End_Date,
	B.Account_Tenure																			as Account_Tenure,
	Partner_Account,
	Partner_Role,
	CASE WHEN  lower(Commissions_Role) like 'pay co%' 
			or lower(Commissions_Role) like '%credit%' 
		 THEN 1 
		 WHEN lower(Partner_Contract_Type) in ('aus reseller agreement','us reseller agreement','emea reseller agreement','apac reseller agreement','hk reseller agreement','latam reseller agreement') 
			and lower(Commissions_Role) not like '%pay co%' 
		 THEN 1 ELSE 0 END																		AS Pay_Commissions,
	Partner_Account_Name,
	Partner_Account_Type,
	Partner_Account_Sub_Type,
	Partner_Contract_Signed_Date,
	Partner_Account_RSA_ID,
	Partner_Account_RV_EXT_ID,
	Partner_Account_RSA_or_RV,
	Partner_Account_Owner,
	Partner_Account_Owner_Role,
	Partner_Contract_Type,
	Commissions_Role,
	A.Oracle_Vendor_ID,
	A.Tier_Level,
	A.Company_Number  as Points,														
	Refresh_Date,
	'Dedicated Partner' as Source,
	null						AS Partner_Type,
	A.Partner_Type AS  US_Partner_Type,
	A.PartnerAssociated,
	A.Partner_Divested
FROM
	`rax-abo-72-dev`.sales.dedicated_opportunity_daily_snapshot Q  
JOIN 
	`rax-landing-qa`.salesforce_ods.qaccount A 
ON Q.Partner_Account = A.Id 
LEFT JOIN
	`rax-abo-72-dev`.sales.dim_final_opportunity_type F 
ON Q.Final_Opportunity_Type = F.Final_Opportunity_Type
LEFT JOIN 
	`rax-abo-72-dev`.net_revenue.dedicated_contact_info B
ON Q.Account_Num = B.Account_Number
JOIN
	Contracts C 
ON Q.Opportunity_ID = C.opportunity_number
LEFT JOIN
	ORACLE E 
ON  lower(cast(C.Opportunity_Number as string)) = lower(E.Opportunity_NUM)
AND lower(cast(C.Device_Number as string)) = lower(E.Device_Num)

WHERE 
	Q.Partner_Account is not null and Q.Partner_Account <> 'N/A' and Q.Partner_Account <> ' '
--and Q.Delete_flag <> 'Y'
and lower(stagename) = 'closed won'	
and lower(Final_Opportunity_Type_Grouping) in ('hosting','mail')
--and Contract_Signed_Date is not null
----------------------------------------
UNION ALL
----------------------------------------
SELECT DISTINCT
	OPPORTUNITY_ID,
	Master_OPPORTUNITY_ID,
	ACCOUNTID,
	Account_Num,
	Core_Account,
	Q.Account_Name,
	Q.Account_Type,
	Q.Account_Sub_Type,
	Account_Owner,
	Account_Owner_ISACTIVE,
	Account_Owner_Role,
	Account_Owner_Role_Segment,
	Account_Owner_Group,
	Account_Owner_Sub_Group,
	Opportunity_Owner,
	Opportunity_Owner_ISACTIVE,
	Opportunity_Owner_Role,
	Opportunity_Owner_Role_Segment,
	Opportunity_Owner_Group,
	Opportunity_Owner_Sub_Group,
	q.Email_Account_Num,
	Opportunity_Type,
	'99999999' as DDI,
	ifnull(cast( C.Device_Number as STRING ) ,'0')				AS Device_Number,		-- modified to string_08.03.16jcm
	CASE WHEN 
	   lower(Partner_Contract_Type) = 'vc/pe strategic agreement' 
	or lower(A.Program_Type) = 'vc/pe program'   
	--or Primary_Business_Model = 'Master Agent/Agent'
	or ifnull(cast( C.Device_Number as STRING ) ,'0') = '0	'		-- modified to string_08.03.16jcm
	then cast('9999-01-01' as datetime)
		 ELSE DATE_ADD(CAST(ifnull(EC_END_DATE,DATE_ADD(CAST(ifnull(C.ONLINE_DATE,E.ONLINE_DATE) AS DATE),interval ifnull((CASE WHEN TERM is null or CAST(TERM AS INT64)<6 THEN CAST(Q.Contract_Term AS INT64) ELSE CAST(TERM AS INT64) END),24) month)) AS DATE), interval ifnull(CAST(Free_Time AS INT64),ifnull(CAST(Days_Free AS INT64),0)) day)
		 END AS Device_END_DATE,
	Opp_ISDELETED,
	Billing_Name,
	Category,
	ifnull(ifnull(CASE WHEN TERM <12 THEN Q.Contract_Term ELSE TERM END,Q.Contract_Term),24)		as Contract_Term,
	ifnull(Free_Time,ifnull(Days_Free,0))																	as Free_Days,
	Q.Final_Opportunity_Type,
	Final_Opportunity_Type_Group,
	Opportunity_Sub_Type,
	Close_Date,
	cast(Close_Date_Time_Month_Key as int64),
	CurrentTime_Month																			AS Time_Month_Key,
	StageName,
	ON_DEMAND_RECONCILED,
	B.Linked_Flag																				as Is_Linked_Account,
	0																							as Is_Consolidated_Billing,
	cast(B.Internal_Flag as int64)	as Is_Internal_Account,
	EXTRACT(DAY FROM B.Account_Last_Invoiced_Date)															as Account_Desired_Billing_Date,
	B.Account_Last_Invoiced_Date																as Account_Last_Billing_Date,
	B.Account_Status																			as Account_Status,
	B.Account_Created_Date																		as Account_Create_Date,
	CAST('1900-01-01'	as datetime)																as Account_End_Date,
	B.Account_Tenure																			as Account_Tenure,
	Partner_Account,
	Partner_Role,
	CASE WHEN  lower(Commissions_Role) like 'pay co%' 
			or lower(Commissions_Role) like '%credit%' 
		 THEN 1 
		 WHEN lower(Partner_Contract_Type) in ('aus reseller agreement','us reseller agreement','emea reseller agreement','apac reseller agreement','hk reseller agreement','latam reseller agreement') 
			and lower(Commissions_Role) not like '%pay co%' 
		 THEN 1 ELSE 0 END AS Pay_Commissions,
	Partner_Account_Name,
	Partner_Account_Type,
	Partner_Account_Sub_Type,
	Partner_Contract_Signed_Date,
	Partner_Account_RSA_ID,
	Partner_Account_RV_EXT_ID,
	Partner_Account_RSA_or_RV,
	Partner_Account_Owner,
	Partner_Account_Owner_Role,
	Partner_Contract_Type,
	Commissions_Role,
	A.Oracle_Vendor_ID,
	A.Tier_Level,
	A.Company_Number  as Points,														
	Refresh_Date,
	'Dedicated Partner' as Source,
	null						AS Partner_Type,
	A.Partner_Type AS  US_Partner_Type,
	A.PartnerAssociated,
	A.Partner_Divested
FROM
	`rax-abo-72-dev`.sales.dedicated_opportunity_daily_snapshot Q  
JOIN 
	`rax-landing-qa`.salesforce_ods.qaccount A 
ON Q.Partner_Account = A.Id 
LEFT JOIN
	`rax-abo-72-dev`.sales.dim_final_opportunity_type F 
ON Q.Final_Opportunity_Type = F.Final_Opportunity_Type
LEFT JOIN 
	`rax-abo-72-dev`.net_revenue.dedicated_contact_info B
ON Q.Account_Num = B.Account_Number
JOIN
	Contracts C 
ON Q.Billing_Name = C.SF_opportunity_number
LEFT JOIN
	ORACLE E 
ON cast(C.Opportunity_Number as string) = E.Opportunity_NUM
AND cast(C.Device_Number as string) = E.Device_Num

WHERE 
	Q.Partner_Account is not null and Q.Partner_Account <> 'N/A' and Q.Partner_Account <> ' '
--and Q.Delete_flag <> 'Y'
and lower(stagename) = 'closed won'	
and lower(Final_Opportunity_Type_Grouping) in ('hosting','mail')
--and Contract_Signed_Date is not null
----------------------------------------
UNION ALL
----------------------------------------
SELECT 
	OPPORTUNITY_ID,
	Master_OPPORTUNITY_ID,
	ACCOUNTID,
	Account_Num,
	Core_Account_Number,
	Account_Name,
	Account_Type,
	Account_Sub_Type,
	Account_Owner,
	Account_Owner_Is_Active,
	Account_Owner_Role,
	Account_Owner_Role_Segment,
	Account_Owner_Group,
	Account_Owner_Sub_Group,
	Opportunity_Owner,
	Opportunity_Owner_Is_Active,
	Opportunity_Owner_Role,
	Opportunity_Owner_Role_Segment,
	Opportunity_Owner_Group,
	Opportunity_Owner_Sub_Group,
	q.Email_Account_Num,
	Opportunity_Type,
	q.DDI,
	'0'											AS Device_Number,		-- modified to string_08.03.16jcm
	cast('9999-01-01' as datetime)				AS Device_End_Date,
	Opp_ISDELETED,
	Billing_Name,
	Category,
	24											AS Contract_Term,
	0											AS Days_Free,
	Q.Final_Opportunity_Type,
	Final_Opportunity_Type_Group,
	Opportunity_Sub_Type,
	Close_Date,
	`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(Close_Date)		AS Close_Date_Time_Month_Key,
	CurrentTime_Month							AS Time_Month_Key,
	StageName,
	ON_DEMAND_RECONCILED,
	Is_Linked_Account,
	Is_Consolidated_Billing,
	cast(Is_Internal_Account as int64) as Is_Internal_Account,
	Cloud_Desired_Billing_Date				AS Account_Desired_Billing_Date,
	Cloud_Last_Billing_Date					AS Account_Last_Billing_Date,
	Cloud_Account_Status						AS Account_Status,
	Cloud_Account_Create_Date					AS Account_Create_Date,
	Cloud_Account_End_Date					AS Account_End_Date,
	Cloud_Account_Tenure						AS Account_Tenure,
	Partner_Account,
	Partner_Role,
	CASE WHEN  lower(Commissions_Role) like 'pay co%' 
			or lower(Commissions_Role) like '%credit%' 
		 THEN 1 
		 WHEN   lower(Partner_Contract_Type) in ('aus reseller agreement','us reseller agreement','emea reseller agreement','apac reseller agreement','hk reseller agreement','latam reseller agreement') 
			and lower(Commissions_Role) not like '%pay co%' 
		 THEN 1 ELSE 0 END						AS Pay_Commissions,
	Partner_Account_Name,
	Partner_Account_Type,
	Partner_Account_Sub_Type,
	Partner_Contract_Signed_Date,
	Partner_Account_RSA_ID,
	Partner_Account_RV_EXT_ID,
	Partner_Account_RSA_or_RV,
	Partner_Account_Owner,
	Partner_Account_Owner_Role,
	Partner_Contract_Type,
	Commissions_Role,	
	A.Oracle_Vendor_ID,
	A.Tier_Level,
	A.Company_Number								AS Points,
	Refresh_Date,
	'Affiliate'									AS Source,
	null										AS Partner_Type,
	A.Partner_Type								AS US_Partner_Type,
	A.PartnerAssociated,
	A.Partner_Divested
FROM 
	`rax-abo-72-dev`.sales.cloud_opportunity_daily_snapshot Q  
JOIN 
	`rax-landing-qa`.salesforce_ods.qaccount A 
ON Q.Partner_Account = A.Id

WHERE 
	Q.Partner_Account is not null and Q.Partner_Account <> 'N/A' and Q.Partner_Account <> ' '
and	lower(Opportunity_Source) = 'affiliate'
and lower(StageName) = 'closed won'
--and Partner_Account_RSA_or_RV <> 'RSA'
--and Contract_Signed_Date is not null
----------------------------------------
UNION ALL
----------------------------------------
SELECT  
	cast(OPPORTUNITY_ID as string) as OPPORTUNITY_ID,
	null,
	cast(Account_ID as string) as Account_ID,
	null,
	cast(Core_Account as string) as Core_Account,
	cast(Account_Name as string) as Account_Name,
	cast(Account_Type as string) as Account_Type,
	cast(Account_Sub_Type as string) as Account_Sub_Type,
	cast(Account_Owner as string) as Account_Owner,
	cast(Account_Owner_ISACTIVE as string) as Account_Owner_ISACTIVE,
	cast(Account_Owner_Role as string) as Account_Owner_Role,
	null,
	cast(Account_Owner_Group as string) as Account_Owner_Group,
	cast(Account_Owner_Sub_Group as string) as Account_Owner_Sub_Group,
	cast(Opportunity_Owner as string) as Opportunity_Owner,
	cast(Opportunity_Owner_Is_Active as string) as Opportunity_Owner_Is_Active,
	cast(Opportunity_Owner_Role as string) as Opportunity_Owner_Role,
	cast(Opportunity_Owner_Role_Segment as string) as Opportunity_Owner_Role_Segment,
	cast(Opportunity_Owner_Group as string) as Opportunity_Owner_Group,
	cast(Opportunity_Owner_Sub_Group as string) Opportunity_Owner_Sub_Group,
	null,
	null,
	Cast(q.DDI as string)						AS DDI,
	'0'										AS Device_Number,		-- modified to string_08.03.16jcm
	cast('9999-01-01' as datetime)				AS Device_End_Date,
	null,
	null,
	null,
	24,
	0,
	null,
	null,
	null,
	Close_Date,
	`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(Close_date)		AS Close_Date_Time_Month_Key,
	cast(CurrentTime_Month	 as int64)						AS Time_Month_Key,
	null,
	null,
	0											AS Is_Linked_Account,
	0											AS Is_Consolidated_Billing,
	cast(0 as int64)											AS Is_Internal_Account,
	1				AS Account_Desired_Billing_Date,
	CAST('1900-01-01'	as datetime)				AS Account_Last_Billing_Date,
	null										AS Account_Status,
	null										AS Account_Create_Date,
	CAST('1900-01-01'	as datetime)				AS Account_End_Date,
	null										AS Account_Tenure,
	CAST(Partner_Account AS STRING) AS Partner_Account,
	CAST(Partner_Role AS STRING) AS Partner_Role,
	CASE WHEN  lower(Partner_Role) like 'pay co%' 
			or lower(Partner_Role) like '%credit%' 
		 THEN 1 ELSE 0 END						AS Pay_Commissions,
	CAST(Partner_Account_Name AS string ) as Partner_Account_Name,
	CAST(Partner_Account_Type  AS string ) as Partner_Account_Type,
	CAST(Partner_Account_Sub_Type   AS string ) as Partner_Account_Sub_Type,
	CAST(Partner_Contract_Signed_Date as datetime) as Partner_Contract_Signed_Date,
	CAST(Partner_Account_RSA_ID   AS string ) as Partner_Account_RSA_ID,
	CAST(Partner_Account_RV_EXT_ID AS string ) as Partner_Account_RV_EXT_ID,
	CAST(Partner_Account_RSA_or_RV AS string ) as Partner_Account_RSA_or_RV,
	CAST(Partner_Account_Owner AS string ) as Partner_Account_Owner,
	CAST(Partner_Account_Owner_Role AS string ) as Partner_Account_Owner_Role,
	CAST(Partner_Contract_Type AS string ) as Partner_Contract_Type ,
	'Affiliate'									AS Commissions_Role,
	CAST(A.Oracle_Vendor_ID as string) as Oracle_Vendor_ID,
	CAST(A.Tier_Level as string) as Tier_Level,
	CAST(A.Company_Number as string) AS Points,
	current_date(),
	'Affiliate'									AS Source,
	null										AS Partner_Type,
	CAST(A.Partner_Type	 as string) AS US_Partner_Type,
	CAST(A.PartnerAssociated as string) AS PartnerAssociated,
	CAST(A.Partner_Divested as string) AS  Partner_Divested
FROM 
	`rax-abo-72-dev`.sales.affiliate_customer_snapshot Q 
JOIN 
	`rax-landing-qa`.salesforce_ods.qaccount A 
ON Q.Partner_Account = A.Id
WHERE
	Q.Partner_Account is not null and Q.Partner_Account <> 'N/A' and Q.Partner_Account <> ' '
	--Partner_Account_RSA_or_RV <> 'RSA'
--and Contract_Signed_Date is not null
;


INSERT INTO Partner_Accounts_All_Temp
SELECT DISTINCT
	OPPORTUNITY_ID,
	Master_OPPORTUNITY_ID,
	ACCOUNTID,
	Account_Num,
	Core_Account_Number,
	Q.Account_Name,
	Q.Account_Type,
	Q.Account_Sub_Type,
	Account_Owner,
	Account_Owner_Is_Active,
	Account_Owner_Role,
	Account_Owner_Role_Segment,
	Account_Owner_Group,
	Account_Owner_Sub_Group,
	Opportunity_Owner,
	Opportunity_Owner_Is_Active,
	Opportunity_Owner_Role,
	Opportunity_Owner_Role_Segment,
	Opportunity_Owner_Group,
	Opportunity_Owner_Sub_Group,
	Q.Email_Account_Num,
	Opportunity_Type,
	'99999999' as DDI,
	ifnull(cast( C.Device_Number as string ) ,'0')				AS Device_Number,		-- modified to string_08.03.16jcm
	CASE WHEN 
	Partner_Contract_Type = 'VC/PE Strategic Agreement' 
	or A.Program_Type = 'VC/PE Program'   
	--or Primary_Business_Model = 'Master Agent/Agent'
	or ifnull(cast( C.Device_Number as string ) ,'0') = '0	'		-- modified to string_08.03.16jcm
	then cast('9999-01-01' as datetime)
		 ELSE DATE_ADD(CAST(ifnull(EC_END_DATE,DATE_ADD(CAST(ifnull(C.ONLINE_DATE,E.ONLINE_DATE) AS DATE),interval ifnull((CASE WHEN TERM is null or CAST(TERM AS INT64)<6 THEN 24 ELSE CAST(TERM AS INT64) END),24) month)) AS DATE), interval ifnull(CAST(Free_Time AS INT64),ifnull(CAST(Days_Free AS INT64),0)) day)
		 END AS Device_END_DATE,
	Opp_ISDELETED,
	Billing_Name,
	Category,
	ifnull(ifnull(CASE WHEN TERM <12 THEN 24 ELSE TERM END,24),24)		as Contract_Term,
	ifnull(Free_Time,ifnull(Days_Free,0))																	as Free_Days,
	Q.Final_Opportunity_Type,
	Final_Opportunity_Type_Group,
	Opportunity_Sub_Type,
	Close_Date,
	`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(Close_date) AS Close_Date_Time_Month_Key,
	CurrentTime_Month																			AS Time_Month_Key,
	StageName,
	ON_DEMAND_RECONCILED,
	B.Linked_Flag																				as Is_Linked_Account,
	0																							as Is_Consolidated_Billing,
	B.Internal_Flag																				as Is_Internal_Account,
	EXTRACT(DAY FROM B.Account_Last_Invoiced_Date) as Account_Desired_Billing_Date,
	B.Account_Last_Invoiced_Date																as Account_Last_Billing_Date,
	B.Account_Status																			as Account_Status,
	B.Account_Created_Date																		as Account_Create_Date,
	CAST('1900-01-01'	as datetime)																as Account_End_Date,
	B.Account_Tenure																			as Account_Tenure,
	Partner_Account,
	Partner_Role,
	CASE WHEN  lower(Commissions_Role) like 'pay co%' 
			or lower(Commissions_Role) like '%credit%' 
		 THEN 1 
		 WHEN  lower(Partner_Contract_Type) in ('aus reseller agreement','us reseller agreement','emea reseller agreement','apac reseller agreement','hk reseller agreement','latam reseller agreement') 
			and lower(Commissions_Role) not like '%pay co%' 
		 THEN 1 ELSE 0 END																		AS Pay_Commissions,
	Partner_Account_Name,
	Partner_Account_Type,
	Partner_Account_Sub_Type,
	Partner_Contract_Signed_Date,
	Partner_Account_RSA_ID,
	Partner_Account_RV_EXT_ID,
	Partner_Account_RSA_or_RV,
	Partner_Account_Owner,
	Partner_Account_Owner_Role,
	Partner_Contract_Type,
	Commissions_Role,
	A.Oracle_Vendor_ID,
	A.Tier_Level,
	A.Company_Number  as Points,														
	Refresh_Date,
	'Dedicated Partner' as Source,
	null						AS Partner_Type,
	A.Partner_Type				AS US_Partner_Type,
	A.PartnerAssociated,
	A.Partner_Divested
FROM
	`rax-abo-72-dev`.sales.cloud_opportunity_daily_snapshot Q  
JOIN 
	`rax-landing-qa`.salesforce_ods.qaccount A 
ON Q.Partner_Account = A.Id 
LEFT JOIN
	`rax-abo-72-dev`.sales.dim_final_opportunity_type F 
ON Q.Final_Opportunity_Type = F.Final_Opportunity_Type
LEFT JOIN 
	`rax-abo-72-dev`.net_revenue.dedicated_contact_info B
ON Q.Account_Num = B.Account_Number
LEFT JOIN
	Contracts C 
ON Q.Opportunity_ID = C.opportunity_number
LEFT JOIN
	ORACLE E 
ON cast(C.Opportunity_Number as string) = E.Opportunity_NUM
AND cast(C.Device_Number as string) = E.Device_Num

WHERE 
	Q.Partner_Account is not null and Partner_Account <> 'N/A' and Partner_Account <> ' '
and (Final_Opportunity_Type_Group='Mail' and Account_num is not null and Q.Email_account_num is null)
and lower(StageName) = 'closed won'
--and Partner_Account_RSA_or_RV <> 'RSA'
--and Contract_Signed_Date is not null
and Opportunity_ID not in (select distinct Opportunity_ID from Partner_Accounts_All_Temp)
;
--------------------------------------------------------------------------------------------------------------
INSERT INTO Partner_Accounts_All_Temp
SELECT DISTINCT
	OPPORTUNITY_ID,
	Master_OPPORTUNITY_ID,
	ACCOUNTID,
	Account_Num,
	Core_Account,
	Q.Account_Name,
	Q.Account_Type,
	Q.Account_Sub_Type,
	Account_Owner,
	Account_Owner_ISACTIVE,
	Account_Owner_Role,
	Account_Owner_Role_Segment,
	Account_Owner_Group,
	Account_Owner_Sub_Group,
	Opportunity_Owner,
	Opportunity_Owner_ISACTIVE,
	Opportunity_Owner_Role,
	Opportunity_Owner_Role_Segment,
	Opportunity_Owner_Group,
	Opportunity_Owner_Sub_Group,
	Q.Email_Account_Num,
	Opportunity_Type,
	'99999999' as DDI,
	ifnull(cast( C.Device_Number as string ) ,'0')				AS Device_Number,		-- modified to string_08.03.16jcm
	CASE WHEN 
	Partner_Contract_Type = 'VC/PE Strategic Agreement' 
	or A.Program_Type = 'VC/PE Program'  
	--or Primary_Business_Model = 'Master Agent/Agent'
	or ifnull(cast( C.Device_Number as string ) ,'0') = '0	'		-- modified to string_08.03.16jcm
	then cast('9999-01-01' as datetime)
		 ELSE DATE_ADD(CAST(ifnull(EC_END_DATE,DATE_ADD(CAST(ifnull(C.ONLINE_DATE,E.ONLINE_DATE) AS DATE),interval ifnull((CASE WHEN TERM is null or CAST(TERM AS INT64)<6 THEN CAST(Q.Contract_Term AS INT64) ELSE CAST(TERM AS INT64) END),24) month)) AS DATE), interval ifnull(CAST(Free_Time AS INT64),ifnull(CAST(Days_Free AS INT64),0)) day)
		 END AS Device_END_DATE,
	Opp_ISDELETED,
	Billing_Name,
	Category,
	ifnull(ifnull(CASE WHEN TERM <12 THEN Q.Contract_Term ELSE TERM END,Q.Contract_Term),24)		as Contract_Term,
	ifnull(Free_Time,ifnull(Days_Free,0))																	as Free_Days,
	Q.Final_Opportunity_Type,
	Final_Opportunity_Type_Group,
	Opportunity_Sub_Type,
	Close_Date,
	Close_Date_Time_Month_Key,
	CurrentTime_Month																			AS Time_Month_Key,
	StageName,
	ON_DEMAND_RECONCILED,
	B.Linked_Flag																				as Is_Linked_Account,
	0																							as Is_Consolidated_Billing,
	B.Internal_Flag																				as Is_Internal_Account,
	EXTRACT(DAY FROM B.Account_Last_Invoiced_Date)															as Account_Desired_Billing_Date,
	B.Account_Last_Invoiced_Date																as Account_Last_Billing_Date,
	B.Account_Status																			as Account_Status,
	B.Account_Created_Date																		as Account_Create_Date,
	CAST('1900-01-01'	as datetime)																as Account_End_Date,
	B.Account_Tenure																			as Account_Tenure,
	Partner_Account,
	Partner_Role,
	CASE WHEN ifnull(cast( C.Device_Number as string ) ,'0') = '0' 
		 THEN 1
		 WHEN  LOWER(Commissions_Role) like 'pay co%' 
			or LOWER(Commissions_Role) like '%credit%' 
		 THEN 1 
		 WHEN LOWER(Partner_Contract_Type) in ('aus reseller agreement','us reseller agreement','emea reseller agreement','apac reseller agreement','hk reseller agreement','latam reseller agreement') 
			and LOWER(Commissions_Role) not like '%pay co%' 
		 THEN 1 ELSE 0 END																		AS Pay_Commissions,
	Partner_Account_Name,
	Partner_Account_Type,
	Partner_Account_Sub_Type,
	Partner_Contract_Signed_Date,
	Partner_Account_RSA_ID,
	Partner_Account_RV_EXT_ID,
	Partner_Account_RSA_or_RV,
	Partner_Account_Owner,
	Partner_Account_Owner_Role,
	Partner_Contract_Type,
	Commissions_Role,
	A.Oracle_Vendor_ID,
	A.Tier_Level,
	A.Company_Number  as Points,														
	Refresh_Date,
	'Dedicated Partner' as Source,
	null						AS Partner_Type,
	A.Partner_Type				AS US_Partner_Type,
	A.PartnerAssociated,
	A.Partner_Divested
	
FROM
	`rax-abo-72-dev`.sales.dedicated_opportunity_daily_snapshot Q  
JOIN 
	`rax-landing-qa`.salesforce_ods.qaccount A 
ON Q.Partner_Account = A.Id 
LEFT JOIN
	`rax-abo-72-dev`.sales.dim_final_opportunity_type F 
ON Q.Final_Opportunity_Type = F.Final_Opportunity_Type
LEFT JOIN 
	`rax-abo-72-dev`.net_revenue.dedicated_contact_info B
ON Q.Account_Num = B.Account_Number
JOIN
	Contracts C 
ON Q.Billing_Name = CONCAT('ren_',cast(C.SF_Opportunity_Number as string))
LEFT JOIN
	ORACLE E 
ON cast(C.Opportunity_Number as string) = E.Opportunity_NUM
AND cast(C.Device_Number as string) = E.Device_Num
WHERE 
	Q.Partner_Account is not null and Q.Partner_Account <> 'N/A' and Q.Partner_Account <> ' '
and LOWER(Final_Opportunity_Type_Group) in ('hosting','mail')
and LOWER(StageName) = 'closed won'
--and Partner_Account_RSA_or_RV <> 'RSA'
and Opportunity_ID not in (select distinct Opportunity_ID from Partner_Accounts_All_Temp)
;
--------------------------------------------------------------------------------------------------------------
UPDATE Partner_Accounts_All_Temp A
SET
	Device_End_Date = '9999-01-01'
WHERE	TERM<6;
--------------------------------------------------------------------------------------------------------------
UPDATE Partner_Accounts_All_Temp A
SET
	Term = 24,
	Device_End_Date = '9999-01-01'
WHERE
	LOWER(Final_Opportunity_Type) = 'revenue_ticket' or LOWER(Final_Opportunity_Type) = 'revenue ticket'
	;
--------------------------------------------------------------------------------------------------------------
UPDATE Partner_Accounts_All_Temp PAT
SET
	PAT.Term = 24,
	PAT.Device_End_Date = '9999-01-01'
FROM
	Partner_Accounts_All_Temp A
JOIN
	`rax-abo-72-dev`.net_revenue.dedicated_account_invoice_detail B  
ON cast(A.Device_Number as string) = cast(ifnull(B.Server,'0'	) as string)
WHERE
	LOWER(Product_Type) = 'virtual machine'
	;
--------------------------------------------------------------------------------------------------------------
UPDATE Partner_Accounts_All_Temp PAT
SET
	PAT.Term = 24,
	PAT.Device_End_Date = '9999-01-01'
FROM
	Partner_Accounts_All_Temp A
JOIN
	`rax-abo-72-dev`.sales.partner_compensation_nonexpiring_partners B  
ON A.Partner_Account_RV_EXT_ID = B.Partner_Number
WHERE
	A.Device_End_Date >= B.Start_Date
	;
--------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE Alliance AS
SELECT --INTO #Alliance
	Namex as Partner_Account_Name,
	ISPARTNER,
	Company_Number,
	Type0 as Partner_Account_Type,
	Program_Type as Partner_Account_Sub_Type,
	Contract_Signed_Date as Partner_Contract_Signed_Date,
	Contract_type as Partner_Contract_Type,
	Partner_Type as US_Partner_Type,
	Oracle_Vendor_ID,
	Tier_Level
FROM
	`rax-landing-qa`.salesforce_ods.qaccount A 
WHERE
ISPARTNER='true'
AND upper(id) = '0016100001dr0TOAAY'
	;
--------------------------------------------------------------------------------------------------------------
UPDATE Partner_Accounts_All_Temp PAT
SET
	PAT.Partner_Account_Type = B.Partner_Account_Type,
	PAT.Partner_Account_Sub_Type = B.Partner_Account_Sub_Type,
	PAT.Partner_Contract_Type = B.Partner_Contract_Type,
	PAT.Tier_Level = B.Tier_Level,
	PAT.US_Partner_Type = B.US_Partner_Type
FROM
	Partner_Accounts_All_Temp A
JOIN
	Alliance B
on A.Oracle_Vendor_ID = B.Oracle_Vendor_ID
WHERE
	A.Oracle_Vendor_ID = '120249'
AND A.Close_Date >= A.Partner_Contract_Signed_Date;
--------------------------------------------------------------------------------------------------------------
UPDATE Partner_Accounts_All_Temp PAT
SET
PAT.Partner_Contract_Type=B.Partner_Contract_Type
FROM
	Partner_Accounts_All_Temp A
JOIN
	(SELECT DISTINCT Opportunity_ID, Partner_Account, Partner_Role, Pay_Commissions, Partner_Account_Name, Partner_Account_Type, Partner_Account_Sub_Type, Partner_Contract_Signed_Date, Partner_Account_RSA_ID, Partner_Account_RV_EXT_ID, Partner_Account_RSA_or_RV, Partner_Account_Owner, Partner_Account_Owner_Role, Partner_Contract_Type, Commissions_Role
		FROM `rax-abo-72-dev`.sales.partner_accounts_all 
		WHERE Time_Month_Key = PreviousTime_Month
		AND ( LOWER(Partner_Contract_Type) like '%strategic%' and LOWER(Partner_Contract_Type )not like 'vc/pe%')) B
ON A.Opportunity_ID = B.Opportunity_ID
AND A.Partner_Account_Name = B.Partner_Account_Name
WHERE
	( LOWER(A.Tier_Level) not like '%spa%'
and LOWER(A.Tier_Level) <> 'government')
;
-------------------------------------------------------------------------------
UPDATE Partner_Accounts_All_Temp PAT
SET

	PAT.Partner_Account_Type = B.Partner_Account_Type,
	PAT.Partner_Account_Sub_Type = B.Partner_Account_Sub_Type,
	PAT.Partner_Contract_Signed_Date = B.Partner_Contract_Signed_Date,
	PAT.Partner_Contract_Type = B.Partner_Contract_Type,
	PAT.Tier_Level = B.Tier_Level,
	--A.Partner_Type = B.Partner_Type,
	PAT.US_Partner_Type = B.US_Partner_Type
FROM
	Partner_Accounts_All_Temp A
JOIN(SELECT DISTINCT A.Opportunity_Id, 
	A.Close_Date,
	--A.Partner_Contract_Signed_Date,
	A.Partner_Account, 
	A.Partner_Role, 
	A.Pay_Commissions, 
	A.Partner_Account_Name, 
	A.Partner_Account_Type,
	A.Partner_Account_Sub_Type,
	A.Partner_Contract_Signed_Date,
	A.Partner_Account_RV_EXT_ID,
	A.Partner_Account_Owner,
	A.Partner_Account_Owner_Role,
	A.Partner_Contract_Type,
	A.Commissions_Role,
	A.Oracle_Vendor_ID,
	A.Tier_Level,
	A.Points,
	A.Partner_Type,
	A.US_Partner_Type
		FROM `rax-abo-72-dev`.sales.partner_accounts_all A
		WHERE Time_Month_Key = PreviousTime_Month) B
ON A.Opportunity_ID = B.Opportunity_ID
WHERE upper(A.Tier_Level) like '%SPA%'
AND A.Close_Date < A.Partner_Contract_Signed_Date
AND A.Device_End_Date > CurrentMonthYear
;
-------------------------------------------------------------------------------
UPDATE Partner_Accounts_All_Temp PTA
SET
	PTA.Term = 24,
	PTA.Device_End_Date = '9999-01-01'
FROM
	Partner_Accounts_All_Temp A
WHERE upper(A.Tier_Level) like '%SPA%'
OR (A.Oracle_Vendor_ID = '120249' and upper(A.Tier_Level) <>'%SPA%' and A.Device_End_Date < CurrentMonthYear)
;
-------------------------------------------------------------------------------
--DELETE FROM Partner_Accounts_All_Temp
--WHERE
--Source = 'Cloud Partner'
--and (Final_Opportunity_Type_Group='Mail' and Account_num is not null and Email_account_num is null)
-------------------------------------------------------------------------------
INSERT INTO `rax-abo-72-dev`.sales.partner_accounts_all
SELECT
*
FROM
	Partner_Accounts_All_Temp;
-------------------------------------------------------------------------------
/*
-------------------------------------------------------------------------------
-- delete dups for cloud accounts based on DDI and close date
WITH cte_cloud AS (
    SELECT 
		OPPORTUNITY_ID
		,ACCOUNTID
		,DDI
		,Close_Date_Time_Month_Key
		,Partner_Account
		--,Partner_Account_RV_EXT_ID
		,Partner_Account_Owner
		,Partner_Account_Owner_Role,
		time_month_key,
		source,
		account_name,
        ROW_NUMBER() OVER (
            PARTITION BY 
		--Master_OPPORTUNITY_ID
		ACCOUNTID
		,DDI
		,Close_Date_Time_Month_Key
		,Partner_Account
		--,Partner_Account_RV_EXT_ID
		,Partner_Account_Owner
		,Partner_Account_Owner_Role
		--,time_month_key
            ORDER BY 
		OPPORTUNITY_ID
		,ACCOUNTID
		,DDI
		,Close_Date_Time_Month_Key
		,Partner_Account
		--,Partner_Account_RV_EXT_ID
		--,Partner_Account_Owner
		,Partner_Account_Owner_Role
        ) row_num
     FROM 
         Partner_Accounts_All 
		-- where partner_account_owner = 'O.J. Garza'
		--and account_name = 'Synchronoss Technologies'
		--and time_month_key >= '202001'
		where source = 'Cloud Partner'
		and time_month_key >= '202001'
		--and account_name = 'Synchronoss Technologies'

		--where invoice_Time_Month_Key >= '202001'
		--and account_name = 'Synchronoss Technologies'
		--where OPPORTUNITY_ID in ('3993599','3972666')
		--and time_month_key = '202001'
		--where DDI= '1282177' 
		--partner_name = 'Apollo Global Management, LLC'

		--and DDI = '1161445' --(account number later)
		--and Time_Month_Key = '202001'
)
DELETE FROM cte_cloud
WHERE row_num > 1
--select * from cte where row_num = 1


-------------------------------------------------------------------------------

-- added device for dedicated, delete dups for dedicated accounts based on every account and device should have one opp ( same device with multiple ops is a dup condition)
;WITH cte_dedicated AS (
    SELECT 
		OPPORTUNITY_ID
		,device_number
		,ACCOUNTID
		,DDI
		,Close_Date_Time_Month_Key
		,time_month_key
		,Partner_Account
		--,Partner_Account_RV_EXT_ID
		,Partner_Account_Owner
		,Partner_Account_Owner_Role,
		--time_month_key,
		source,
		account_name,
        ROW_NUMBER() OVER (
            PARTITION BY 
		--Master_OPPORTUNITY_ID
		ACCOUNTID
		,device_number
		,DDI
		,Close_Date_Time_Month_Key
		,Partner_Account
		--,Partner_Account_RV_EXT_ID
		,Partner_Account_Owner
		,Partner_Account_Owner_Role
		--,time_month_key
            ORDER BY 
		OPPORTUNITY_ID
		,ACCOUNTID
		,DDI
		,Close_Date_Time_Month_Key
		,Partner_Account
		--,Partner_Account_RV_EXT_ID
		--,Partner_Account_Owner
		,Partner_Account_Owner_Role
        ) row_num
     FROM 
         Partner_Accounts_All 
		-- where partner_account_owner = 'O.J. Garza'
		--and account_name = 'Synchronoss Technologies'
		--and time_month_key >= '202001'
		where time_month_key >= '202001'
		and source = 'Dedicated Partner'
		--and  partner_account_name = 'Clover Communications Management LLC'
		--and account_name in ('Ascena Retail Group Inc.','EY EWT DC Colos')
		--and account_name = 'Synchronoss Technologies'

)
DELETE FROM cte_dedicated
WHERE row_num > 1
--select * from cte  where row_num = 1

DROP TABLE #Contracts
DROP TABLE #OLD_REV_TIX
DROP TABLE #SF_REVTIX
DROP TABLE #TIX
DROP TABLE #ORACLE
DROP TABLE Partner_Accounts_All_Temp

*/
end;
