CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.report_tables.udsp_etl_revenue_materialization_revenue_tickets()
begin
/****************************************************************************
**  Last Modified by Kano Cannick		
**  6/29/2016			
**  Was missing records due to logic

02/28/2019		CHANDRA PUTTA		DATA-3941: Converted ntext to varchar to prevent ntext incompatible error for SQL Server 2016
*****************************************************************************/
-----------------------------------------------------------------------------
create or replace temp table User as
SELECT --INTO       #User
    A.ID							AS SF_User_ID,
    A.Namex							AS SF_User,
    CAST(C.NAMEX as string)	AS SF_User_Role,
    GROUPX                          AS SF_User_Group,
    REGION                          AS SF_User_Region
FROM   
    `rax-landing-qa`.salesforce_ods.quser A 
LEFT OUTER JOIN
    `rax-landing-qa`.salesforce_ods.quserrole C 
ON A.USERROLEID= C.ID
WHERE
   A.DELETE_FLAG <> 'Y';
   
create or replace temp table salesforce as
SELECT --INTO     #salesforce 
	CONCAT('0' ,'-', CAST(TICKET_NUMBER as string))	AS Oracle_Key,
	TICKET_NUMBER,
	APPROVAL_AMOUNT,
	Probability,
	cast(CLOSEDATE as date) AS CLOSEDATE,
	A.hosting_fee,
	setup_fee,
	booking,
	A.CURRENCYISOCODE,
	trunc(CAST(1 AS numeric),6)									AS CONVERSIONRATE,
	cvp_verified,
	vm_fees,
	0 device_count,
	A.Typex final_opportunity_type,
	Opportunity_id,
	0 core_account_number,
	u.SF_User														AS Opp_Owner,
	CAST('Salesforce' as string)								AS Revenue_Ticket_Source 
FROM 
    `rax-landing-qa`.salesforce_ods.qopportunity  A  
LEFT OUTER JOIN 	
    `rax-landing-qa`.salesforce_ods.qaccount AS B 
 ON A.ACCOUNTID = B.ID
LEFT OUTER JOIN 
    User U 
ON A.OWNERID = U.SF_User_ID    
WHERE 
    TICKET_NUMBER is not NULL
AND STAGENAME='Closed Won'
AND `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(CLOSEDATE) >=201501
--AND TICKET_TYPE <>'downgrade'
AND lower(cast(TICKET_TYPE as string)) <>'downgrade'
AND lower(A.Typex)='revenue ticket'
AND lower(cvp_verified)='true'
AND A.Delete_flag <> 'Y';

/*
UPDATE salesforce s
SET
	s.CONVERSIONRATE=cast(B.CONVERSIONRATE as numeric)
FROM
	salesforce A
INNER JOIN
	`rax-landing-qa`.salesforce_ods.qdatedconversionrate B 
ON	a.CLOSEDATE >= b.startdate and a.CLOSEDATE < b.nextstartdate
AND A.CURRENCYISOCODE=B.ISOCODE
where true;
*/

create or replace temp table Oracle as
SELECT--INTO   #Oracle
    CAST(concaT(Customer_Num ,'-', opportunity_num) as string)	AS Oracle_Key,
    --CAST(dbo.fnStripNonNumerics(Customer_Num) as STRING)	AS Account_Number,
	cast(Customer_Num as string) AS Account_Number,
    Component_Meaning,
    Change_Description,
    TRANSACTION,
    TRANSACTION_TYPE,
    cast(opportunity_num as string)												AS Ticket_Number,
    CAST('N/A' as STRING)									AS Opportunity_id,
    Device_Num					AS Device_Num,
    ifnull(SUM(Monthly_Increase),0)								AS Monthly_Increase,
	trunc(CAST(0 as numeric),6)									AS SF_Approval_Amount,
    trunc(CAST(0 as numeric),6)									AS SF_Hosting_fee,
    trunc(CAST(0 as numeric),6)									AS SF_Setup_fee,
    trunc(CAST(0 as numeric),6)									AS SF_VM_fee,
    ifnull(SUM(Setup_Fee),6)									AS Oracle_Setup_Fee,
    ifnull(SUM(MONTHLY_FEE),0)									AS Oracle_MONTHLY_FEE,
    Product_Name,
    Currency_Code,
    CAST('1900-01-01'	as date)									AS Close_Date,
    CAST('N/A' as string)									AS SF_Opp_Owner,
    CAST('Oracle' as string)	 							AS Revenue_Ticket_Source 
FROM
    `rax-landing-qa`.ebs_ods.raw_xxrs_sc_history_tbl O 
WHERE
    O.opportunity_num IN (SELECT DISTINCT ticket_number FROM salesforce)
OR  O.opportunity_num IN (SELECT DISTINCT Opportunity_id FROM salesforce)
GROUP BY
    Customer_Num,
    Device_Num,
    opportunity_num,
    Currency_Code,
    Product_Name,
    Component_Meaning,
    Change_Description,
    TRANSACTION,
    TRANSACTION_TYPE  ;
    
    
	
create or replace temp table VM as
SELECT DISTINCT--INTO     #VM
	Ticket_Number,
	'0' core_account_number,
	Opportunity_id,
	Closedate,
	vm_fees,
	Opp_Owner  
FROM salesforce A
WHERE 
	vm_fees <> 0;
/*
UPDATE Oracle o
SET
	o.SF_Approval_Amount=CAST(APPROVAL_AMOUNT AS NUMERIC),
    o.SF_Hosting_fee=CAST(hosting_fee AS NUMERIC),
    o.SF_Setup_fee=CAST(setup_fee AS NUMERIC) ,
    o.SF_VM_fee= CAST(vm_fees AS NUMERIC) ,
    o.Close_Date=CLOSEDATE,
    o.Opportunity_id=B.Opportunity_id,
    o.SF_Opp_Owner=B.Opp_Owner
FROM
    Oracle A
INNER JOIN 
    salesforce B 
ON  A.Ticket_Number=B.ticket_number 
--AND A.Account_Number=B.core_account_number
where true
;
---------------------------------------------------------------------------------
UPDATE Oracle o
SET
	o.SF_Approval_Amount=CAST(APPROVAL_AMOUNT AS NUMERIC),
	o.SF_Hosting_fee=CAST(hosting_fee AS NUMERIC),
	o.SF_Setup_fee=CAST(setup_fee AS NUMERIC),
	o.SF_VM_fee=CAST(vm_fees AS NUMERIC),
	o.Close_Date=CLOSEDATE,
	o.Opportunity_id=B.Opportunity_id,
	o.SF_Opp_Owner=B.Opp_Owner
FROM Oracle A
INNER JOIN 
    salesforce B 
ON  A.Ticket_Number=B.Opportunity_id 
--AND A.Account_Number=B.core_account_number
where true
;    
*/


INSERT INTO  Oracle
SELECT
    Oracle_Key,
    '0' 					AS Account_Number,
    'N/A'									AS Component_Meaning,
    'N/A'									AS Change_Description,
    'N/A'									AS TRANSACTION,
    'N/A'									AS TRANSACTION_TYPE,
    Ticket_Number							AS Ticket_Number,
    Opportunity_id							AS Opportunity_id,
    '0'										AS Device_Num,
    trunc(CAST(0 as numeric),2)				AS Monthly_Increase,
	cast(ifnull(APPROVAL_AMOUNT,0)	as numeric)			AS SF_Approval_Amount,
    cast(ifnull(hosting_fee,0)as numeric)					AS SF_Hosting_fee,
    cast(ifnull(setup_fee,0)	as numeric)						AS SF_Setup_fee,
    cast(ifnull(vm_fees,0)   as numeric)					AS SF_VM_fee,
    trunc(CAST(0 as numeric),2)				AS Oracle_Setup_Fee,
    trunc(CAST(0 as numeric),2)				AS Oracle_MONTHLY_FEE,
    CAST('N/A' as STRING)				AS Product_Name,
    CURRENCYISOCODE							AS Currency_Code,
    CLOSEDATE,
    Opp_Owner,
    Revenue_Ticket_Source 
FROM salesforce O 
WHERE
    Oracle_Key  NOT IN (SELECT Oracle_Key FROM  Oracle WHERE Oracle_Key is NOT NULL);
 --------------------------------------------------------------------------------- 
create or replace temp table Revenue_Materialization_Revenue_Tickets as
SELECT --INTO    #Revenue_Materialization_Revenue_Tickets	
	Oracle_Key,
    s.Ticket_Number,
    ifnull(Component_Meaning,'N/A')					AS Component_Meaning,
    ifnull(Change_Description,'N/A')				AS Change_Description,
    Monthly_Increase,
    (Monthly_Increase)*(30-(extract(day from Close_Date))-1) /30	AS ProRated_MonthlyIncrease,
    Product_Name,
    Device_Num										AS Device_Number,
    Currency_Code,
    Account_Number									AS Account_Number,
    ifnull(Transaction,'N/A')						AS Transaction,
    ifnull(Transaction_Type,'N/A')					AS Transaction_Type,
    ifnull(SF_Hosting_fee,0)						AS SF_Hosting_fee,
	ifnull(SF_Approval_Amount,0)					AS SF_Approval_Amount,
    ifnull(Oracle_Setup_Fee,0)						AS Setup_Fee,
    ifnull(SF_Setup_fee,0)	  						AS SF_Setup_Fee,
    ifnull(Oracle_MONTHLY_FEE,0)					AS New_Fee,
    ifnull(SF_VM_fee,0)								AS SF_VM_Fee,    
    'Non virtualization'							AS Ticket_Type, 
    `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(Close_Date)			AS Rev_Ticket_TMK,
    s.Opportunity_Id,
    SF_Opp_Owner						   AS Opp_Owner  
FROM  Oracle s;



UPDATE Revenue_Materialization_Revenue_Tickets r
SET
    r.Ticket_Type='Virtualization',
    r.Component_Meaning='Virtualization',
    r.Change_Description='Virtualization',
    r.TRANSACTION='Upgrade',
    r.TRANSACTION_TYPE='Contract'
FROM
	Revenue_Materialization_Revenue_Tickets A
JOIN
	VM B
ON A.Ticket_Number = B.Ticket_Number
AND A.Opportunity_ID = B.Opportunity_ID
AND A.Account_Number=B.core_account_number
WHERE 
	lower(A.Product_Name) = 'virtualization'
AND lower(A.Ticket_Type) <> 'virtualization'
;
-----------------------------------------------------------------------------
UPDATE  Revenue_Materialization_Revenue_Tickets r
SET
    r.Ticket_Type='Virtualization',
    r.Component_Meaning='Virtualization',
    r.Change_Description='Virtualization'
FROM  Revenue_Materialization_Revenue_Tickets A 
WHERE
    concat(A.SF_Hosting_fee,A.Setup_Fee,A.SF_Setup_Fee,A.New_Fee)='0'
AND A.SF_VM_Fee <>0
AND
(
  A.Ticket_Type<>'Virtualization'
OR LOWER(A.Component_Meaning)<>'virtualization'
OR LOWER(A.Change_Description)<>'virtualization'
);
-----------------------------------------------------------------------------
UPDATE Revenue_Materialization_Revenue_Tickets R 
SET
    R.Product_Name='Virtualization',
    R.TRANSACTION='Upgrade',
    R.TRANSACTION_TYPE='Contract'
FROM 
    Revenue_Materialization_Revenue_Tickets A 
WHERE
    concat(A.SF_Hosting_fee,A.Setup_Fee,A.SF_Setup_Fee,A.New_Fee)='0'
AND A.SF_VM_Fee <>0
AND
(
   A.Product_Name='N/A'
OR A.Transaction='N/A'
OR A.Transaction_Type='N/A'
);

CREATE OR REPLACE TABLE `rax-abo-72-dev`.report_tables.revenue_materialization_revenue_tickets AS
SELECT 
	Oracle_Key,
    Ticket_Number,
    Component_Meaning,
    Change_Description,
    Monthly_Increase,
    ProRated_MonthlyIncrease,
    Product_Name,
    Device_Number,
    Currency_Code,
    Account_Number,
    Transaction,
    Transaction_Type,
    SF_Hosting_fee,
	SF_Approval_Amount,
	SF_Setup_Fee,
	SF_VM_Fee,
    New_Fee,
	Setup_Fee,
    Ticket_Type, 
    Rev_Ticket_TMK,
    Opportunity_Id,
    Opp_Owner  
FROM Revenue_Materialization_Revenue_Tickets;

CREATE OR REPLACE TEMP TABLE oracle_Agg AS
SELECT 
    Ticket_Number,
    SUM(Monthly_Increase)						AS Monthly_Increase,
    SUM(ProRated_MonthlyIncrease)				AS ProRated_MonthlyIncrease,
	SUM(New_Fee)								AS Oracle_New_Fee,
	SUM(Setup_Fee)								AS Oracle_Setup_Fee,
	Account_Number 								AS Account_Number,
	Rev_Ticket_TMK
--INTO	#oracle_Agg
FROM `rax-abo-72-dev`.report_tables.revenue_materialization_revenue_tickets
GROUP BY
	Ticket_Number,
	Rev_Ticket_TMK,
	Account_Number;
CREATE OR REPLACE TEMP TABLE SF AS	
SELECT 	DISTINCT
	Ticket_Number,
    SF_Hosting_fee,
	SF_Approval_Amount,
	SF_Setup_Fee,
	SF_VM_Fee,
    Rev_Ticket_TMK,
    Opportunity_Id,
    Opp_Owner 
--INTO	#SF
FROM `rax-abo-72-dev`.report_tables.revenue_materialization_revenue_tickets;

CREATE OR REPLACE TABLE `rax-abo-72-dev`.report_tables.revenue_materialization_revenue_tickets_account_level AS
SELECT
	A.Ticket_Number,
	Account_Number,
	Opportunity_Id,
	Opp_Owner,
    A.Rev_Ticket_TMK,
	Monthly_Increase,
	ProRated_MonthlyIncrease,
	Oracle_New_Fee,
	Oracle_Setup_Fee,
    SF_Hosting_fee,
	SF_Approval_Amount,
	SF_Setup_Fee,
	SF_VM_Fee

FROM oracle_Agg A
LEFT OUTER JOIN
 SF B
ON A.Ticket_Number=B.Ticket_Number
AND A.Rev_Ticket_TMK=B.Rev_Ticket_TMK
ORDER By	1;

END;