CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.report_tables.udsp_etl_revenue_materialization()
begin
--Set StartTMK = (Yesterday TMK - 1 year)

DECLARE StartTMK INT64; 
set StartTMK= (	SELECT `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(MIN(cast(updated_at as datetime))) 
			FROM `rax-landing-qa`.econnect_ods.am_devices a 
		);
/*********************************************************************************************************************
* Section 1) Pull in SalesForce Information **************************************************************************
**********************************************************************************************************************/

-----------------------------------------------------------------------------
-- 1.1. Pull in Base Opportunities --
-----------------------------------------------------------------------------
create or replace temp table stages as
SELECT  DISTINCT  --INTO  #stages
     STAGENAME      AS SF_Stage_Name,
     iswon		AS SF_iswon
FROM  
    `rax-landing-qa`.salesforce_ods.qopportunity A 
WHERE  
    lower(isclosed) = 'true'
and lower(iswon) = 'true'
or  lower(stagename)='closed lost';

---------------------------------------------------------------------------------
create or replace temp table Rev_tickets as
SELECT --INTO    #Rev_tickets
    opportunity_id	    AS opportunity_number,
    TICKET_NUMBER,
    id			    AS sf_opportunity_id,
    CLOSEDATE	    AS completed_date,
    CREATEDDATE	    AS authorized_date
FROM
    `rax-landing-qa`.salesforce_ods.qopportunity RTD 
WHERE
	TICKET_NUMBER is not NULL
AND lower(stagename)='closed won'
AND `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(CLOSEDATE) >=201501
AND  cast( lower(TICKET_TYPE) as string)  <>'downgrade' 
--AND lower(FINAL_OPPORTUNITY_TYPE)='revenue ticket'
AND lower(cvp_verified)='true'
AND Delete_flag <> 'Y';
---------------------------------------------------------------------------------
create or replace temp table opp as
SELECT --INTO      #opp
    OPPORTUNITY_ID,
    NAMEX								   AS Opportunity_Name,
    STAGENAME							   AS SF_Stage_Name,
    CLOSEDATE							   AS Bookings_Date,
    `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(closedate)	   AS Bookings_TMK,
    CURRENCYISOCODE						   AS Bookings_Currency,
    Approval_AMOUNT						   AS Opp_Bookings_Local,
    category							   AS SF_Opportunity_Type,
    OWNERID,
    ACCOUNTID,
    owner_role							   AS SF_Owner_Role,
    CAST(null as string)			   AS Rev_Ticket_Number,
    CAST('1900-01-01' as datetime)	 		   AS Rev_Ticket_Authorized_Date,
    CAST('1900-01-01' as datetime)			   AS Rev_Ticket_Completion_Date,
    ON_DEMAND_RECONCILED
FROM  
    `rax-landing-qa`.salesforce_ods.qopportunity A 
LEFT JOIN
      Rev_tickets B
ON A.OPPORTUNITY_ID=B.opportunity_number 
INNER JOIN
    stages stgs
ON A.STAGENAME=stgs.SF_Stage_Name
AND A.iswon=stgs.SF_iswon
WHERE  
   ( DELETE_FLAG <> 'Y'
AND lower(isclosed) = 'true'
--AND IfNULL(final_opportunity_type,'Unknown') NOT IN ( 'cloud', 'mail', 'Managed Cloud' )
--AND split_Category <> 'Split - Secondary'
AND `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(closedate) >= StartTMK
)
OR OPPORTUNITY_ID='1309855'  ;   
-------------------------------------------------------------------------------
/*
UPDATE opp o
SET
    o.Rev_Ticket_Number=TICKET_NUMBER,
    o.Rev_Ticket_Authorized_Date=authorized_date,
    o.Rev_Ticket_Completion_Date=completed_date
FROM opp A
INNER JOIN
    Rev_tickets B
ON A.OPPORTUNITY_ID=B.opportunity_number 
where true;
*/
-------------------------------------------------------------------------------
-- 1.2 Pull Users --
--------------------
create or replace temp table User as
SELECT --INTO       #User
    A.ID						 AS SF_User_ID,
    A.Namex					 AS SF_User,
    CAST(C.NAMEX as string)	 AS SF_User_Role,
    GROUPX					 AS SF_User_Group,
    REGION					 AS SF_User_Region

FROM  
     `rax-landing-qa`.salesforce_ods.quser A 
LEFT OUTER JOIN
	`rax-landing-qa`.salesforce_ods.quserrole C 
ON A.USERROLEID= C.ID
WHERE
   A.DELETE_FLAG <> 'Y';
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- 1.3 Pull Account Info --
create or replace temp table Account as
SELECT ID,--INTO       #Account
       CORE_ACCOUNT_NUMBER AS SF_Core_Account_Number,
       OWNERID,
       Namex               AS SF_Account_Name,
       support_segment     AS SF_Support_Segment,
       Support_Team        AS SF_Support_Team
FROM   `rax-abo-72-dev`.sales.qaccount 
WHERE  DELETE_FLAG <> 'Y'
AND	   lower(TYPEX) in ('customer','former customer');
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- 1.4 Pull in Exchange Rates, and put it all together --
-------------------------------------------------------------------------------
create or replace table `rax-abo-72-dev`.report_tables.SF_Conversion_Rate as
SELECT startdate,--INTO   #SF_Conversion_Rate
       nextstartdate,
       isocode,
       ( 1 / conversionrate ) AS conversionrate

FROM   `rax-landing-qa`.salesforce_ods.qdatedconversionrate 
WHERE  delete_Flag = 'N'
UNION all
SELECT '1900-01-01',
       '3333-12-31',
       'USD',
       1;
-------------------------------------------------------------------------------
create or replace temp table SalesForce as
SELECT O.Opportunity_ID,--INTO    #SalesForce
	  O.ACCOUNTID,
       O.Rev_Ticket_Number,
       O.Rev_Ticket_Authorized_Date,
       O.Rev_Ticket_Completion_Date,
       Replace(Replace(Replace (O.Opportunity_Name, chr(13), ''), chr(10), ''), chr(9), '') AS Opportunity_Name,
       O.SF_Stage_Name,
       O.Bookings_Date,
       O.Bookings_TMK,
       trunc(Cast(Round(O.Opp_Bookings_Local * US.Conversionrate, 4) AS numeric),2)              AS Opp_Bookings_USD,
       trunc(Cast(O.Opp_Bookings_Local AS numeric),2) AS Opp_Bookings_Local,
       US.Conversionrate AS Bookings_Conversion_Rate,
       O.Bookings_Currency,
       O.SF_Opportunity_Type,
       A.SF_Core_Account_Number,
       A.SF_Account_Name,
       U.SF_User AS BDC,
       U.SF_User_Role AS BDC_Role,
       U.SF_User_Region AS BDC_Region,
       UL.SF_User AS Opp_Owner,
       UL.SF_User_Role AS Opp_Owner_Role,
       UL.SF_User_Region AS Opp_Owner_Region,
       ifnull(Account_Team_Business_Segment,A.SF_Support_Segment) AS SF_Support_Segment,
       ifnull(Account_Team_Name,A.SF_Support_Team) AS SF_Support_Team,
       Account_Team_Region AS Support_Region
 
FROM   opp O
       LEFT OUTER JOIN User UL 
                    ON O.OWNERID = UL.SF_User_ID
       LEFT OUTER JOIN Account AS A 
                    ON O.ACCOUNTID = A.ID
       LEFT OUTER JOIN User U 
                    ON A.OWNERID = U.SF_User_ID             
       LEFT JOIN `rax-abo-72-dev`.report_tables.SF_Conversion_Rate US
              ON O.Bookings_Currency = US.isocode
                 AND O.Bookings_Date > US.STARTDATE
                 AND O.Bookings_Date <= US.nextstartdate
--join to hierarchy to get support region
       LEFT JOIN `rax-abo-72-dev`.net_revenue.dedicated_contact_info  AI 
              ON A.SF_Core_Account_Number = AI.Account_Number
        
--Pulls in any deals sold by US Sales or supported by US Support
WHERE  (  upper(UL.SF_User_Region) = 'US'
       OR upper(Account_Team_Region) in ('USA','LATAM')
       );
--deletes US Sold, UK Supported Opps
DELETE FROM SalesForce
WHERE  upper(Support_Region) = 'INTL';
-----------------------------------------------------------------------------------
/*********************************************************************************************************************
* Section 2) Pull in eConnect Information ****************************************************************************
**********************************************************************************************************************/
--Pull in latest opp status
create or replace temp table GetStatus as
SELECT 	a.Contract_Hand_Off_ID,--INTO		 #GetStatus 
		c.qs_name,
		queue_state_id,
		partial_cp_billing_dataset_id,
		CASE WHEN
			S.Contract_Hand_Off_Id is not null THEN 1
			ELSE 0 END AS OM_Flag -- Added Flag to identify if ever in queue_state_id = 6   
FROM   `rax-landing-qa`.econnect_ods.queue_managers a 
	INNER JOIN (SELECT a.Contract_Hand_Off_ID,
					Max(a.edit_datetime) AS MaxDate
				FROM   `rax-landing-qa`.econnect_ods.queue_managers a 
				INNER JOIN `rax-landing-qa`.econnect_ods.queue_states C 
				ON a.queue_state_id = c.id
				--WHERE  c.QS_Name NOT IN ( 'US - Work Complete', 'UK - Work Complete', 'Intensification - Work Complete', 'Revenue Tickets: Work Complete' )
				GROUP  BY Contract_Hand_Off_ID)b
	ON a.Contract_Hand_Off_ID = b.Contract_Hand_Off_ID
		AND a.edit_datetime = b.MaxDate
	INNER JOIN `rax-landing-qa`.econnect_ods.queue_states C 
		ON a.queue_state_id = c.id
	LEFT JOIN (SELECT contract_hand_off_id FROM `rax-landing-qa`.econnect_ods.queue_managers  where queue_state_id = 6) S
		ON A.Contract_Hand_off_ID = S.Contract_Hand_off_ID
		;
-------------------------------------------------------------------------------

-- Tie in Opp Details
create or replace temp table eConnect_Contracts as 
SELECT DISTINCT --INTO        #eConnect_Contracts
    qs.qs_name						   AS EC_Queue_Name,
    queue_state_id						   AS queue_state_id,
    CASE cho.Account_Number
	WHEN '00000' THEN NULL
	ELSE cho.Account_Number
    END								   AS EC_Account_Number,
    cho.account_name					   AS EC_Account_Name,
    cho.opportunity_number				   AS EC_Opportunity_Number,
    cho.days_free						   AS Free_Days,
    cho.contract_rcv_date				   AS EC_Contract_Received_Date,
    cho.id							   AS EC_contract_hand_off_id,
    qm.partial_cp_billing_dataset_id		   AS QM_partial_cp_billing_dataset_id,
    Cic.code							   AS Device_Currency,
    cho.contract_Type					   AS Contract_Type_ID,
    COALESCE(Replace(Replace(Replace (rc.text, chr(13), ''), chr(10), ''), chr(9), ''), '')  AS EC_OM_Reason,
    COALESCE(Replace(Replace(Replace (amd.note, chr(13), ''), chr(10), ''), chr(9), ''), '') AS EC_OM_Note,
    0								   AS IS_EC_OM,
    cho.ac_forecast_date					   AS Device_Initial_Forecast_Date,
    cast(ifnull(amd.forecast,cast(cho.ac_forecast_date as string)) as datetime)  AS Device_Forecasted_Online_Date,
    d.id								   AS Device_ID,
    d.device_number,
    d.online_date						   AS Device_Online_Date,
    d.core_status						   AS Device_Status,
    d.platform						   AS Device_Type,
    DT.type_name						   AS Device_Type_eConnect,
    d.mrr								   AS EC_Gross_MRR,
    d.previous_mrr						   AS EC_Previous_MRR,
    ( d.mrr - d.previous_mrr )			   AS EC_Net_MRR,
    debooks.id							   AS debooks_id,
    OM_Flag								   AS Has_Been_OM
from   opp sf
inner join
    `rax-landing-qa`.econnect_ods.contract_hand_offs cho 
on sf.opportunity_id = cho.opportunity_number
left join
    GetStatus qm 
on cho.id = qm.contract_hand_off_id
left join 
    `rax-landing-qa`.econnect_ods.devices d 
on d.contract_hand_off_id = cho.id
left join 
    `rax-landing-qa`.econnect_ods.currency_iso_codes cic 
on cho.currency_iso_code_id = cic.id 
left join 
  `rax-landing-qa`.econnect_ods.device_types dt 
on d.device_type_id=dt.id
left join	 
   `rax-landing-qa`.econnect_ods.queue_states qs 
on qm.queue_state_id = qs.id
left join 
	`rax-landing-qa`.econnect_ods.debooks 
on d.id = debooks.device_id
left outer join
    `rax-landing-qa`.econnect_ods.am_devices amd 
on cast(d.id as string) = amd.device_id
left join
    `rax-landing-qa`.econnect_ods.reason_codes rc 
on cast(amd.reason_code_id  as int64) = rc.id;
-------------------------------------------------------------------------------
--QC pass 10/15/14 altered code to capture revenue in dual queue states
UPDATE eConnect_Contracts
SET
    IS_EC_OM=1
WHERE 
	(queue_state_id =6
	and lower(Device_Status) not in ('online/complete','computer no longer active', 'decommission', 'support maintenance', 'suspended - vm', 'under repair')
	and device_number <>0 
	and Device_Online_Date is null 
	and debooks_id is null
	and QM_partial_cp_billing_dataset_id = 0) 
or
	(Queue_state_id = 13
	and lower(Device_Status) not in ('online/complete','computer no longer active', 'decommission', 'support maintenance', 'suspended - vm', 'under repair')
	and device_number <>0 
	and Device_Online_Date is null 
	and debooks_id is null
	and Has_Been_OM = 1) ;
-------------------------------------------------------------------------------
-- 2.2 bring in debooks, and sum on device - 
-------------------------------------------------------------------------------
create or replace temp table EC_Pre_Base as 
SELECT  --INTO   #EC_Pre_Base
	   ECC.EC_Queue_Name,
	   ECC.EC_Opportunity_Number,
	   ECC.EC_contract_hand_off_id,
	   ECC.EC_Contract_Received_Date,
	   CT.t_name    AS EC_Contract_Type,
	   ECC.EC_Account_Number,
	   ECC.EC_Account_Name,
	   ECC.Device_Number,
	   ECC.Device_Type,
	   ECC.Device_Type_eConnect,
	   ECC.Device_Status,
	   ECC.Device_Online_Date,
	   ECC.Free_Days,
	   EC_OM_Reason,
	   EC_OM_Note,
	   IS_EC_OM,
	   ECC.Device_ID,
	   Device_Initial_Forecast_Date,
	   Device_Forecasted_Online_Date,
--if device is in debook table, get debook date
       CASE
         WHEN COALESCE(db.device_id, -1) = -1 THEN 0
         ELSE 1
       END            AS Debook_Flag,
       db.debook_Date AS Debook_Date,
--if device is in debook table, calculate Debooked MRR
       CASE
         WHEN COALESCE(db.device_id, -1) = -1 THEN 0
         ELSE ( ECC.EC_Gross_MRR - ECC.EC_Previous_MRR )
       END            AS EC_Debooked_MRR,
--If Device is in Debook Table, then zero out materialized MRR
       CASE
         WHEN COALESCE(db.device_id, -1) = -1 THEN ECC.EC_Gross_MRR
         ELSE 0
       END            AS EC_Gross_MRR,
--If Device is in Debook Table, then zero out materialized MRR
       CASE
         WHEN COALESCE(db.device_id, -1) = -1 THEN ECC.EC_Previous_MRR
         ELSE 0
       END            AS EC_Previous_MRR,
--If Device is in Debook Table, then zero out materialized MRR
       CASE
         WHEN COALESCE(db.device_id, -1) = -1 THEN ( ECC.EC_Gross_MRR - ECC.EC_Previous_MRR )
         ELSE 0
       END            AS EC_Net_MRR,
       ECC.Device_Currency

FROM   eConnect_Contracts ECC
       LEFT JOIN `rax-landing-qa`.econnect_ods.contract_types CT 
              ON ECC.contract_type_ID = CT.id
       LEFT JOIN `rax-landing-qa`.econnect_ods.debooks DB 
              ON ECC.EC_contract_hand_off_id = DB.contract_hand_off_id 
                 AND ECC.Device_ID = DB.device_id
       LEFT JOIN `rax-landing-qa`.econnect_ods.debook_reasons DBR 
              ON DB.debook_reason_id = DBR.ID   ;         
----------------------------------------------------
-- Sum up on device (since each part is in eConnect) 
create or replace temp table EC_Base as 
SELECT --INTO       #EC_Base
    EC_Queue_Name,
    EC_Opportunity_Number,
    EC_contract_hand_off_id,
    EC_Contract_Received_Date,
    EC_Contract_Type,
    EC_Account_Number,
    EC_Account_Name,
    Device_Number,
    Device_Type,
    Device_Type_eConnect,
    Device_Status,
    Device_Online_Date,
    Free_Days,
    EC_OM_Reason,
    EC_OM_Note,
    IS_EC_OM,
    Device_ID,
    Device_Initial_Forecast_Date,
    Device_Forecasted_Online_Date,
    Max(COALESCE(Debook_Flag, 0))                                                      AS Debook_Flag,
    Max(COALESCE(Debook_Date, null))                                                     AS Debook_Date,
    `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(max(cast(Debook_Date as datetime))) AS EC_Debook_TMK,
    Sum(EC_Debooked_MRR)                                                               AS EC_Debooked_MRR,
    Sum(COALESCE(EC_Gross_MRR, 0))                                                     AS EC_Gross_MRR,
    Sum(COALESCE(EC_Previous_MRR, 0))                                                  AS EC_Previous_MRR,
    Sum(COALESCE(EC_Net_MRR, 0))                                                       AS EC_Net_MRR,
    Device_Currency
FROM   EC_Pre_Base
GROUP  BY 
    EC_Queue_Name,
    EC_Opportunity_Number,
    EC_contract_hand_off_id,
    EC_Contract_Received_Date,
    EC_Contract_Type,
    EC_Account_Number,
    EC_Account_Name,
    Device_Number,
    Device_Type,
    Device_Type_eConnect,
    Device_Status,
    Device_Online_Date,
    Free_Days,
    EC_OM_Reason,
    EC_OM_Note,
    IS_EC_OM,
    Device_ID,
    Device_Initial_Forecast_Date,
    Device_Forecasted_Online_Date,
    Device_Currency;
----------------------------------------------------------------------------------------------------------------------
--pull all kickbacks for Opps in data so far
----------------------------------------------------------------------------------------------------------------------
create or replace temp table KickBacks as 
SELECT DISTINCT--INTO       #KickBacks
    EC_contract_hand_off_id,
    CAST(`rax-abo-72-dev`.bq_functions.udfdatepart(KB.created_at) as datetime)									AS Kickback_Reason_Created,
    CAST(`rax-abo-72-dev`.bq_functions.udfdatepart(KB.resolved_at)as datetime)									AS Kickback_Reason_Resolved,
    KBR.id																				AS Kickback_Reason_ID,
    KBR.text																			AS Kickback_Reason,
    EC_Kickback_Group																	AS Kickback_Reason_Group,
    Sales_EC_Kickback_Group																AS Kickback_Reason_Sales_Group,
    DATEtime_DIFF(kb.created_at,kb.resolved_at, hour)											AS Time_in_Kickback,
	CASE WHEN  kb.resolved_at IS NULL  AND kb.created_at is not null then 1 else 0 END	AS In_Kickback
FROM  EC_Base ec
LEFT JOIN 
    `rax-landing-qa`.econnect_ods.kickbacks KB 
ON ec.EC_contract_hand_off_id = kb.contract_hand_off_id
INNER JOIN 
    `rax-landing-qa`.econnect_ods.kickback_reasons KBR 
ON KB.kickback_reason_ID = KBR.id
INNER JOIN
    `rax-abo-72-dev`.sales.dim_econnect_kickback_reasons DKBR
ON KBR.ID=DKBR.Kickback_Reason_ID;
----------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------
-- Join Kickbacks to dataset
create or replace temp table EC_Semi_Final as 
SELECT --INTO       #EC_Semi_Final
    a.*,
    Kickback_Reason_Created,
    Kickback_Reason_Resolved,
    Kickback_Reason_ID,
    Kickback_Reason,
    Kickback_Reason_Group,
    Kickback_Reason_Sales_Group,
    Time_in_Kickback,
    In_Kickback

FROM EC_Base a
LEFT JOIN
    KickBacks b
ON a.EC_contract_hand_off_id = b.EC_contract_hand_off_id;
----------------------------------------------------------------------------------------------------------------------
-- 2.5 Clean up eConnect Data -
----------------------------------------------------------------------------------------------------------------------
create or replace temp table Econnect as
SELECT --INTO       #Econnect
    EC_Queue_Name,
    EC_Opportunity_Number,
    EC_contract_hand_off_id,
    Cast(EC_Contract_Received_Date AS DATETIME)                  AS EC_Contract_Received_Date,
    EC_Contract_Type,
    EC_Account_Number,
    EC_Account_Name,
    Device_ID,
    Device_Number,
    Device_Type,
    Device_Type_eConnect,
    Device_Status,
    Cast(Device_Online_Date AS DATETIME)                         AS Device_Online_Date,
    Free_Days,
    Debook_Flag,
    Debook_Date,
    EC_Debooked_MRR                                                                 AS Debook_Device_MRR_Local,
    EC_Gross_MRR                                                                    AS Device_Gross_MRR_Local,
    EC_Previous_MRR                                                                 AS Device_Previous_MRR_Local,
    EC_Net_MRR                                                                      AS Device_Net_MRR_Local,
    Device_Currency,
    EC_OM_Reason,
    EC_OM_Note,
    IS_EC_OM,
    Device_Initial_Forecast_Date,
    Device_Forecasted_Online_Date,
    Kickback_Reason_Created,
    Kickback_Reason_Resolved,
    Kickback_Reason_ID,
    Kickback_Reason,
    Kickback_Reason_Group,
    Kickback_Reason_Sales_Group,
    Time_in_Kickback,
    In_Kickback
FROM   
    EC_Semi_Final a	;   
--SELECT COUNT(DISTINCT(EC_OM_Device_ID))  FROM  #Econnect  
/*********************************************************************************************************************
* Section 3) Combine and Clean ***************************************************************************************
**********************************************************************************************************************/
-------------------------------------------------------------------------------
-- 3.1 Join SF to EC data -
---------------------------

create or replace  table `rax-abo-72-dev`.report_tables.Output as
SELECT --INTO       #Output
    Opportunity_ID,
    Rev_Ticket_Number,
    Rev_Ticket_Authorized_Date,
    Rev_Ticket_Completion_Date,
    Opportunity_Name,
    SF_Stage_Name,
    Bookings_Date,
    Bookings_TMK,
    Opp_Bookings_USD,
    Opp_Bookings_Local,
    Bookings_Conversion_Rate,
    Bookings_Currency,
    SF_Opportunity_Type,
    --sometimes account numbers differ (opp 1254096,1770524,1931082) use eConnect if available
    COALESCE(EC_Account_Number, SF_Core_Account_Number)					AS Account_Number,
    COALESCE(EC_Account_Name, SF_Account_Name)							AS Account_Name,
    BDC,
    BDC_Role,
    BDC_Region,
    Opp_Owner,
    Opp_Owner_Role,
    Opp_Owner_Region,
    SF_Support_Segment,
    SF_Support_Team,
    Support_Region,
    EC_Queue_Name,
    EC_Contract_Received_Date,
    EC_Contract_Type,
    Device_ID,
    Device_Number,
    Device_Type,
    Device_Type_eConnect,
    Device_Status,
    Device_Online_Date,
    Device_Initial_Forecast_Date,
    Device_Forecasted_Online_Date,
    Free_Days,
    Debook_Flag,
    Debook_Date,
    Debook_Device_MRR_Local,
    Device_Gross_MRR_Local,
    Device_Previous_MRR_Local,
    Device_Net_MRR_Local,
    Device_Currency,
    EC_OM_Reason,
    EC_OM_Note,
    IS_EC_OM,
    Kickback_Reason_Created,
    Kickback_Reason_Resolved,
    Kickback_Reason_ID,
    Kickback_Reason,
    Kickback_Reason_Group,
    Kickback_Reason_Sales_Group,
    Time_in_Kickback,
    In_Kickback
FROM  
    Econnect  a
 LEFT OUTER JOIN 
    SalesForce  b
ON a.EC_Opportunity_Number=b.Opportunity_ID 
;

create or replace table `rax-abo-72-dev`.report_tables.With_Types as
SELECT *,--INTO   #With_Types
--Set is Trackable flag based on contract_Type or available data
       CASE
         WHEN Rev_Ticket_Number IS NOT NULL THEN 'Revenue Ticket'
         WHEN Debook_Flag = 1 THEN 'Debooked'
         WHEN EC_Contract_Received_Date IS NULL THEN 'Not in eConnect'
         WHEN Device_Number = 0 THEN 'Account_Level'
         WHEN lower(EC_Contract_Type) NOT IN ( 'migration', 'new sale' ) THEN 'Non-Trackable Contract Type'
         WHEN Device_Online_Date < Bookings_Date THEN 'Non-Trackable Contract Type'
         WHEN Device_Online_Date IS NOT NULL THEN 'Online'
--when the forecasted date is available but earlier than yesterday, we missed the forecast
         WHEN Device_Forecasted_Online_Date  < cast(Date_add(current_date(), interval -1 day) as datetime) THEN 'Failed to meet Forecast'
--when forecast date is available, it's forecasted
         WHEN Device_Forecasted_Online_Date IS NOT NULL THEN 'Forecasted'
         ELSE 'Missing Forecast'
       END AS Materialization_Type,
--is trackable doesn't necessarily mean we have a trackable date, just that it should be trackable
       CASE
         WHEN EC_Contract_Received_Date IS NULL THEN 0
         WHEN Rev_Ticket_Completion_Date IS NULL THEN 0
         WHEN Device_Number = 0 THEN 0
         WHEN lower(EC_Contract_Type) not in ( 'migration', 'new sale' ) then 0
         WHEN Device_Online_Date < Bookings_Date THEN 0
         ELSE 1
       END AS Is_Trackable
FROM   `rax-abo-72-dev`.report_tables.Output;
---------------
---------------------------------------
--Set materialization date
create or replace  table `rax-abo-72-dev`.report_tables.With_Date as
SELECT *,--INTO   #With_Date
       CASE Materialization_Type
         WHEN 'Revenue Ticket' THEN cast(Rev_Ticket_Completion_Date as datetime)
         WHEN 'Debooked' THEN cast(Debook_Date as datetime)
         WHEN 'Online' THEN cast(Datetime_add(cast(Device_Online_Date as datetime), interval  Free_Days day ) as datetime)
         WHEN 'Forecasted' THEN cast(Datetime_add(cast(Device_Forecasted_Online_Date as datetime),  interval  Free_Days day) as datetime)
         ELSE date(NULL)
       END AS Materialization_date
FROM   `rax-abo-72-dev`.report_tables.With_Types;
---------------------------------------
--Increase Materialization_date one month for VM's --Added 5/30/2014
/*
UPDATE `rax-abo-72-dev`.report_tables.With_Date
SET
	Materialization_date=cast(date_add(cast(Materialization_date as date), interval 1 month) as string)--Query error: Invalid date: '2012-11-26 00:00:00'
WHERE 
	lower(Device_Type_eConnect) like '%virtual%';
---------------------------------------
--Increase Materalization Date one month for services associated with VM's --Added 4/23/15 Rachel Payne
UPDATE `rax-abo-72-dev`.report_tables.With_Date
SET
	Materialization_date=date_add(cast(Materialization_date as date), interval 1 month)	--Query error: Invalid date: '2012-11-26 00:00:00'
FROM  (
		select
			w.account_number,
			w.device_number,
			w.materialization_date,
			w.device_type_econnect,
			w.device_type
		from `rax-abo-72-dev`.report_tables.With_Date w
			inner join (
							select distinct
								account_number,
								device_number,
								device_type_econnect
							from `rax-abo-72-dev`.report_tables.With_Date
							where lower(w.Device_Type_eConnect) like '%virtual%'
							) x
			on w.Account_Number = x.Account_Number
			and w.device_number = x.device_number
		where lower(w.Device_Type_eConnect) not like '%virtual%'
		) A
		where true;
		*/
-------------------------------------------------------------
--Set Materialization Date, Conversion at Materialization etc
create or replace  table `rax-abo-72-dev`.report_tables.NeedProration as
SELECT --INTO      #NeedProration
    ifnull(Opportunity_ID, Rev_Ticket_Number) AS Opportunity_ID,
    Rev_Ticket_Number,
    Rev_Ticket_Authorized_Date,
    Rev_Ticket_Completion_Date,
    Opportunity_Name,
    SF_Stage_Name,
    Bookings_Date,
    Bookings_TMK,
    Opp_Bookings_USD,
    Opp_Bookings_Local,
    Bookings_Conversion_Rate,
    Bookings_Currency,
    SF_Opportunity_Type,
    Account_Number,
    Account_Name,
    BDC,
    BDC_Role,
    BDC_Region,
    Opp_Owner,
    Opp_Owner_Role,
    Opp_Owner_Region,
    SF_Support_Segment,
    SF_Support_Team,
    Support_Region,
    EC_Queue_Name,
    EC_Contract_Received_Date,
    EC_Contract_Type,
    CASE    WHEN device_online_date>cast(device_forecasted_online_date as datetime) THEN 'late'
      		WHEN device_online_date<cast(device_forecasted_online_date as datetime) THEN 'Early'
      		WHEN device_online_date=cast(device_forecasted_online_date as datetime) THEN 'On Time' 
    END AS Online_to_Forecast_Flag,
    CASE    WHEN device_online_date>cast(Device_Initial_Forecast_Date as datetime) THEN 'late'
      		WHEN device_online_date<cast(Device_Initial_Forecast_Date as datetime) THEN 'Early'
      		WHEN device_online_date=cast(Device_Initial_Forecast_Date as datetime) THEN 'On Time' 
    END AS Online_to_InitialForecast_Flag,
    CASE WHEN IS_EC_OM=1 AND Debook_Flag<>1 THEN 1
      		ELSE 0 END																AS OM_Flag,
    CASE WHEN Rev_Ticket_Number is not null THEN 'Rev Ticket' ELSE 'Opp' END		AS OPP_ID_Type,
    CASE WHEN lower(Device_Type) like '%virtual%' THEN 1 ELSE 0 END						AS VM_Flag,
    CASE WHEN IS_EC_OM=1   AND Device_Forecasted_Online_Date IS NOT NULL 
    AND Device_Forecasted_Online_Date >current_datetime() THEN  
     				(CASE 
					WHEN DATETIME_DIFF(EC_Contract_Received_Date,current_datetime(),DAY) BETWEEN 0 AND 30  THEN 'OM -Current Forecast'
					WHEN DATETIME_DIFF(EC_Contract_Received_Date,current_datetime(),DAY) BETWEEN 30 AND 60  THEN 'OM -Age >30 Days'
					WHEN DATETIME_DIFF(EC_Contract_Received_Date,current_datetime(),DAY) BETWEEN 60 AND 90  THEN 'OM -Age >60 Days'
					WHEN DATETIME_DIFF(EC_Contract_Received_Date,current_datetime(),DAY) >90 THEN 'OM -Age >90 Days' 
					END)
				WHEN IS_EC_OM=1   AND Device_Forecasted_Online_Date IS NOT NULL and Device_Forecasted_Online_Date <current_datetime() THEN 
					(CASE 
					WHEN DATETIME_DIFF(EC_Contract_Received_Date,current_datetime(),DAY) BETWEEN 0 AND 30  then 'OM -Age <30 Days Past Due Forecast'
					WHEN DATETIME_DIFF(EC_Contract_Received_Date,current_datetime(),DAY) BETWEEN 30 AND 60  then 'OM -Age >30 Days Past Due Forecast'
					WHEN DATETIME_DIFF(EC_Contract_Received_Date,current_datetime(),DAY) BETWEEN 60 AND 90  then 'OM -Age >60 Days Past Due Forecast'
					WHEN DATETIME_DIFF(EC_Contract_Received_Date,current_datetime(),DAY) >90 then 'OM -Age >90 Days Past Due Forecast' 
					END )
				WHEN IS_EC_OM=1   AND Device_Forecasted_Online_Date IS  NULL THEN
					(CASE 
					WHEN DATETIME_DIFF(EC_Contract_Received_Date,current_datetime(),DAY) BETWEEN 0 AND 6  then 'OM -Under 5 days'
					WHEN DATETIME_DIFF(EC_Contract_Received_Date,current_datetime(),DAY) BETWEEN 6 AND 16  then 'OM -No Forecast(6-15 Days)'
					WHEN DATETIME_DIFF(EC_Contract_Received_Date,current_datetime(),DAY) BETWEEN 16 AND 30  then 'OM -No Forecast(16-30 Days)'
					WHEN DATETIME_DIFF(EC_Contract_Received_Date,current_datetime(),DAY) >30 then 'OM -No Forecast(>30 Days)' 
					END)	
				ELSE NULL END AS OM_Bucket,
    Device_ID,
    Device_Number,
    Device_Type,
    Device_Type_eConnect,
    Device_Status,
    Device_Online_Date,
    `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(Device_Online_Date)								AS Device_Online_TMK,
    Device_Initial_Forecast_Date,
    `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(Device_Initial_Forecast_Date)					AS Device_Initial_TMK,
    DATEtime_DIFF(Device_Initial_Forecast_Date,Device_Online_Date,DAY)			Days_Online_InitialForecast,
    CAST(Device_Forecasted_Online_Date AS datetime)								AS Device_Forecasted_Online_Date,
    `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(CAST(Device_Forecasted_Online_Date AS datetime))			AS Device_Forecast_TMK,
    DATEtime_DIFF(Device_Forecasted_Online_Date,Device_Online_Date,DAY)			AS Days_Online_Forecasted,
    CASE WHEN Opp_Bookings_USD between 0 and 1000 Then '1 1-1k'
      		WHEN Opp_Bookings_USD between 1000 and 5000 Then '2 1k-5k'
      		WHEN Opp_Bookings_USD between 5000 and 10000 Then '3 5k-10k'
      		WHEN Opp_Bookings_USD between 10000 and 50000 Then '4 10k-50k'
      		WHEN Opp_Bookings_USD between 50000 and 100000 Then '5 50k-100k'
      		WHEN Opp_Bookings_USD between 100000 and 200000 Then '6 100k-200k'
      		WHEN Opp_Bookings_USD >200000 Then '7 >200k' END					AS Bookings_Bucket,
    Free_Days,
    Debook_Flag,
    Debook_Date,
    Debook_Device_MRR_Local,
    ( Debook_Device_MRR_Local * Bookings_Conversion_Rate )                                AS Debooked_MRR,
    --Gross MRR is Full Materialization (independant of migrations,upgrades)
    Device_Gross_MRR_Local,
    ( Device_Gross_MRR_Local * COALESCE(US.Conversionrate, Bookings_Conversion_Rate) )    AS Device_Gross_MRR_USD,
    --Previous MRR is the expected decrease from migrations (also previous from upgrades, but upgrades are non-trackable)
    Device_Previous_MRR_Local,
    ( Device_Previous_MRR_Local * COALESCE(US.Conversionrate, Bookings_Conversion_Rate) ) AS Device_Previous_MRR_USD,
    --Net MRR shows materialization with decrease from migrations included
    Device_Net_MRR_Local,
    ( Device_Net_MRR_Local * COALESCE(US.Conversionrate, Bookings_Conversion_Rate) )      AS Device_Net_MRR_USD,
    Device_Currency,
    EC_OM_Reason,
    EC_OM_Note,
    IS_EC_OM,
    Kickback_Reason_Created,
    Kickback_Reason_Resolved,
    Kickback_Reason_ID,
    Kickback_Reason,
    Kickback_Reason_Group,
    Kickback_Reason_Sales_Group,
    Time_in_Kickback,
    In_Kickback,
    Is_Trackable,
    Materialization_Type,
    --Check if Materialization date is valid
    CASE
		WHEN EXTRACT(YEAR FROM Materialization_date) < 1998 THEN NULL
		ELSE Materialization_date
    END AS Materialization_date,
    CASE
		WHEN EXTRACT(YEAR FROM Materialization_date) < 1998 THEN NULL
		ELSE `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(Materialization_date)
    END AS Materialization_TMK,
    CASE
		WHEN EXTRACT(YEAR FROM Materialization_date) < 1998 THEN NULL
		ELSE 
		/*trunc(Cast(date_diff( Materialization_date, (date_add(date_add(cast('1900-01-01' as date), interval (date_diff(cast('1900-01-01' as date), Materialization_date, month)+1) second ), interval -1 second )),day)
		+ 1 AS numeric),2) / trunc(Cast(extract ( day from (date_add(date_add(cast('1900-01-01' as date), interval (date_diff(cast('1900-01-01' as date), Materialization_date, month)+1) second ), interval -1 second ))) AS numeric),2)
		*/
		trunc(datetime_diff(`rax-abo-72-dev`.bq_functions.udf_lastdayofmonth(Materialization_date),Materialization_date, day)+1,2) / extract(day from `rax-abo-72-dev`.bq_functions.udf_lastdayofmonth(Materialization_date)) ----Total reaning days +1 / Total days in month
    END AS Prorate_Multiple,
    CASE
	WHEN EXTRACT(YEAR FROM Materialization_date) < 1998 THEN NULL
	ELSE COALESCE(US.Conversionrate, Bookings_Conversion_Rate)
    END AS Materialization_Conversion_Rate,
    --Get full bookings amount (independant of migration devices)
    CASE
	WHEN A.Device_Gross_MRR_Local IS NOT NULL THEN A.Device_Gross_MRR_Local
	WHEN Rev_Ticket_Number IS NOT NULL THEN A.Opp_Bookings_Local
    END                                                                                   AS Full_Materialization_Local,
    trunc(Cast(Round(( CASE
			   WHEN A.Device_Gross_MRR_Local IS NOT NULL THEN A.Device_Gross_MRR_Local * US.Conversionrate
			   WHEN Rev_Ticket_Number IS NOT NULL THEN A.Opp_Bookings_Local * Bookings_Conversion_Rate
			 END ), 4) AS numeric) ,2)                                            AS Full_Materialization_USD,
    --Get net bookings amount
    CASE
	WHEN A.Device_Net_MRR_Local IS NOT NULL THEN A.Device_Net_MRR_Local
	WHEN Rev_Ticket_Number IS NOT NULL THEN A.Opp_Bookings_Local
    END AS Materialization_Minus_Migration_Local,
    trunc(Cast(Round(( CASE
			   WHEN A.Device_Net_MRR_Local IS NOT NULL THEN A.Device_Net_MRR_Local * US.Conversionrate
			   WHEN Rev_Ticket_Number IS NOT NULL THEN A.Opp_Bookings_Local * Bookings_Conversion_Rate
			 END ), 4) AS numeric),2) AS Materialization_Minus_Migration_USD
FROM   `rax-abo-72-dev`.report_tables.With_Date A
       LEFT JOIN `rax-abo-72-dev`.report_tables.SF_Conversion_Rate US
              ON A.Device_Currency = US.isocode
                 --if there is no materialization date, use today for conversion date
                 AND COALESCE(A.Materialization_date, current_date()) > US.STARTDATE
                 AND COALESCE(A.Materialization_date, current_date()) <= US.nextstartdate
				 ;
--SELECT COUNT(DISTINCT(EC_OM_Device_ID))  FROM  #NeedProration
-------------------------------------
-- Prorate the materialization Amount
create or replace temp table NeedID as 
SELECT *,--INTO   #NeedID
       CASE mod(Materialization_TMK, 100)
         WHEN 12 THEN Materialization_TMK + 89
         ELSE Materialization_TMK + 1
       END AS Materialization_TMK2,
       CASE
         WHEN Materialization_TMK IS NULL THEN Full_Materialization_USD
         ELSE trunc(Cast(Prorate_Multiple * Full_Materialization_USD AS numeric),2)
       END AS M1_Prorated_Full,
       CASE
         WHEN Materialization_TMK IS NULL THEN Materialization_Minus_Migration_USD
         ELSE trunc(Cast(Prorate_Multiple * Materialization_Minus_Migration_USD AS numeric),2)
       END AS M1_Prorated_Minus_Migration,
       trunc(Cast(Full_Materialization_USD - ( Prorate_Multiple * Full_Materialization_USD ) AS numeric),2) AS M2_Prorated_Full,
       trunc(Cast(Materialization_Minus_Migration_USD - ( Prorate_Multiple * Materialization_Minus_Migration_USD ) AS numeric),2) AS M2_Prorated_Minus_Migration
FROM   `rax-abo-72-dev`.report_tables.NeedProration;
--SELECT COUNT(DISTINCT(EC_OM_Device_ID))  FROM  #NeedID
-------------------------------------------------------------------------------
-- 3.3 Order items and spit 'em out -
-------------------------------------
---------------------------------------
--Add in Row Number
create or replace temp table withLine as
SELECT Row_number ()
         OVER (
           ORDER BY Opportunity_ID DESC, Device_Number DESC) AS Opp_Line_Num,
       *
--INTO   #withLine
FROM   NeedID;
---------------------------------------
--Add Device Number within Opp
create or replace table `rax-abo-72-dev`.report_tables.Fin as
SELECT (SELECT Count(t2.Opp_Line_Num)
        FROM   withLine AS t2
        WHERE  t1 .Opportunity_ID = t2 .Opportunity_ID
               AND t2 .Opp_Line_Num <= t1 .Opp_Line_Num) AS Device_Line_Num,
       t1.*
--INTO   #Fin
FROM   withLine AS t1;
---------------------------------------
--Spit it out in order
--INSERT INTO Report_Tables.Revenue_Materialization
insert INTO `rax-abo-72-dev`.report_tables.revenue_materialization(device_line_num,opp_line_num,opportunity_id,rev_ticket_number,rev_ticket_authorized_date,rev_ticket_completion_date,opportunity_name,sf_stage_name,bookings_date,bookings_tmk,opp_bookings_usd,opp_bookings_local,bookings_conversion_rate,bookings_currency,sf_opportunity_type,account_number,account_name,bdc,bdc_role,bdc_region,opp_owner,opp_owner_role,opp_owner_region,sf_support_segment,sf_support_team,support_region,ec_queue_name,ec_contract_received_date,ec_contract_type,online_to_forecast_flag,online_to_initialforecast_flag,om_flag,opp_id_type,vm_flag,om_bucket,device_id,device_number,device_type,device_type_econnect,device_status,device_online_date,device_online_tmk,device_initial_forecast_date,device_initial_tmk,days_online_initialforecast,device_forecasted_online_date,device_forecast_tmk,days_online_forecasted,bookings_bucket,free_days,debook_flag,debook_date,debook_device_mrr_local,debooked_mrr,device_gross_mrr_local,device_gross_mrr_usd,device_previous_mrr_local,device_previous_mrr_usd,device_net_mrr_local,device_net_mrr_usd,device_currency,ec_om_reason,ec_om_note,is_ec_om,kickback_reason_created,kickback_reason_resolved,kickback_reason_id,kickback_reason,kickback_reason_group,kickback_reason_sales_group,time_in_kickback,in_kickback,is_trackable,materialization_type,materialization_date,materialization_tmk,prorate_multiple,materialization_conversion_rate,full_materialization_local,full_materialization_usd,materialization_minus_migration_local,materialization_minus_migration_usd,materialization_tmk2,m1_prorated_full,m1_prorated_minus_migration,m2_prorated_full,m2_prorated_minus_migration,load_date)
SELECT device_line_num,opp_line_num,opportunity_id,rev_ticket_number,rev_ticket_authorized_date,rev_ticket_completion_date,opportunity_name,sf_stage_name,bookings_date,bookings_tmk,opp_bookings_usd,opp_bookings_local,cast(bookings_conversion_rate as numeric) as bookings_conversion_ratebookings_conversion_rate ,bookings_currency,sf_opportunity_type,account_number,account_name,bdc,bdc_role,bdc_region,opp_owner,opp_owner_role,opp_owner_region,sf_support_segment,sf_support_team,support_region,ec_queue_name,ec_contract_received_date,ec_contract_type,online_to_forecast_flag,online_to_initialforecast_flag,om_flag,opp_id_type,vm_flag,om_bucket,device_id,device_number,device_type,device_type_econnect,device_status,device_online_date,device_online_tmk,device_initial_forecast_date,device_initial_tmk,days_online_initialforecast,device_forecasted_online_date,device_forecast_tmk,days_online_forecasted,bookings_bucket,free_days,debook_flag,debook_date,debook_device_mrr_local,cast( debooked_mrr as numeric) as debooked_mrr,device_gross_mrr_local,cast(device_gross_mrr_usd as numeric) as device_gross_mrr_usd,device_previous_mrr_local,cast(device_previous_mrr_usd as numeric) as device_previous_mrr_usd,device_net_mrr_local,cast(device_net_mrr_usd as numeric) as device_net_mrr_usd,device_currency,ec_om_reason,ec_om_note,is_ec_om,kickback_reason_created,kickback_reason_resolved,kickback_reason_id,kickback_reason,kickback_reason_group,kickback_reason_sales_group,time_in_kickback,in_kickback,is_trackable,materialization_type,materialization_date,materialization_tmk,cast(prorate_multiple as numeric) as prorate_multiple,cast(materialization_conversion_rate as numeric) as materialization_conversion_rate,full_materialization_local,full_materialization_usd,materialization_minus_migration_local,materialization_minus_migration_usd,materialization_tmk2,m1_prorated_full,m1_prorated_minus_migration,m2_prorated_full,m2_prorated_minus_migration, current_date() As load_Date
FROM   `rax-abo-72-dev`.report_tables.Fin
ORDER  BY Opp_Line_Num,
          Device_Line_Num;
end;