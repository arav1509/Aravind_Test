
CREATE OR REPLACE PROCedure `rax-abo-72-dev`.sales.udsp_etl_ub_upfront_bookings()
begin
--------------------------------------------------------------------------------------------------------------------      
DECLARE MAXDATE datetime;
DECLARE v_DATE datetime;

--------------------------------------------------------------------------------------------------------------------  
SET MAXDATE =(SELECT MAX(Updated_Datetime) FROM `rax-abo-72-dev`.sales.ub_upfront_bookings );


create or replace temp table OppState_Stage as 
SELECT * --INTO     OppState_Stage
FROM (
SELECT
     OS.Close_Date_Key,
     OS.Opportunity_Deleted,
	OS.Record_Updated_Datetime,	
	O.Opportunity_Account_SF_ID,
	O.Opportunity_DDI,
	O.Opportunity_SF_ID,
	O.Opportunity_ID,
	O.Opportunity_Close_Date,
	O.Opportunity_Type,
	O.Opportunity_Currency_Iso_Code,
	O.Opportunity_Cloud_Booking_Type_2,
	O.Opportunity_Cloud_Contract_Type,
	A.Account_Key,
	A.Account_Number,
	A.Account_DDI,
	A.Account_Customer_id_Datapipe,
	Amount,
	Debook_Amount,
	Professional_Services_One_Time_Fee,
	Setup_Booking_Fee
FROM 
	`rax-datamart-dev`.dwh_db.fact_sf_rpt_opportunity_state OS 
INNER JOIN
    `rax-datamart-dev`.dwh_db.dim_sf_opportunity O 
ON OS.Opportunity_Key = O.Opportunity_Key
INNER JOIN
    `rax-datamart-dev`.dwh_db.dim_sf_account A 
ON OS.Account_Key = A.Account_Key 
INNER JOIN
    `rax-datamart-dev`.dwh_db.dim_sf_opportunity_amounts OA 
ON OS.Opportunity_Amounts_Key = OA.Opportunity_Amounts_Key
WHERE 
    OS.Current_Record = 1 
AND UPPER(OS.Opportunity_Deleted) <> 'Y'
AND UPPER(O.Opportunity_Stage_Name) <> 'CLOSED LOST'
AND UPPER(O.Opportunity_is_closed) = 'TRUE'
AND UPPER(O.Opportunity_Cloud_Contract_Type) in ('REGULAR','COMMIT','HLD','EDP')
AND Record_Updated_Datetime >=datetime_add(MAXDATE, interval -1 Hour)
);



DELETE FROM  `rax-abo-72-dev`.sales.ub_upfront_bookings
WHERE
   EXISTS
(
SELECT
	Opportunity_ID
FROM
	OppState_Stage XX
WHERE
	XX.Opportunity_ID =UB_Upfront_Bookings.Opportunity_ID
);
--********************************************************************************************************************* 


INSERT INTO `rax-abo-72-dev`.sales.ub_upfront_bookings
SELECT DISTINCT
	Opportunity_Account_SF_ID as Account_SF_ID,
	Account_Number,
	Account_DDI as DDI,
	--A.Account_Number_Datapipe,
	--A.Account_Id_Datapipe,
	Account_Customer_id_Datapipe as DP_Customer_ID,
	Opportunity_DDI,
	Opportunity_SF_ID,
	Opportunity_ID,
	Opportunity_Close_Date as CloseDate,
	cast((Close_Date_Key/100) as int64) as CloseDateKey,
	Opportunity_Type as Type,
	Amount,
	Debook_Amount,
	--(OA.Amount + OA.Debook_Amount) as Upfront_Booking,
	Amount - (.1*ifnull(professional_services_one_time_fee,0)) - Setup_Booking_Fee + 
	(case when Debook_amount <> 0 then Debook_Amount + (.1*ifnull(professional_services_one_time_fee,0)) + Setup_Booking_Fee else Debook_amount end) as Upfront_Booking,
	Opportunity_Currency_Iso_Code as CurrencyISOCode,
	Opportunity_Cloud_Booking_Type_2 as Cloud_Booking_Type2,
	Opportunity_Cloud_Contract_Type as Cloud_Contract_Type,
	Opportunity_Deleted as Delete_Flag,
	Professional_Services_One_Time_Fee as Professional_Services_Fee,
	Setup_Booking_Fee as Set_Up_Fees,
	CASE WHEN upper(Opportunity_Cloud_Booking_Type_2) in ('STANDARD CLOUD','SERVICE BLOCKS') THEN Opportunity_Type
	ELSE Opportunity_Cloud_Booking_Type_2 END AS Booking_Platform,
	Record_Updated_Datetime			AS Updated_Datetime
FROM 
	OppState_Stage OS
;
--********************************************************************************************************************* 

DELETE `rax-abo-72-dev`.sales.ub_upfront_bookings A
where A.Opportunity_ID in(
select A.Opportunity_ID
FROM
`rax-abo-72-dev`.sales.ub_upfront_bookings A
JOIN (SELECT * FROM (
SELECT Opportunity_ID 
FROM `rax-datamart-dev`.dwh_db.dim_sf_opportunity O  
WHERE Opportunity_Current_Record = 1 and upper(Opportunity_Stage_Name) = 'CLOSED LOST'
)) B
ON A.Opportunity_id = B.Opportunity_ID
)
;


end;
