create or replace procedure `rax-staging-dev`.stage_three_dw.udsp_etl_fact_invoice_line_item_dedicated_prepay()
BEGIN
/* =============================================
-- Created By :	Rahul Chourasiya
-- Create date: 8.27.2019
-- Description: Deletes and reloads 2 months of prepay records from Fact_Invoice_Event_Detail_Dedicated
-- =============================================*/
--------------------------------------------------------------------------------------------------------------

DECLARE GL_BeginDate int64;
DECLARE GL_EndDate int64;
DECLARE vRNUM  int64;
DECLARE MAX_ROW  int64;

DECLARE FIN_START_TIME_MONTH  int64;
DECLARE FIN_END_TIME_MONTH  int64;
DECLARE BILL_NUM  string;

DECLARE Fin_Start_Date int64;
DECLARE Fin_End_Date int64;
DECLARE Fin_Start_Date_Key int64;
DECLARE Fin_End_Date_Key int64;
DECLARE RunDate  int64;
DECLARE Execution_Time DateTime;
DECLARE Looping int64;

DECLARE VDate  datetime;
DECLARE Prior_Date datetime;
DECLARE Time_Key  int64;

SET GL_BeginDate = `rax-staging-dev`.bq_functions.udf_yearmonth_nohyphen(date_sub(current_date(), INTERVAL 2 month));
SET GL_EndDate = `rax-staging-dev`.bq_functions.udf_yearmonth_nohyphen(current_date());

create or replace temp  table  Prepays_ALL as
SELECT distinct --INTO #Prepays_ALL
     Invoice.Bill_Number
    ,Fact.Date_Month_Key
    ,Fact.Revenue_Type_Key
    ,Fact.Account_Key
    ,Fact.Team_Key
    ,Fact.Primary_Contact_Key
    ,Fact.Billing_Contact_Key
    ,Fact.Product_Key
    ,Fact.Datacenter_Key
    ,Fact.Device_Key
    ,Fact.Item_Key
    ,Fact.GL_Product_Key
    ,GL_Account_Amortized.GL_Account_Key
    ,Fact.Glid_Configuration_Key
    ,Fact.Payment_Term_Key
    ,Fact.Event_Type_Key
    ,Fact.Invoice_Key
    ,Fact.Invoice_Attribute_Key
    ,Fact.Raw_Start_Month_Key
    ,Fact.Invoice_Date_Utc_Key
    ,Fact.Bill_Created_Date_Utc_Key
    ,Fact.Bill_Created_Time_Utc_Key
    ,Fact.Bill_Created_Date_Cst_Key
    ,Fact.Bill_Created_Time_Cst_Key
    ,Fact.Bill_Start_Date_Utc_Key
    ,Fact.Bill_End_Date_Utc_Key
    ,Fact.Prepay_Start_Date_Utc_Key
    ,Fact.Prepay_End_Date_Utc_Key
    ,Fact.Event_Min_Created_Date_Utc_Key
    ,Fact.Event_Min_Created_Time_Utc_Key
    ,Fact.Event_Min_Created_Date_Cst_Key
    ,Fact.Event_Min_Created_Time_Cst_Key
    ,Fact.Earned_Min_Start_Date_Utc_Key
    ,Fact.Earned_Min_Start_Time_Utc_Key
    ,Fact.Earned_Min_Start_Date_Cst_Key
    ,Fact.Earned_Min_Start_Time_Cst_Key
	,Fact.Event_Max_Created_Date_Utc_Key
    ,Fact.Event_Max_Created_Time_Utc_Key
    ,Fact.Event_Max_Created_Date_Cst_Key
    ,Fact.Event_Max_Created_Time_Cst_Key
    ,Fact.Earned_Max_Start_Date_Utc_Key
    ,Fact.Earned_Max_Start_Time_Utc_Key
    ,Fact.Earned_Max_Start_Date_Cst_Key
    ,Fact.Earned_Max_Start_Time_Cst_Key
    ,Fact.Currency_Key
    ,Fact.Transaction_Term
    ,Fact.Quantity
    ,Fact.Billing_Days_In_Month
	,Fact.Amount_Usd
	,Fact.Extended_Amount_Usd
	,Fact.Unit_Selling_Price_Usd
	,Fact.Amount_Gbp
	,Fact.Extended_Amount_Gbp
	,Fact.Unit_Selling_Price_Gbp
    ,Fact.Amount_Local
    ,Fact.Extended_Amount_Local
    ,Fact.Unit_Selling_Price_Local
    ,Fact.Is_Standalone_Fee
	,1 As Is_Prepay
	,1 As Is_Amortize_Prepay
    ,Fact.Is_Back_Bill
    ,Fact.Is_Fastlane
	,Fact.Source_System_Key
    ,ifnull(PARSE_TIMESTAMP("%Y%m",cast(Fact.Raw_Start_Month_Key as string) ),PARSE_TIMESTAMP("%Y%m%d",cast(Fact.Invoice_Date_Utc_Key as string))) As Raw_Start_Convert_Date
    ,case
        when Fact.Transaction_Term=0 then Fact.Quantity 
        when Fact.Transaction_Term < Fact.Quantity then Fact.Quantity
        else Fact.Transaction_Term
     end as Amortized_Calculated_Value
    ,cast(ifnull(coalesce(Quantity,Transaction_Term),1) as int64) as Amortized_Term1
    ,cast(ifnull(Transaction_Term,1) as int64) as Amortized_Term2
    ,case
        when lower(Invoice.Unit_Measure_Of_Code) in ('mth','ea') then Fact.Unit_Selling_Price_Local
        when lower(Invoice.Unit_Measure_Of_Code) in ('day') then Fact.Extended_Amount_Local
     end     as Amortized_value,
	  DATETIME_TRUNC(Invoice_Date.Time_Full_Date, MONTH)	AS Invoice_Date_to_First_Day_of_Month,
	date('1900-01-01')					    AS  PREPAY_START_DATE_K,
	date('1900-01-01')						    AS  PREPAY_END_DATE_K,
	round(CAST(0 as numeric),4)							    AS  AMOUNT, 
	CAST(0 as numeric)							    AS  Calculated_QUANTITY,
	Invoice.Unit_Measure_Of_Code,
	0 As Fin_Start_Time_Month_Key,
	0 As Fin_End_Time_Month_Key,
	round(CAST(0 as numeric),4) As Amortized,
	round(CAST(0 as numeric),4) As Amortized_USD,
	round(CAST(0 as numeric),4) As Amortized_GBP
FROM 
     `rax-datamart-dev`.corporate_dmart.fact_invoice_line_item_dedicated fact 
    left join  `rax-datamart-dev`.corporate_dmart.dim_gl_account glaccount  on fact.gl_account_key=glaccount.gl_account_key
    left join  `rax-datamart-dev`.corporate_dmart.dim_invoice invoice  on fact.invoice_key=invoice.invoice_key and Invoice.Bill_Number is not null
	left join `rax-staging-dev`.stage_three_dw.dim_time invoice_date  on invoice_date.time_key = fact.invoice_date_utc_key
	left join  `rax-datamart-dev`.corporate_dmart.dim_gl_account gl_account_amortized  on concat(lower(glaccount.gl_account_id),'a') = lower(gl_account_amortized.gl_account_id )
	and gl_account_amortized.current_record = 1 and lower(gl_account_amortized.source_system_name) = 'brm'
WHERE
    lower(GLAccount.GL_Account_Name) like'%prepay%hosting%'
AND lower(GLAccount.GL_Account_Id) Not In ('419600A','419000A')
AND Fact.Amount_Local   <> 0
--AND Fact.Date_Month_Key >= GL_BeginDate
--AND	Fact.Date_Month_Key <= GL_EndDate

;


update  Prepays_ALL A
set
    Unit_Measure_Of_Code= 'DAY'
WHERE
     Amortized_Calculated_value=1
and IFNULL(lower(Unit_Measure_Of_Code),'unknown') <>'mth' AND IfNULL(lower(Unit_Measure_Of_Code),'unknown')<>'day';

----------------------------------------------------------------------------------------------------------------- 
UPDATE   Prepays_ALL  A
SET
	A.Bill_Start_Date_Utc_Key =cast( ifnull(FORMAT_DATE("%Y%m%d",date(Raw_Start_Convert_Date) ) ,'19000101') as int64),
	A.Bill_End_Date_Utc_Key = cast(FORMAT_DATE("%Y%m%d",date(datetime_add( cast(Raw_Start_Convert_Date as datetime),INTERVAL cast(Amortized_Calculated_value as int64) DAY)) )  as int64),
	A.PREPAY_START_DATE_Utc_Key=cast( ifnull(FORMAT_DATE("%Y%m%d",date(Raw_Start_Convert_Date) ) ,'19000101') as int64),
	A.PREPAY_END_DATE_Utc_Key= cast(FORMAT_DATE("%Y%m%d",date(datetime_add( cast(Raw_Start_Convert_Date as datetime),INTERVAL cast(Amortized_Calculated_value as int64) DAY)) )  as int64),
	PREPAY_START_DATE_K=date(DATETIME_TRUNC(cast(Raw_Start_Convert_Date as datetime), MONTH)), 
	PREPAY_END_DATE_K=date(`rax-staging-dev`.bq_functions.udf_lastdayofmonth(datetime_add( cast(Raw_Start_Convert_Date as datetime),INTERVAL cast(Amortized_Calculated_value as int64) DAY)) )
WHERE  
    upper(Unit_Measure_Of_Code)= 'DAY';
----------------------------------------------------------------------------------------------------------------- 
UPDATE   Prepays_ALL  A
SET
	A.Bill_Start_Date_Utc_Key =cast( ifnull(FORMAT_DATE("%Y%m%d",date(Raw_Start_Convert_Date) ) ,'19000101') as int64),
	A.Bill_End_Date_Utc_Key = cast(FORMAT_DATE("%Y%m%d",date(datetime_add( cast(Raw_Start_Convert_Date as datetime),INTERVAL cast(Amortized_Calculated_value as int64) DAY)) )  as int64),
	A.PREPAY_START_DATE_Utc_Key=cast( ifnull(FORMAT_DATE("%Y%m%d",date(Raw_Start_Convert_Date) ) ,'19000101') as int64),
	A.PREPAY_END_DATE_Utc_Key= cast(FORMAT_DATE("%Y%m%d",date(datetime_add( cast(Raw_Start_Convert_Date as datetime),INTERVAL cast(Amortized_Calculated_value as int64) DAY)) )  as int64)
	--Prepay_Match_Method=	'No source source so calculated (case when Trx_Term = 0 then Trx_Qty else trx_term end) =term, PREPAY_START_DATE=conversion of trx_raw_start_date to date, PREPAY_END_DATE = DateAdd(Month,CAST(case when Trx_Term = 0 then Trx_Qty else trx_term end) AS numeric),trx_raw_start_date to date)'
WHERE
   A.PREPAY_START_DATE_Utc_Key = 19000101;
-----------------------------------------------------------------------------------------------------------------
UPDATE Prepays_ALL  trg
SET
	
	PREPAY_START_DATE_K= DATE(`rax-abo-72-dev`.bq_functions.udf_firstdayofmonth(src.Time_Full_Date)), 
	PREPAY_END_DATE_K=DATE((CASE 
							WHEN src.Time_Full_Date = `rax-staging-dev`.bq_functions.udf_lastdayofmonth(src.Time_Full_Date) THEN  DATE_ADD(DATE(src.Time_Full_Date), interval 2 day)
							WHEN src.Time_Full_Date <> `rax-abo-72-dev`.bq_functions.udf_firstdayofmonth(src.Time_Full_Date) THEN  `rax-abo-72-dev`.bq_functions.udf_firstdayofmonth(src.Time_Full_Date) 
							ELSE src.Time_Full_Date
						END))
FROM
(
select distinct PREPAY_START_DATE.Time_Full_Date,Bill_Number
from(
	`rax-datamart-dev`.corporate_dmart.Prepays_ALL A	
	inner join `rax-staging-dev`.stage_three_dw.dim_time prepay_start_date 
	on prepay_start_date.time_key = a.prepay_start_date_utc_key
	inner join `rax-staging-dev`.stage_three_dw.dim_time prepay_end_date 
	on prepay_end_date.time_key = a.prepay_end_date_utc_key
and
     upper(A.Unit_Measure_Of_Code) <> 'DAY'
	 )
)src
where trg.Bill_Number=src.Bill_Number;
----------------------------------------------------------------------------------------------------------------------	
Update Prepays_ALL A
Set
	A.Fin_Start_Time_Month_Key = `rax-abo-72-dev`.bq_functions.udf_time_key_nohyphen(date(PREPAY_START_DATE_K)),
	A.Fin_End_Time_Month_Key = CASE 
	   WHEN  
		  upper(Unit_Measure_Of_Code)<> 'DAY' 
	   THEN 
			`rax-abo-72-dev`.bq_functions.udf_time_key_nohyphen(date_sub(date(PREPAY_END_DATE_K), interval 1 month))
	   ELSE
		  `rax-abo-72-dev`.bq_functions.udf_time_key_nohyphen(date(PREPAY_END_DATE_K))
	   END,
	A.Amortized = CASE 
	   WHEN  
		  upper(Unit_Measure_Of_Code)= 'DAY' 
	   THEN
		  round(CAST(Amount_Local/1 as numeric),4)
	   ELSE
		   round(CAST(Amount_Local/Amortized_Calculated_value as numeric),4)
	END,
	A.Amortized_USD = CASE 
	   WHEN  
		  upper(Unit_Measure_Of_Code)= 'DAY' 
	   THEN
		  round(CAST(Amount_USD/1 as numeric),4)
	   ELSE
		  round(CAST(Amount_USD/Amortized_Calculated_value as numeric),4)	
	END,
	A.Amortized_GBP = CASE 
	   WHEN  
		  upper(Unit_Measure_Of_Code)= 'DAY' 
	   THEN
		  round(CAST(Amount_GBP/1 as numeric),4)	
	   ELSE
		   round(CAST(Amount_GBP/Amortized_Calculated_value as numeric),4)	
	END
  where true
;

----------------------------------------------------------------------------------------------------------------------
--TRUNCATING STAGE TABLE FOR INCREMENT LOAD
----------------------------------------------------------------------------------------------------------------------
delete from  `rax-staging-dev`.stage_two_dw.stage_fact_invoice_line_item_dedicated_prepay where true;



----------------------------------------------------------------------------------------------------------------------
--LOADING DATA INTO #ROW_BILL_ID FROM #DISTINCT_BILL_ID WITH ADDITIONAL COLUMN ROW NUMBER FOR PREPAY LOOP
----------------------------------------------------------------------------------------------------------------------


create or replace temp table Row_Bill_Id as
SELECT BILL_NUMBER,--INTO #Row_Bill_Id
       FIN_START_TIME_MONTH_KEY,
	   FIN_END_TIME_MONTH_KEY,
	   ROW_NUMBER() OVER (ORDER BY BILL_NUMBER, FIN_START_TIME_MONTH_KEY, FIN_END_TIME_MONTH_KEY) AS RNUM
FROM (
	SELECT DISTINCT BILL_NUMBER, --INTO #Distinct_Bill_Id
		   FIN_START_TIME_MONTH_KEY,
		   FIN_END_TIME_MONTH_KEY
	FROM Prepays_ALL
  where BILL_NUMBER is not null
	ORDER BY BILL_NUMBER,FIN_START_TIME_MONTH_KEY,FIN_END_TIME_MONTH_KEY ASC
	)--#Distinct_Bill_Id
;
---
SET vRNUM    = 1;
SET MAX_ROW = 2;

WHILE vRNUM <= MAX_ROW 
DO

select AS STRUCT BILL_NUMBER,FIN_START_TIME_MONTH_KEY, FIN_END_TIME_MONTH_KEY fROM Row_Bill_Id WHERE RNUM = vRNUM;

set (BILL_NUM, FIN_START_TIME_MONTH,  FIN_END_TIME_MONTH) = ( select AS STRUCT BILL_NUMBER,FIN_START_TIME_MONTH_KEY, FIN_END_TIME_MONTH_KEY fROM Row_Bill_Id WHERE RNUM = vRNUM);

SET Fin_Start_Date = (SELECT MIN(FIN_START_TIME_MONTH_KEY) FROM Prepays_ALL A Where BILL_NUMBER = BILL_NUM and FIN_START_TIME_MONTH_KEY = FIN_START_TIME_MONTH and FIN_END_TIME_MONTH_KEY = FIN_END_TIME_MONTH );
SET Fin_End_Date   = (SELECT MAX(FIN_END_TIME_MONTH_KEY)   FROM Prepays_ALL A Where BILL_NUMBER = BILL_NUM and FIN_START_TIME_MONTH_KEY = FIN_START_TIME_MONTH and FIN_END_TIME_MONTH_KEY = FIN_END_TIME_MONTH);
select Fin_Start_Date;
SET RunDate        = Fin_Start_Date;

SET Execution_Time = current_datetime();--CAST(GETDATE() AS DATETIME2(2))
SET Looping        = RunDate;-- CONVERT(VARCHAR(8),RunDate,112)

SET Fin_Start_Date_Key = cast((SELECT substr(cast(MIN(FIN_START_TIME_MONTH_KEY) as string),1,6) FROM Prepays_ALL WHERE FIN_START_TIME_MONTH_KEY <> 19700101) as int64);
SET Fin_End_Date_Key   = cast((SELECT substr(cast(MAX(FIN_END_TIME_MONTH_KEY)  as string) ,1,6)  FROM Prepays_ALL WHERE FIN_START_TIME_MONTH_KEY <> 19700101)as int64);


	WHILE RunDate <= Fin_End_Date
	DO
		
		SET VDate= RunDate;
		SET Time_Key =`rax-abo-72-dev`.bq_functions.udf_time_key_nohyphen(VDate);
		
		
			INSERT INTO `rax-staging-dev`.stage_two_dw.stage_fact_invoice_line_item_dedicated_prepay
				(
				 Bill_Number
				,Date_Month_Key
				,Revenue_Type_Key
				,Account_Key
				,Team_Key
				,Primary_Contact_Key
				,Billing_Contact_Key
				,Product_Key
				,Datacenter_Key
				,Device_Key
				,Item_Key
				,GL_Product_Key
				,GL_Account_Key
				,Glid_Configuration_Key
				,Payment_Term_Key
				,Event_Type_Key
				,Invoice_Key
				,Invoice_Attribute_Key
				,Raw_Start_Month_Key
				,Invoice_Date_Utc_Key
				,Bill_Created_Date_Utc_Key
				,Bill_Created_Time_Utc_Key
				,Bill_Start_Date_Utc_Key
				,Bill_End_Date_Utc_Key
				,Prepay_Start_Date_Utc_Key
				,Prepay_End_Date_Utc_Key
				,Event_Min_Created_Date_Utc_Key
				,Event_Min_Created_Time_Utc_Key
				,Earned_Min_Start_Date_Utc_Key
				,Earned_Min_Start_Time_Utc_Key
				,Event_Max_Created_Date_Utc_Key
				,Event_Max_Created_Time_Utc_Key
				,Earned_Max_Start_Date_Utc_Key
				,Earned_Max_Start_Time_Utc_Key
				,Currency_Key
				,Transaction_Term
				,quantity
				,Billing_Days_In_Month
				,Amount_Usd
				,Extended_Amount_Usd
				,Unit_Selling_Price_Usd
				,Amount_Gbp
				,Extended_Amount_Gbp
				,Unit_Selling_Price_Gbp
				,Amount_Local
				,Extended_Amount_Local
				,Unit_Selling_Price_Local
				,Is_Standalone_Fee
				,Is_Prepay
				,Is_Amortize_Prepay
				,Is_Back_Bill
				,Is_Fastlane
				,Bill_Created_Date_Cst_Key
				,Bill_Created_Time_Cst_Key
				,Event_Min_Created_Date_Cst_Key
				,Event_Min_Created_Time_Cst_Key
				,Event_Max_Created_Date_Cst_Key
				,Event_Max_Created_Time_Cst_Key
				,Earned_Min_Start_Date_Cst_Key
				,Earned_Min_Start_Time_Cst_Key
				,Earned_Max_Start_Date_Cst_Key
				,Earned_Max_Start_Time_Cst_Key
				,Raw_Start_Convert_Date
				,Amortized_Calculated_Value
				,Amortized_Term1
				,Amortized_Term2
				,Amortized_value
				,Invoice_Date_to_First_Day_of_Month
				,Prepay_Start_Date_K
				,Prepay_End_Date_K
				,Amount
				,Calculated_Quantity
				,Unit_Measure_Of_Code
				,Fin_Start_Time_Month_Key
				,Fin_End_Time_Month_Key
				,Amortized
				,Amortized_USD
				,Amortized_GBP
				,Amortized_Invoice_Date
				,Time_Month_Key
				,TOTAL
				,Record_Created_By
				,Record_Created_Datetime
				,Record_Updated_By
				,Record_Updated_Datetime
				,Source_System_Key
				)
		SELECT DISTINCT 
				 Bill_Number
				,Date_Month_Key
				,Revenue_Type_Key
				,Account_Key
				,Team_Key
				,Primary_Contact_Key
				,Billing_Contact_Key
				,Product_Key
				,Datacenter_Key
				,Device_Key
				,Item_Key
				,GL_Product_Key
				,GL_Account_Key
				,Glid_Configuration_Key
				,Payment_Term_Key
				,Event_Type_Key
				,Invoice_Key
				,Invoice_Attribute_Key
				,Raw_Start_Month_Key
				,Invoice_Date_Utc_Key
				,Bill_Created_Date_Utc_Key
				,Bill_Created_Time_Utc_Key
				,Bill_Start_Date_Utc_Key
				,Bill_End_Date_Utc_Key
				,Prepay_Start_Date_Utc_Key
				,Prepay_End_Date_Utc_Key
				,Event_Min_Created_Date_Utc_Key
				,Event_Min_Created_Time_Utc_Key
				,Earned_Min_Start_Date_Utc_Key
				,Earned_Min_Start_Time_Utc_Key
				,Event_Max_Created_Date_Utc_Key
				,Event_Max_Created_Time_Utc_Key
				,Earned_Max_Start_Date_Utc_Key
				,Earned_Max_Start_Time_Utc_Key
				,Currency_Key
				,Transaction_Term
				,Quantity
				,Billing_Days_In_Month
				,Amount_Usd
				,Extended_Amount_Usd
				,Unit_Selling_Price_Usd
				,Amount_Gbp
				,Extended_Amount_Gbp
				,Unit_Selling_Price_Gbp
				,Amount_Local
				,Extended_Amount_Local
				,Unit_Selling_Price_Local
				,cast(Is_Standalone_Fee as INT64  ) as Is_Standalone_Fee
				,cast(Is_Prepay  as INT64  ) as Is_Prepay
				,cast(Is_Amortize_Prepay  as INT64  ) as Is_Amortize_Prepay
				,cast(Is_Back_Bill  as INT64  ) as Is_Back_Bill
				,cast(Is_Fastlane  as INT64  ) as Is_Fastlane
				,Bill_Created_Date_Cst_Key
				,Bill_Created_Time_Cst_Key
				,Event_Min_Created_Date_Cst_Key
				,Event_Min_Created_Time_Cst_Key
				,Event_Max_Created_Date_Cst_Key
				,Event_Max_Created_Time_Cst_Key
				,Earned_Min_Start_Date_Cst_Key
				,Earned_Min_Start_Time_Cst_Key
				,Earned_Max_Start_Date_Cst_Key
				,Earned_Max_Start_Time_Cst_Key
				,cast(Raw_Start_Convert_Date as DATETIME ) as Raw_Start_Convert_Date
				,Amortized_Calculated_Value
				,Amortized_Term1
				,Amortized_Term2
				,Amortized_value
				,Invoice_Date_to_First_Day_of_Month
				,PREPAY_START_DATE_K
				,PREPAY_END_DATE_K
				,AMOUNT
				,cast(Calculated_QUANTITY as int64 ) as Calculated_QUANTITY
				,Unit_Measure_Of_Code
				,Fin_Start_Time_Month_Key
				,Fin_End_Time_Month_Key
				,Amortized
				,Amortized_USD
				,Amortized_GBP
				,CAST(DT.Time_Full_Date as datetime)				AS Amortized_Invoice_Date
				,Time_Key											AS Time_Month_Key
				,ROUND(CAST(Amortized as NUMERIC),4)					AS TOTAL
				,'udsp_etl_Fact_Invoice_Line_Item_Dedicated_Prepay' As Record_Created_By
				,cast(Execution_Time as datetime)				As Record_Created_Datetime
				,'udsp_etl_Fact_Invoice_Line_Item_Dedicated_Prepay' As Record_Updated_By
				,cast(Execution_Time as datetime)				As Record_Updated_Datetime
				,Source_System_Key
		FROM   
			 Prepays_ALL A --Prepays_ALL A
		INNER JOIN
			`rax-staging-dev`.stage_three_dw.dim_time DT 
		ON (Time_Key/100) = (DT.Time_Key/100)
		AND Time_Last_Day_Month_Flag = 1
		WHERE  
			BILL_NUMBER = BILL_NUM 
		AND Fin_Start_Time_Month_Key = FIN_START_TIME_MONTH 
		AND Fin_End_Time_Month_Key = FIN_END_TIME_MONTH
		AND	Time_Key >= Fin_Start_Date
		AND Time_Key <= Fin_End_Date;
		
		SET RunDate = date_add(date(RunDate), interval 1 month );--(DATEADD(M,1,stage_three_dw.udfdatepart(RunDate)))
	
	END WHILE ;--- Inner Loop

	SET vRNUM = vRNUM+1;

END WHILE; -- Main Loop



---------------------------------------------------------------------------------------------------------------
--DELETING ALREADY LOADED DATA FROM FACT TABLE
---------------------------------------------------------------------------------------------------------------
DELETE from `rax-datamart-dev`.corporate_dmart.fact_invoice_line_item_dedicated  trg 
where trg.GL_Account_Key in(
select Fact.GL_Account_Key
FROM 
    `rax-datamart-dev`.corporate_dmart.fact_invoice_line_item_dedicated Fact
INNER JOIN
    `rax-datamart-dev`.corporate_dmart.dim_gl_account GL_Account
	on Fact.GL_Account_Key = GL_Account.GL_Account_Key 
WHERE 
   upper( GL_Account.GL_Account_Id) in('419600A','419000A') AND
    Date_Month_Key >= Fin_Start_Date_Key
AND Date_Month_Key <= Fin_End_Date_Key
);

-----------------------------------------------------------------------------------------------------------------
--INSERTING INVOICE DATA INTO FACT TABLE FROM STAGE TABLE
---------------------------------------------------------------------------------------------------------------
INSERT INTO `rax-datamart-dev`.corporate_dmart.fact_invoice_line_item_dedicated
           (Date_Month_Key
           ,Revenue_Type_Key
           ,Account_Key
           ,Team_Key
           ,Primary_Contact_Key
           ,Billing_Contact_Key
           ,Product_Key
           ,Datacenter_Key
           ,Device_Key
           ,Item_Key
           ,GL_Product_Key
           ,GL_Account_Key
           ,GLid_Configuration_Key
           ,Payment_Term_Key
           ,Event_Type_Key
           ,Invoice_Key
           ,Invoice_Attribute_Key
           ,Raw_Start_Month_Key
           ,Invoice_Date_Utc_Key
           ,Bill_Created_Date_Utc_Key
           ,Bill_Created_Time_Utc_Key
           ,Bill_Start_Date_Utc_Key
           ,Bill_End_Date_Utc_Key
           ,Prepay_Start_Date_Utc_Key
           ,Prepay_End_Date_Utc_Key
           ,Event_Min_Created_Date_Utc_Key
           ,Event_Min_Created_Time_Utc_Key
           ,Event_Max_Created_Date_Utc_Key
           ,Event_Max_Created_Time_Utc_Key
           ,Earned_Min_Start_Date_Utc_Key
           ,Earned_Min_Start_Time_Utc_Key
           ,Earned_Max_Start_Date_Utc_Key
           ,Earned_Max_Start_Time_Utc_Key
           ,Currency_Key
           ,Transaction_Term
           ,Quantity
           ,Billing_Days_In_Month
           ,Amount_Usd
           ,Extended_Amount_Usd
           ,Unit_Selling_Price_Usd
           ,Amount_Gbp
           ,Extended_Amount_Gbp
           ,Unit_Selling_Price_Gbp
           ,Amount_Local
           ,Extended_Amount_Local
           ,Unit_Selling_Price_Local
           ,Is_Standalone_Fee
		   ,Is_Prepay
		   ,Is_Amortize_Prepay
           ,Is_Back_Bill
           ,Is_Fastlane
           ,Bill_Created_Date_Cst_Key
           ,Bill_Created_Time_Cst_Key
           ,Event_Min_Created_Date_Cst_Key
           ,Event_Min_Created_Time_Cst_Key
           ,Event_Max_Created_Date_Cst_Key
           ,Event_Max_Created_Time_Cst_Key
           ,Earned_Min_Start_Date_Cst_Key
           ,Earned_Min_Start_Time_Cst_Key
           ,Earned_Max_Start_Date_Cst_Key
           ,Earned_Max_Start_Time_Cst_Key
		   ,Record_Created_Source_Key
		   ,Record_Created_Datetime
		   ,Record_Updated_Source_Key 
		   ,Record_Updated_Datetime
           ,Source_System_Key)
 SELECT 
  cast(substr(CAST(Time_Month_Key AS string),1,6) as int64) As Date_Month_Key
 ,Revenue_Type_Key
 ,Account_Key
 ,Team_Key
 ,Primary_Contact_Key
 ,Billing_Contact_Key
 ,Product_Key
 ,Datacenter_Key
 ,Device_Key
 ,Item_Key
 ,GL_Product_Key
 ,GL_Account_Key
 ,GLid_Configuration_Key
 ,Payment_Term_Key
 ,Event_Type_Key
 ,Invoice_Key
 ,Invoice_Attribute_Key
 ,Raw_Start_Month_Key
 ,`rax-abo-72-dev`.bq_functions.udf_time_key_nohyphen(Amortized_Invoice_Date) As Invoice_Date_Utc_Key
 ,Bill_Created_Date_Utc_Key
 ,Bill_Created_Time_Utc_Key
 ,Bill_Start_Date_Utc_Key
 ,Bill_End_Date_Utc_Key
 ,Prepay_Start_Date_Utc_Key
 ,Prepay_End_Date_Utc_Key
 ,Event_Min_Created_Date_Utc_Key
 ,Event_Min_Created_Time_Utc_Key
 ,Event_Max_Created_Date_Utc_Key
 ,Event_Max_Created_Time_Utc_Key
 ,Earned_Min_Start_Date_Utc_Key
 ,Earned_Min_Start_Time_Utc_Key
 ,Earned_Max_Start_Date_Utc_Key
 ,Earned_Max_Start_Time_Utc_Key
 ,Currency_Key
 ,Transaction_Term
 ,Quantity
 ,Billing_Days_In_Month
 ,Amortized_USD AS Amount_Usd
 ,Amortized_USD AS Extended_Amount_Usd
 ,Amortized_USD AS Unit_Selling_Price_Usd
 ,Amortized_GBP AS Amount_Gbp
 ,Amortized_GBP AS Extended_Amount_Gbp
 ,Amortized_GBP AS Unit_Selling_Price_Gbp
 ,TOTAL AS Amount_Local
 ,TOTAL AS Extended_Amount_Local
 ,TOTAL AS Unit_Selling_Price_Local
 ,Is_Standalone_Fee
 ,Is_Prepay
 ,Is_Amortize_Prepay
 ,Is_Back_Bill
 ,Is_Fastlane
 ,Bill_Created_Date_Cst_Key
 ,Bill_Created_Time_Cst_Key
 ,Event_Min_Created_Date_Cst_Key
 ,Event_Min_Created_Time_Cst_Key
 ,Event_Max_Created_Date_Cst_Key
 ,Event_Max_Created_Time_Cst_Key
 ,Earned_Min_Start_Date_Cst_Key
 ,Earned_Min_Start_Time_Cst_Key
 ,Earned_Max_Start_Date_Cst_Key
 ,Earned_Max_Start_Time_Cst_Key
 ,DRC.Record_Source_Key
 ,Record_Created_Datetime
 ,DRC.Record_Source_Key
 ,Record_Updated_Datetime
 ,Source_System_Key
 FROM `rax-staging-dev`.stage_two_dw.stage_fact_invoice_line_item_dedicated_prepay Prepay
 INNER JOIN `rax-staging-dev`.stage_three_dw.dim_record_source DRC 
 ON Prepay.Record_Created_By = DRC.Record_Source_Name
 WHERE
    Date_Month_Key >= Fin_Start_Date_Key
AND Date_Month_Key <= Fin_End_Date_Key;
-------------------------------------------------------------------------------------------------------------------
/*
		EXEC msdb..Usp_send_cdosysmail 'no_replyrackspace.com','Rahul.Chourasiyarackspace.com','NRD FACT Invoice Line Item Dedicated Prepay load job success',''
END TRY
BEGIN CATCH

       --ROLLBACK TRANSACTION

              DECLARE subject nvarchar(max) = 'NRD Fact Load Failure Notification';
              DECLARE body nvarchar(max) = 'Data Transformation Failed during Fact Invoice Line Item Dedicated Prepay Load' 
              + CHAR(10) + CHAR(13) + 'Error Number:  ' + CAST(ERROR_NUMBER() AS nvarchar(50))
              + CHAR(10) + CHAR(13) + 'Error Severity:  ' + CAST(ERROR_SEVERITY() AS nvarchar(50))
              + CHAR(10) + CHAR(13) + 'Error State:  ' + CAST(ERROR_STATE() AS nvarchar(50))
              + CHAR(10) + CHAR(13) + 'Error Procedure:  ' + CAST(ERROR_PROCEDURE() AS nvarchar(100))
              + CHAR(10) + CHAR(13) + 'Error Line:  ' + CAST(ERROR_LINE() AS nvarchar(50))
              + CHAR(10) + CHAR(13) + 'Error Message: ' + ERROR_MESSAGE()
              + CHAR(10) + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) + CHAR(13) + 'This is a system generated mail. DO NOT REPLY  ';
              DECLARE to nvarchar(max) = 'Rahul.Chourasiyarackspace.com';
              DECLARE profile_name sysname = 'Jobs';
              EXEC msdb.sp_send_dbmail profile_name = profile_name,
              recipients = to, subject = subject, body = body;

		THROW    
       END CATCH

END
GO
*/

end;
