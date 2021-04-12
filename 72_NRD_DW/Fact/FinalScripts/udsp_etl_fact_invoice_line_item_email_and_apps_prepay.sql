CREATE or replace procedure `rax-staging-dev`.stage_three_dw.udsp_etl_fact_invoice_line_item_email_and_apps_prepay()
BEGIN
/* =============================================
-- Created By :	Rahul Chourasiya
-- Create date: 10.24.2019
-- Description: Deletes and reloads 2 months of prepay records from Fact_Invoice_Event_Detail_Email_And_Apps
-- =============================================*/
-----------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
DECLARE GL_BeginDate int64;
DECLARE GL_EndDate int64;
DECLARE VRNUM  INT64;
DECLARE MAX_ROW  INT64;
DECLARE FIN_START_TIME_MONTH INT64;
DECLARE FIN_END_TIME_MONTH INT64;
DECLARE BILL_NUM  string;


DECLARE Fin_Start_Date INT64;
DECLARE Fin_End_Date INT64;
DECLARE Fin_Start_Date_Key INT64;
DECLARE Fin_End_Date_Key INT64;
DECLARE RunDate  int64;
DECLARE Execution_Time DateTime;
DECLARE Looping INT64;

DECLARE VDate  int64;
DECLARE Prior_Date datetime;
DECLARE Time_Key  int64;


SET GL_BeginDate = `rax-staging-dev`.bq_functions.udf_yearmonth_nohyphen(date_sub(current_date(), interval 2 Month));
SET GL_EndDate   = `rax-staging-dev`.bq_functions.udf_yearmonth_nohyphen(current_date());


--- `rax-datamart-dev`.corporate_dmart.Prepays_ALL_Email_And_Apps    `rax-datamart-dev`.corporate_dmart.Prepays_ALL_Email_And_Apps 



SELECT BILL_NUMBER,--INTO #Row_Bill_Id
       Prepay_Start_Date_Utc_Key,
	   Prepay_End_Date_Utc_Key,
	   ROW_NUMBER() OVER (ORDER BY BILL_NUMBER, Prepay_Start_Date_Utc_Key, Prepay_End_Date_Utc_Key) AS RNUM
FROM (
		SELECT DISTINCT BILL_NUMBER,  ---INTO #Distinct_Bill_Id
			   Prepay_Start_Date_Utc_Key,
			   Prepay_End_Date_Utc_Key
		FROM `rax-datamart-dev`.corporate_dmart.Prepays_ALL_Email_And_Apps 
		ORDER BY BILL_NUMBER,Prepay_Start_Date_Utc_Key,Prepay_End_Date_Utc_Key ASC
)--#Distinct_Bill_Id
;
----------------------------------------------------------------------------------------------------------------------
--DECLARING AND DEFINING VARIABLES FOR PREPAY LOOP AND WILL EXECUTE LOOP FOR EACH BILL WITH MAX OF BILL NUMBER COUNT
----------------------------------------------------------------------------------------------------------------------


SET VRNUM    = 1;
SET MAX_ROW = (
					SELECT MAX(RNUM) FROM 
          (
					SELECT BILL_NUMBER,--INTO #Row_Bill_Id
						   Prepay_Start_Date_Utc_Key,
						   Prepay_End_Date_Utc_Key,
						   ROW_NUMBER() OVER (ORDER BY BILL_NUMBER, Prepay_Start_Date_Utc_Key, Prepay_End_Date_Utc_Key) AS RNUM
					FROM (
							   SELECT DISTINCT BILL_NUMBER,  ---INTO #Distinct_Bill_Id
								   Prepay_Start_Date_Utc_Key,
								   Prepay_End_Date_Utc_Key
							   FROM `rax-datamart-dev`.corporate_dmart.Prepays_ALL_Email_And_Apps 
							   ORDER BY BILL_NUMBER,Prepay_Start_Date_Utc_Key,Prepay_End_Date_Utc_Key ASC
					     )
         )
);



WHILE  VRNUM <= MAX_ROW  --- Outer loop
do
----------------------------------------------------------------------------------------------------------------------
--ASSINGING ONE BILL NUMBER VALUES IN VARIABLE AT A TIME
----------------------------------------------------------------------------------------------------------------------

SELECT  (BILL_NUM, FIN_START_TIME_MONTH, FIN_END_TIME_MONTH) =
	( 
	select AS STRUCT BILL_NUMBER, Prepay_Start_Date_Utc_Key,  Prepay_End_Date_Utc_Key  
	FROM (
					SELECT BILL_NUMBER,--INTO #Row_Bill_Id
						   Prepay_Start_Date_Utc_Key,
						   Prepay_End_Date_Utc_Key,
						   ROW_NUMBER() OVER (ORDER BY BILL_NUMBER, Prepay_Start_Date_Utc_Key, Prepay_End_Date_Utc_Key) AS RNUM
					FROM (
							   SELECT DISTINCT BILL_NUMBER,  ---INTO #Distinct_Bill_Id
								   Prepay_Start_Date_Utc_Key,
								   Prepay_End_Date_Utc_Key
							   FROM `rax-datamart-dev`.corporate_dmart.Prepays_ALL_Email_And_Apps 
							   ORDER BY BILL_NUMBER,Prepay_Start_Date_Utc_Key,Prepay_End_Date_Utc_Key ASC
					     )
		)
--#Row_Bill_Id 
	WHERE RNUM = VRNUM
  	)
	;



SET Fin_Start_Date = (
					SELECT MIN(Prepay_Start_Date_Utc_Key) 
					FROM `rax-datamart-dev`.corporate_dmart.Prepays_ALL_Email_And_Apps  A 
					Where BILL_NUMBER = BILL_NUM and Prepay_Start_Date_Utc_Key = FIN_START_TIME_MONTH and Prepay_End_Date_Utc_Key = FIN_END_TIME_MONTH 
					);
SET Fin_End_Date   = (
					SELECT MAX(Prepay_End_Date_Utc_Key) 
					FROM `rax-datamart-dev`.corporate_dmart.Prepays_ALL_Email_And_Apps A 
					Where BILL_NUMBER = BILL_NUM and Prepay_Start_Date_Utc_Key = FIN_START_TIME_MONTH and Prepay_End_Date_Utc_Key = FIN_END_TIME_MONTH
					);
SET RunDate        = Fin_Start_Date;

SET Execution_Time = current_datetime();
SET Looping        = RunDate;


SET Fin_Start_Date_Key = cast((SELECT substr(cast(Prepay_Start_Date_Utc_Key as string),1,6) FROM `rax-datamart-dev`.corporate_dmart.Prepays_ALL_Email_And_Apps) as int64);
SET Fin_End_Date_Key   = cast((SELECT substr( cast(Prepay_End_Date_Utc_Key as string) ,1,6)FROM `rax-datamart-dev`.corporate_dmart.Prepays_ALL_Email_And_Apps) as int64);


----------------------------------------------------------------------------------------------------------------------
--LOOPING TO GENERATE PREPAY DATA FOR UNIT_MEASURE_OF_CODE = MONTH OR DAY USING BILL NUMBER, Prepay_Start_Date_Utc_Key AND Prepay_End_Date_Utc_Key
----------------------------------------------------------------------------------------------------------------------
WHILE  RunDate <= Fin_End_Date -- Inner Loop

do

-------------------------------------------------------------------------------------------
SET VDate= RunDate;
SET Time_Key =VDate;
---------------------------------------------------------------------------------------------
--PRINT ' Running for Current EOM ' +  cast(Time_Key as varchar)
---------------------------------------------------------------------------------------------
--INSERTING PREPAY DATA INTO STAGE TABLE
---------------------------------------------------------------------------------------------


INSERT INTO `rax-staging-dev`.stage_two_dw.stage_fact_invoice_line_item_email_and_apps_prepay
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
		,Quantityx
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
		,Is_Include_In_Payable
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
		,Amortized_Local
		,Amortized_USD
		,Amortized_GBP
		,Amortized_Invoice_Date
		,Time_Month_Key
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
		,cast(Is_Standalone_Fee as bool) as Is_Standalone_Fee
		,cast(Is_Include_In_Payable as bool) as Is_Include_In_Payable
		,cast(Is_Prepay as bool) as  Is_Prepay
		,cast(Is_Amortize_Prepay as bool) as Is_Amortize_Prepay
		,cast(Is_Back_Bill as bool) as  Is_Back_Bill
		,cast(Is_Fastlane as bool) as Is_Fastlane
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
		,Amortized_Local
		,Amortized_USD
		,Amortized_GBP
		,CAST(DT.Time_Full_Date as datetime)				     AS Amortized_Invoice_Date
		,Time_Key											     AS Time_Month_Key
		,'udsp_etl_Fact_Invoice_Line_Item_Email_And_Apps_Prepay' As Record_Created_By
		,cast(Execution_Time as datetime)				     AS Record_Created_Datetime
		,'udsp_etl_Fact_Invoice_Line_Item_Email_And_Apps_Prepay' AS Record_Updated_By
		,cast(Execution_Time as datetime)				     AS Record_Updated_Datetime
		,Source_System_Key
FROM   
	`rax-datamart-dev`.corporate_dmart.Prepays_ALL_Email_And_Apps A
INNER JOIN
	`rax-staging-dev`.stage_three_dw.dim_time DT 
ON (Time_Key) =DT.Time_Key
AND Time_Last_Day_Month_Flag = 1
WHERE  
    BILL_NUMBER = BILL_NUM 
AND Prepay_Start_Date_Utc_Key = FIN_START_TIME_MONTH 
AND Prepay_End_Date_Utc_Key   = FIN_END_TIME_MONTH
AND	Time_Key >= Fin_Start_Date
AND Time_Key < Fin_End_Date
;



		SET RunDate = date_add(date(RunDate), interval 1 month );
	---------------------------------------------------------------------------------------------------------------
	END while;--INNER LOOP ENDS HERE



	SET VRNUM = VRNUM+1;

END while; --Outer loop


--OUTER WHILE LOOP ENDS HERE 
---------------------------------------------------------------------------------------------------------------
--DELETING ALREADY LOADED DATA FROM FACT TABLE
---------------------------------------------------------------------------------------------------------------
DELETE from `rax-datamart-dev`.corporate_dmart.fact_invoice_line_item_email_and_apps
WHERE Is_Amortize_Prepay = 1
AND Date_Month_Key >= Fin_Start_Date_Key
AND Date_Month_Key <= Fin_End_Date_Key
;
-----------------------------------------------------------------------------------------------------------------
--INSERTING INVOICE DATA INTO FACT TABLE FROM STAGE TABLE

iNSERT INTO `rax-datamart-dev`.corporate_dmart.fact_invoice_line_item_email_and_apps
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
		   ,Is_Include_In_Payable
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
  cast(SUBSTRING(CAST(Time_Month_Key AS string),1,6) as int64) As Date_Month_Key
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
 ,`rax-staging-dev`.bq_functions.udf_yearmonth_nohyphen(amortized_invoice_date) As Invoice_Date_Utc_Key
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
 ,Quantityx
 ,Billing_Days_In_Month
 ,Amortized_USD AS Amount_Usd
 ,Amortized_USD AS Extended_Amount_Usd
 ,Amortized_USD AS Unit_Selling_Price_Usd
 ,Amortized_GBP AS Amount_Gbp
 ,Amortized_GBP AS Extended_Amount_Gbp
 ,Amortized_GBP AS Unit_Selling_Price_Gbp
 ,Amortized_Local AS Amount_Local
 ,Amortized_Local AS Extended_Amount_Local
 ,Amortized_Local AS Unit_Selling_Price_Local
 ,cast(Is_Standalone_Fee as int64) as Is_Standalone_Fee
 ,cast(Is_Include_In_Payable as int64) as Is_Include_In_Payable
 ,cast(Is_Prepay as int64) as Is_Prepay
 ,cast(Is_Amortize_Prepay as int64) as Is_Amortize_Prepay
 ,cast(Is_Back_Bill as int64) as Is_Back_Bill
 ,cast(Is_Fastlane as int64) as Is_Fastlane
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
 FROM `rax-staging-dev`.stage_two_dw.stage_fact_invoice_line_item_email_and_apps_prepay Prepay
 INNER JOIN `rax-staging-dev`.stage_three_dw.dim_record_source DRC 
 ON Prepay.Record_Created_By = DRC.Record_Source_Name
 WHERE
    Date_Month_Key >= Fin_Start_Date_Key
AND Date_Month_Key <= Fin_End_Date_Key
;
-----------------------------------------------------------------------------------------------------------------

/*
		EXEC msdb..Usp_send_cdosysmail 'no_replyrackspace.com','Rahul.Chourasiyarackspace.com','NRD FACT Invoice Line Item Email_And_Apps Prepay load job success',''
END TRY
BEGIN CATCH

       --ROLLBACK TRANSACTION

              DECLARE subject nvarchar(max) = 'NRD Fact Load Failure Notification';
              DECLARE body nvarchar(max) = 'Data Transformation Failed during Fact Invoice Line Item Email_And_Apps Prepay Load' 
              + CHAR(10) + CHAR(13) + 'Error Number:  ' + CAST(ERROR_NUMBER() AS nvarchar(50))
              + CHAR(10) + CHAR(13) + 'Error Severity:  ' + CAST(ERROR_SEVERITY() AS nvarchar(50))
              + CHAR(10) + CHAR(13) + 'Error State:  ' + CAST(ERROR_STATE() AS nvarchar(50))
              + CHAR(10) + CHAR(13) + 'Error Procedure:  ' + CAST(ERROR_PROCEDURE() AS nvarchar(100))
              + CHAR(10) + CHAR(13) + 'Error Line:  ' + CAST(ERROR_LINE() AS nvarchar(50))
              + CHAR(10) + CHAR(13) + 'Error Message: ' + ERROR_MESSAGE()
              + CHAR(10) + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) + CHAR(13) + 'This is a system generated mail. DO NOT REPLY  ';
              DECLARE to nvarchar(max) = 'Rahul.Chourasiyarackspace.com';
              DECLARE profile_name sysname = 'Jobs';
              EXEC msdb.dbo.sp_send_dbmail profile_name = profile_name,
              recipients = to, subject = subject, body = body;

		THROW    
       END CATCH

END
GO
*/
end;
