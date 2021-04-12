CREATE OR REPLACE PROCEDURE
  `rax-staging-dev`.stage_three_dw.udsp_etl_load_fact_invoice_event_detail_dedicated_daily_load()
BEGIN
  /*====================================================================================================================================================================================================

Created ON: 08/07/2019    
Created By: anil2912,hari4586
    
Description:  Script to load new AND modified Invoice Event records into Fact_Invoice_Event_Detail_Dedicated.    
    
       Modified By    Date     Description    
1) 

/*==================================================================================================================================================================================================================       
INCREMENTAL DATA FROM NRD STAGE ARE PERSISTENT TABLES ARE PULLED HERE
======================================================================================================================================================================================================================*/
  --****************`rax-datamart-dev`.corporate_dmart.Inc_Stage_Data View Created ****************`
  /*==================================================================================================================================================================================================================       
FACT LOAD BEGINS   : KEYING ALL INCREMENTAL RECORDS INTO TEMP TABLE
======================================================================================================================================================================================================================*/
  --****************`rax-datamart-dev`.corporate_dmart.source_data_all View Created  ****************`
  /*==================================================================================================================================================================================================================       
       FACT LOAD : SEGREGATINIG BAD RECORDS BASED ON KEY COLUMNS AVAILABILITY
======================================================================================================================================================================================================================*/
  -- Loading in Temp Table
  /*==================================================================================================================================================================================================================       
       FACT LOAD : DELETING THE MODIFIED MASTER-UNIQUE-IDS FROM SOURCE AND RE-INSERTING FROM THE TEMP FACT TABLE.
                           BY THIS RECORDS AVAILABLE in INCREMENTAL STAGE WILL BE INSERTED INTO FACT TABLE
======================================================================================================================================================================================================================*/
DELETE
  `rax-datamart-dev`.corporate_dmart.fact_invoice_event_detail_dedicated fact
WHERE
  fact.transaction_key IN (
  SELECT
    stg.transaction_key
  FROM
    `rax-datamart-dev`.corporate_dmart.source_data_all Stg);
INSERT INTO
  `rax-datamart-dev`.corporate_dmart.fact_invoice_event_detail_dedicated ( Date_Month_Key,
    Revenue_Type_Key,
    Account_Key,
    Team_Key,
    Primary_Contact_Key,
    Billing_Contact_Key,
    Product_Key,
    Datacenter_Key,
    Device_Key,
    Item_Key,
    GL_Product_Key,
    GL_Account_Key,
    Glid_Configuration_Key,
    Payment_Term_Key,
    Event_Type_Key,
    Invoice_Key,
    Invoice_Attribute_Key,
    Raw_Start_Month_Key,
    Invoice_Date_Utc_Key,
    Bill_Created_Date_Utc_Key,
    Bill_Created_Time_Utc_Key,
    Bill_Created_Date_Cst_Key,
    Bill_Created_Time_Cst_Key,
    Bill_Start_Date_Utc_Key,
    Bill_End_Date_Utc_Key,
    Prepay_Start_Date_Utc_Key,
    Prepay_End_Date_Utc_Key,
    Event_Created_Date_Utc_Key,
    Event_Created_Time_Utc_Key,
    Event_Created_Date_Cst_Key,
    Event_Created_Time_Cst_Key,
    Event_Start_Date_Utc_Key,
    Event_Start_Time_Utc_Key,
    Event_Start_Date_Cst_Key,
    Event_Start_Time_Cst_Key,
    Event_End_Date_Utc_Key,
    Event_End_Time_Utc_Key,
    Event_End_Date_Cst_Key,
    Event_End_Time_Cst_Key,
    Earned_Start_Date_Utc_Key,
    Earned_Start_Time_Utc_Key,
    Earned_Start_Date_Cst_Key,
    Earned_Start_Time_Cst_Key,
    Earned_End_Date_Utc_Key,
    Earned_End_Time_Utc_Key,
    Earned_End_Date_Cst_Key,
    Earned_End_Time_Cst_Key,
    Currency_Key,
    Transaction_Term,
    Quantity,
    Billing_Days_In_Month,
    Amount_Usd,
    Extended_Amount_Usd,
    Unit_Selling_Price_Usd,
    Amount_Gbp,
    Extended_Amount_Gbp,
    Unit_Selling_Price_Gbp,
    Amount_Local,
    Extended_Amount_Local,
    Unit_Selling_Price_Local,
    Is_Standalone_Fee,
    Is_Back_Bill,
    Is_Fastlane,
    Is_Transaction_Completed,
    Transaction_Key,
    Record_Created_Datetime,
    Record_Created_Source_Key,
    Record_Updated_Datetime,
    Record_Updated_Source_Key,
    Source_System_Key )
SELECT
  Date_Month_Key,
  Revenue_Type_Key,
  Account_Key,
  Team_Key,
  Primary_Contact_Key,
  Billing_Contact_Key,
  Product_Key,
  Datacenter_Key,
  Device_Key,
  Item_Key,
  GL_Product_Key,
  GL_Account_Key,
  Glid_Configuration_Key,
  Payment_Term_Key,
  Event_Type_Key,
  Invoice_Key,
  Invoice_Attribute_Key,
  Raw_Start_Month_Key,
  CAST(Invoice_Date_Utc_Key AS int64) Invoice_Date_Utc_Key,
  CAST(Bill_Created_Date_Utc_Key AS int64) AS Bill_Created_Date_Utc_Key,
  CAST(Bill_Created_Time_Utc_Key AS int64) AS Bill_Created_Time_Utc_Key,
  CAST(Bill_Created_Date_Cst_Key AS int64) AS Bill_Created_Date_Cst_Key,
  CAST(Bill_Created_Time_Cst_Key AS int64) AS Bill_Created_Time_Cst_Key,
  CAST(Bill_Start_Date_Utc_Key AS int64) AS Bill_Start_Date_Utc_Key,
  CAST(Bill_End_Date_Utc_Key AS int64) AS Bill_End_Date_Utc_Key,
  CAST(Prepay_Start_Date_Utc_Key AS int64) AS Prepay_Start_Date_Utc_Key,
  CAST(Prepay_End_Date_Utc_Key AS int64) AS Prepay_End_Date_Utc_Key,
  CAST(Event_Created_Date_Utc_Key AS int64) AS Event_Created_Date_Utc_Key,
  CAST(Event_Created_Time_Utc_Key AS int64) AS Event_Created_Time_Utc_Key,
  CAST(Event_Created_Date_Cst_Key AS int64) AS Event_Created_Date_Cst_Key,
  CAST(Event_Created_Time_Cst_Key AS int64) AS Event_Created_Time_Cst_Key,
  CAST(Event_Start_Date_Utc_Key AS int64) AS Event_Start_Date_Utc_Key,
  CAST(Event_Start_Time_Utc_Key AS int64) AS Event_Start_Time_Utc_Key,
  CAST(Event_Start_Date_Cst_Key AS int64) AS Event_Start_Date_Cst_Key,
  CAST(Event_Start_Time_Cst_Key AS int64) AS Event_Start_Time_Cst_Key,
  CAST(Event_End_Date_Utc_Key AS int64) AS Event_End_Date_Utc_Key,
  CAST(Event_End_Time_Utc_Key AS int64) AS Event_End_Time_Utc_Key,
  CAST(Event_End_Date_Cst_Key AS int64) AS Event_End_Date_Cst_Key,
  CAST(Event_End_Time_Cst_Key AS int64) AS Event_End_Time_Cst_Key,
  CAST(Earned_Start_Date_Utc_Key AS int64) AS Earned_Start_Date_Utc_Key,
  CAST(Earned_Start_Time_Utc_Key AS int64) AS Earned_Start_Time_Utc_Key,
  CAST(Earned_Start_Date_Cst_Key AS int64) AS Earned_Start_Date_Cst_Key,
  CAST(Earned_Start_Time_Cst_Key AS int64) AS Earned_Start_Time_Cst_Key,
  CAST(Earned_End_Date_Utc_Key AS int64) AS Earned_End_Date_Utc_Key,
  CAST(Earned_End_Time_Utc_Key AS int64) AS Earned_End_Time_Utc_Key,
  CAST(Earned_End_Date_Cst_Key AS int64) AS Earned_End_Date_Cst_Key,
  CAST(Earned_End_Time_Cst_Key AS int64) AS Earned_End_Time_Cst_Key,
  CAST(Currency_Key AS int64) AS Currency_Key,
  CAST(Transaction_Term AS NUMERIC ) AS Transaction_Term,
  CAST(Quantity AS NUMERIC ) AS Quantity,
  Billing_Days_In_Month,
  Amount_Usd,
  Extended_Amount_Usd,
  Unit_Selling_Price_Usd,
  Amount_Gbp,
  Extended_Amount_Gbp,
  Unit_Selling_Price_Gbp,
  Amount_Local,
  Extended_Amount_Local,
  Unit_Selling_Price_Local,
  cast(Is_Standalone_Fee as bool ) as Is_Standalone_Fee,
  cast(Is_Back_Bill as bool ) as Is_Back_Bill ,
  cast(Is_Fastlane as bool ) as Is_Fastlane,
  cast(Is_Transaction_Completed as bool ) as Is_Transaction_Completed ,
  Transaction_Key,
  cast(Record_Created_Datetime as timestamp) as Record_Created_Datetime,
  DRS.RECORD_SOURCE_KEY AS Record_Created_Source_Key,
  cast(Record_Updated_Datetime  as timestamp) as Record_Updated_Datetime,
  DRS.RECORD_SOURCE_KEY AS Record_Updated_Source_Key,
  Source_System_Key
FROM
  `rax-datamart-dev`.corporate_dmart.source_data_all SDA -- #Source_Data_All SDA
JOIN
  `rax-staging-dev`.stage_three_dw.dim_record_source drs
ON
  SDA.record_created_by=drs.record_source_name;
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  -- Insertion of records into Fact table ends here
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  /*==================================================================================================================================================================================================================       
       FACT LOAD : RE-DIRECTING BAD RECORDS TO RECYCLE TABLE BEGINS 
======================================================================================================================================================================================================================*/
  -- Load the records having Zero Keys into Recycle Table
  -- TRUNCATE TABLE `rax-staging-dev`.stage_two_dw.recycle_fact_invoice_event_detail_dedicated;
CREATE OR REPLACE TABLE
  `rax-staging-dev`.stage_two_dw.recycle_fact_invoice_event_detail_dedicated AS
SELECT
  Date_Month_Key,
  Revenue_Type_Key,
  Account_Key,
  Team_Key,
  Primary_Contact_Key,
  Billing_Contact_Key,
  Product_Key,
  Datacenter_key,
  Device_Key,
  Item_Key,
  GL_Product_Key,
  GL_Account_Key,
  Glid_Configuration_Key,
  Payment_Term_Key,
  Event_Type_Key,
  Invoice_Key,
  Invoice_Attribute_Key,
  Raw_Start_Month_Key,
  Invoice_Date_Utc_Key,
  Bill_Created_Date_Utc_Key,
  Bill_Created_Time_Utc_Key,
  Bill_Created_Date_Cst_Key,
  Bill_Created_Time_Cst_Key,
  Bill_Start_Date_Utc_Key,
  Bill_End_Date_Utc_Key,
  Prepay_Start_Date_Utc_Key,
  Prepay_End_Date_Utc_Key,
  Event_Created_Date_Utc_Key,
  Event_Created_Time_Utc_Key,
  Event_Created_Date_Cst_Key,
  Event_Created_Time_Cst_Key,
  Event_Start_Date_Utc_Key,
  Event_Start_Time_Utc_Key,
  Event_Start_Date_Cst_Key,
  Event_Start_Time_Cst_Key,
  Event_End_Date_Utc_Key,
  Event_End_Time_Utc_Key,
  Event_End_Date_Cst_Key,
  Event_End_Time_Cst_Key,
  Earned_Start_Date_Utc_Key,
  Earned_Start_Time_Utc_Key,
  Earned_Start_Date_Cst_Key,
  Earned_Start_Time_Cst_Key,
  Earned_End_Date_Utc_Key,
  Earned_End_Time_Utc_Key,
  Earned_End_Date_Cst_Key,
  Earned_End_Time_Cst_Key,
  Currency_Key,
  Transaction_Term,
  Quantity,
  Billing_Days_In_Month,
  Amount_Usd,
  Extended_Amount_Usd,
  Unit_Selling_Price_Usd,
  Amount_Gbp,
  Extended_Amount_Gbp,
  Unit_Selling_Price_Gbp,
  Amount_Local,
  Extended_Amount_Local,
  Unit_Selling_Price_Local,
  Is_Standalone_Fee,
  Is_Back_Bill,
  Is_Fastlane,
  Is_Transaction_Completed,
  Transaction_key,
  Record_Created_Datetime,
  DRS.Record_Source_KEY Record_Created_Source_Key,
  Record_Updated_Datetime,
  DRS.Record_Source_KEY Record_Updated_Source_Key,
  source_system_key
FROM (
  SELECT
    * --into #SourceData_Zero_Key_Records
  FROM
    `rax-datamart-dev`.corporate_dmart.source_data_all
  WHERE
    Account_Key = 0
    OR Team_Key = 0
    OR Primary_Contact_Key = 0
    OR Billing_Contact_Key = 0
    OR Item_Key = 0
    OR GL_Product_Key = 0
    OR GL_Account_Key = 0
    OR Glid_Configuration_Key = 0
    OR Event_Type_Key = 0
    OR Currency_Key = '0'
    OR Invoice_Key = 0
    OR Invoice_Attribute_Key = 0 ) sdzr --#SourceData_Zero_Key_Records sdzr
JOIN
  `rax-staging-dev`.stage_three_dw.dim_record_source DRS
ON
  sdzr.record_created_by=drs.record_source_name;
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  -- INSERTION OF BAD RECORDS INTO RECYCLE TABLE ENDS HERE
  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  /*                    --COMMIT TRANSACTION FACT_DED
       exec msdb..Usp_send_cdosysmail 'no_replyrackspace.com','anil.kumarrackspace.com;harish.gowthamrackspace.com','NRD FACT Event Dedicated load JOB SUCCESS','
       END TRY

       BEGIN CATCH

       --ROLLBACK TRANSACTION

              DECLARE subject nvarchar(max) = 'NRD Fact Load Failure Notification';
              DECLARE body nvarchar(max) = 'Data Transformation Failed during Fact Table Load' 
              + CHAR(10) + CHAR(13) + 'Error Number:  ' + CAST(ERROR_NUMBER() AS nvarchar(50))
              + CHAR(10) + CHAR(13) + 'Error Severity:  ' + CAST(ERROR_SEVERITY() AS nvarchar(50))
              + CHAR(10) + CHAR(13) + 'Error State:  ' + CAST(ERROR_STATE() AS nvarchar(50))
              + CHAR(10) + CHAR(13) + 'Error Procedure:  ' + CAST(ERROR_PROCEDURE() AS nvarchar(100))
              + CHAR(10) + CHAR(13) + 'Error Line:  ' + CAST(ERROR_LINE() AS nvarchar(50))
              + CHAR(10) + CHAR(13) + 'Error Message: ' + ERROR_MESSAGE()
              + CHAR(10) + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) + CHAR(13) + 'This is a system generated mail. DO NOT REPLY  ';
              DECLARE to nvarchar(max) = 'anil.kumarrackspace.com;harish.gowthamrackspace.com;Rahul.Chourasiyarackspace.com';
              DECLARE profile_name sysname = 'Jobs';
              EXEC msdb.dbo.sp_send_dbmail profile_name = profile_name,
              recipients = to, subject = subject, body = body;

		THROW    
       END CATCH

END

GO
*/
END
  ;