CREATE or replace procedure `rax-staging-dev`.stage_three_dw.udsp_etl_load_fact_invoice_event_detail_cloud_daily_load() 
/*====================================================================================================================================================================================================

Created On: 08/07/2019    
Created By: anil2912
    
Description:  Script to load new and modified Invoice Event records into Fact_Invoice_Event_Detail_Dedicated.    
    
Modified By    Date     Description    
1) 

====================================================================================================================================================================================================*/
BEGIN

 


--`rax-datamart-dev`.corporate_dmart.Tmp_Inc_Stage_Data view created


--`rax-datamart-dev`.corporate_dmart.Tmp_FACT_TEMP_DATA view created

/*==================================================================================================================================================================================================================       
FACT LOAD : SEGREGATINIG BAD RECORDS BASED ON KEY COLUMNS AVAILABILITY
======================================================================================================================================================================================================================*/              

  
/*==================================================================================================================================================================================================================       
FACT LOAD : DELETING THE MODIFIED MASTER-UNIQUE-IDS FROM SOURCE AND RE-INSERTING FROM THE TEMP FACT TABLE.
BY THIS RECORDS AVAILABLE in INCREMENTAL STAGE WILL BE INSERTED INTO FACT TABLE
======================================================================================================================================================================================================================*/              
     
DELETE `rax-datamart-dev`.corporate_dmart.fact_invoice_event_detail_cloud fact
where fact.transaction_key in( select stg.transaction_key
FROM `rax-datamart-dev`.corporate_dmart.Tmp_FACT_TEMP_DATA Stg 
);
 INSERT INTO `rax-datamart-dev`.corporate_dmart.fact_invoice_event_detail_cloud
     (Date_Month_Key
     ,Revenue_Type_Key
     ,Account_Key
     ,Team_Key
     ,Primary_Contact_Key
     ,Billing_Contact_Key
     ,Product_Key
     ,Item_Key
     ,GL_Product_Key
     ,GL_Account_Key
     ,Glid_Configuration_Key
     ,Event_Type_Key
     ,Invoice_Key
     ,Invoice_Attribute_Key
     ,Invoice_Date_Utc_Key
     ,Bill_Created_Date_Utc_Key
     ,Bill_Created_Time_Utc_Key
     ,Bill_Created_Date_Cst_Key
     ,Bill_Created_Time_Cst_Key
     ,Bill_Start_Date_Utc_Key
     ,Bill_End_Date_Utc_Key
     ,Event_Created_Date_Utc_Key
     ,Event_Created_Time_Utc_Key
     ,Event_Created_Date_Cst_Key
     ,Event_Created_Time_Cst_Key
     ,Event_Start_Date_Utc_Key
     ,Event_Start_Time_Utc_Key
     ,Event_Start_Date_Cst_Key
     ,Event_Start_Time_Cst_Key
     ,Event_End_Date_Utc_Key
     ,Event_End_Time_Utc_Key
     ,Event_End_Date_Cst_Key
     ,Event_End_Time_Cst_Key
     ,Earned_Start_Date_Utc_Key
     ,Earned_Start_Time_Utc_Key
     ,Earned_Start_Date_Cst_Key
     ,Earned_Start_Time_Cst_Key
     ,Earned_End_Date_Utc_Key
     ,Earned_End_Time_Utc_Key
     ,Earned_End_Date_Cst_Key
     ,Earned_End_Time_Cst_Key
     ,Currency_Key
     ,Transaction_Term
     ,Quantity
     ,Billing_Days_In_Month
     ,Amount_Usd
     ,Unit_Selling_Price_Usd
     ,Amount_Gbp
     ,Unit_Selling_Price_Gbp
     ,Amount_Local
     ,Unit_Selling_Price_Local
     ,Is_Standalone_Fee
     ,Is_Back_Bill
     ,Is_Fastlane
     ,Is_Transaction_Completed
     ,Transaction_Key
     ,Record_Created_Datetime
     ,Record_Created_Source_Key
     ,Record_Updated_Datetime
     ,Record_Updated_Source_Key
     ,Source_System_Key)
SELECT
     Date_Month_Key
     ,Revenue_Type_Key
     ,Account_Key
     ,Team_Key
     ,Primary_Contact_Key
     ,Billing_Contact_Key
     ,Product_Key
     ,Item_Key
     ,GL_Product_Key
     ,GL_Account_Key
     ,Glid_Configuration_Key
     ,Event_Type_Key
     ,Invoice_Key
     ,Invoice_Attribute_Key
     ,Invoice_Date_Utc_Key
     ,Bill_Created_Date_Utc_Key
     ,Bill_Created_Time_Utc_Key
     ,Bill_Created_Date_Cst_Key
     ,Bill_Created_Time_Cst_Key
     ,Bill_Start_Date_Utc_Key
     ,Bill_End_Date_Utc_Key
     ,Event_Created_Date_Utc_Key
     ,Event_Created_Time_Utc_Key
     ,Event_Created_Date_Cst_Key
     ,Event_Created_Time_Cst_Key
     ,Event_Start_Date_Utc_Key
     ,Event_Start_Time_Utc_Key
     ,Event_Start_Date_Cst_Key
     ,Event_Start_Time_Cst_Key
     ,Event_End_Date_Utc_Key
     ,Event_End_Time_Utc_Key
     ,Event_End_Date_Cst_Key
     ,Event_End_Time_Cst_Key
     ,Earned_Start_Date_Utc_Key
     ,Earned_Start_Time_Utc_Key
     ,Earned_Start_Date_Cst_Key
     ,Earned_Start_Time_Cst_Key
     ,Earned_End_Date_Utc_Key
     ,Earned_End_Time_Utc_Key
     ,Earned_End_Date_Cst_Key
     ,Earned_End_Time_Cst_Key
     ,Currency_Key
     ,0 AS Transaction_Term
     ,cast(Quantity as numeric) as Quantity
     ,Billing_Days_In_Month
     ,Total_Amount_Usd
     ,Unit_Selling_Price_Usd
     ,Total_Amount_Gbp
     ,Unit_Selling_Price_Gbp
     ,Total_Amount_Local
     ,Unit_Selling_Price_Local
     ,CAST(Is_Standalone_Fee AS bool) AS Is_Standalone_Fee
     ,CAST(Is_Back_Bill AS bool) AS Is_Back_Bill
     ,CAST(Is_Fastlane AS bool) AS Is_Fastlane
     ,CAST(Is_Transaction_Completed AS bool) AS Is_Transaction_Completed
     ,Transaction_key
     ,cast(Record_Created_Datetime as timestamp) as  Record_Created_Datetime
     ,Record_Created_By
     ,cast(Record_Updated_Datetime as timestamp) as  Record_Updated_Datetime
     ,Record_Updated_By
     ,Source_System_Key
FROM `rax-datamart-dev`.corporate_dmart.Tmp_FACT_TEMP_DATA;

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Insertion of records into Fact table ends here 
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/*==================================================================================================================================================================================================================                
FACT LOAD : RE-DIRECTING BAD RECORDS TO RECYCLE TABLE BEGINS 
======================================================================================================================================================================================================================*/                                
                

create or replace table `rax-staging-dev`.stage_two_dw.recycle_fact_invoice_event_detail_cloud  
 as
SELECT
     Date_Month_Key
     ,Revenue_Type_Key
     ,Account_Key
     ,Team_Key
     ,Primary_Contact_Key
     ,Billing_Contact_Key
     ,Product_Key
     ,Item_Key
     ,GL_Product_Key
     ,GL_Account_Key
     ,Glid_Configuration_Key
     ,Event_Type_Key
     ,Invoice_Key
     ,Invoice_Attribute_Key
     ,Invoice_Date_Utc_Key
     ,Bill_Created_Date_Utc_Key
     ,Bill_Created_Time_Utc_Key
     ,Bill_Created_Date_Cst_Key
     ,Bill_Created_Time_Cst_Key
     ,Bill_Start_Date_Utc_Key
     ,Bill_End_Date_Utc_Key
     ,Event_Created_Date_Utc_Key
     ,Event_Created_Time_Utc_Key
     ,Event_Created_Date_Cst_Key
     ,Event_Created_Time_Cst_Key
     ,Event_Start_Date_Utc_Key
     ,Event_Start_Time_Utc_Key
     ,Event_Start_Date_Cst_Key
     ,Event_Start_Time_Cst_Key
     ,Event_End_Date_Utc_Key
     ,Event_End_Time_Utc_Key
     ,Event_End_Date_Cst_Key
     ,Event_End_Time_Cst_Key
     ,Earned_Start_Date_Utc_Key
     ,Earned_Start_Time_Utc_Key
     ,Earned_Start_Date_Cst_Key
     ,Earned_Start_Time_Cst_Key
     ,Earned_End_Date_Utc_Key
     ,Earned_End_Time_Utc_Key
     ,Earned_End_Date_Cst_Key
     ,Earned_End_Time_Cst_Key
     ,Currency_Key
     ,0 AS Transaction_Term
     ,Quantity
     ,Billing_Days_In_Month
     ,Total_Amount_Usd
     ,Unit_Selling_Price_Usd
     ,Total_Amount_Gbp
     ,Unit_Selling_Price_Gbp
     ,Total_Amount_Local
     ,Unit_Selling_Price_Local
     ,Is_Standalone_Fee
     ,Is_Back_Bill
     ,Is_Fastlane
     ,Is_Transaction_Completed
     ,Transaction_Key
     ,current_datetime() AS Record_Created_Datetime
     ,'ETL' AS Record_Created_By
     ,current_datetime() AS Record_Updated_Datetime
     ,'ETL' AS Record_Updated_By
     ,Source_System_Key
 FROM (
 SELECT * ---INTO #Fact_Temp_Data_Zero_Key_Records
                FROM `rax-datamart-dev`.corporate_dmart.Tmp_FACT_TEMP_DATA
WHERE ( Account_Key = 0
                OR Team_Key = 0
                OR Primary_Contact_Key = 0
                OR Billing_Contact_Key = 0
                OR Invoice_Key = 0
		OR Invoice_Attribute_Key = 0
                OR Item_Key = 0
                OR GL_Product_Key = 0
                OR GL_Account_Key = 0
                OR Glid_Configuration_Key = 0
                OR Event_Type_Key = 0
                OR Currency_Key = 0
                )
 
 ) --#Fact_Temp_Data_Zero_Key_Records
 ;

/*
END TRY

BEGIN CATCH


    DECLARE subject nVARCHAR(max) = 'NRD Fact Load Failure Notification';
    DECLARE body nVARCHAR(max) = 'Data Transformation Failed during Fact Table Load' 
    + CHAR(10) + CHAR(13) + 'Error Number:  ' + CAST(ERROR_NUMBER() AS nVARCHAR(50))
    + CHAR(10) + CHAR(13) + 'Error Severity:  ' + CAST(ERROR_SEVERITY() AS nVARCHAR(50))
    + CHAR(10) + CHAR(13) + 'Error State:  ' + CAST(ERROR_STATE() AS nVARCHAR(50))
    + CHAR(10) + CHAR(13) + 'Error Procedure:  ' + CAST(ERROR_PROCEDURE() AS nVARCHAR(100))
    + CHAR(10) + CHAR(13) + 'Error Line:  ' + CAST(ERROR_LINE() AS nVARCHAR(50))
    + CHAR(10) + CHAR(13) + 'Error Message: ' + ERROR_MESSAGE()
    + CHAR(10) + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) + CHAR(13) + 'This is a system generated mail. DO NOT REPLY  ';
    DECLARE to nVARCHAR(max) = 'anil.kumarrackspace.com';
    DECLARE profile_name sysname = 'Jobs';
    EXEC msdb.sp_send_dbmail profile_name = profile_name,
    recipients = to, subject = subject, body = body;

                    THROW    
END CATCH


*/
END;
