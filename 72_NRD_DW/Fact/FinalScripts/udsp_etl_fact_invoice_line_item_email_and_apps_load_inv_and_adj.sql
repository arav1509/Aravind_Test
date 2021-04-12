CREATE or replace PROCEDURE `rax-staging-dev`.stage_three_dw.udsp_etl_fact_invoice_line_item_email_and_apps_load_inv_and_adj()
BEGIN 
/* =============================================
-- Created By :	Rahul Chourasiya
-- Create date: 10.22.2019
-- Description: Loading incremented 2 months of Invoice and Adujstment data from staging tables to Fact_Invoice_Line_Item_Email_And_Apps table
-- =============================================*/
-----------------------------------------------------------------------------------------------------------------

--Inserting Invoice data into Fact table from Stage table
INSERT INTO `rax-datamart-dev`.corporate_dmart.fact_invoice_line_item_email_and_apps
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
  Date_Month_Key
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
 ,cast(Is_Standalone_Fee as bool) as Is_Standalone_Fee
 ,cast(Is_Include_In_Payable as bool) as Is_Include_In_Payable
 ,cast(Is_Prepay as bool) as Is_Prepay
 ,cast(Is_Amortize_Prepay as bool) as Is_Amortize_Prepay
 ,cast(Is_Back_Bill as bool) as  Is_Back_Bill
 ,cast(Is_Fastlane as bool) as  Is_Fastlane
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
 ,cast(Record_Created_Datetime as timestamp) as Record_Created_Datetime
 ,DRC.Record_Source_Key
 ,cast(Record_Updated_Datetime as timestamp) as Record_Updated_Datetime
 ,Source_System_Key
 FROM `rax-staging-dev`.stage_two_dw.stage_fact_invoice_line_item_email_and_apps_invoice Invoice 
 INNER JOIN `rax-staging-dev`.stage_three_dw.dim_record_source DRC 
 ON Invoice.Record_Created_By = DRC.Record_Source_Name;
 
 -------------------------------------------------------------------------------------------------------------------
--Inserting Invoice data into Fact table from Stage table
INSERT INTO `rax-datamart-dev`.corporate_dmart.fact_invoice_line_item_email_and_apps
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
  Date_Month_Key
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
 ,cast(Is_Standalone_Fee as bool) as Is_Standalone_Fee
 ,cast(Is_Include_In_Payable as bool) as Is_Include_In_Payable
 ,cast(Is_Prepay as bool) as Is_Prepay
 ,cast(Is_Amortize_Prepay as bool) as Is_Amortize_Prepay
 ,cast(Is_Back_Bill as bool) as  Is_Back_Bill
 ,cast(Is_Fastlane as bool) as  Is_Fastlane
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
 ,cast(Record_Created_Datetime as timestamp) as Record_Created_Datetime
 ,DRC.Record_Source_Key
 ,cast(Record_Updated_Datetime as timestamp) as Record_Updated_Datetime
 ,Source_System_Key
 FROM `rax-staging-dev`.stage_two_dw.stage_fact_invoice_line_item_email_and_apps_adjustment Adjustment 
 INNER JOIN `rax-staging-dev`.stage_three_dw.dim_record_source DRC 
 ON Adjustment.Record_Created_By = DRC.Record_Source_Name
 ;
-------------------------------------------------------------------------------------------------------------------
/*
		EXEC msdb..Usp_send_cdosysmail 'no_reply@rackspace.com','Rahul.Chourasiya@rackspace.com','NRD FACT Invoice Line Item Email_And_Apps Invoice and Adjustment load Job Success',''
END TRY
BEGIN CATCH

       --ROLLBACK TRANSACTION

              DECLARE @subject nvarchar(max) = 'NRD Fact Load Failure Notification';
              DECLARE @body nvarchar(max) = 'Data Transformation Failed during Fact Invoice Line Item Email_And_Apps Invoice and Adjustment Load' 
              + CHAR(10) + CHAR(13) + 'Error Number:  ' + CAST(ERROR_NUMBER() AS nvarchar(50))
              + CHAR(10) + CHAR(13) + 'Error Severity:  ' + CAST(ERROR_SEVERITY() AS nvarchar(50))
              + CHAR(10) + CHAR(13) + 'Error State:  ' + CAST(ERROR_STATE() AS nvarchar(50))
              + CHAR(10) + CHAR(13) + 'Error Procedure:  ' + CAST(ERROR_PROCEDURE() AS nvarchar(100))
              + CHAR(10) + CHAR(13) + 'Error Line:  ' + CAST(ERROR_LINE() AS nvarchar(50))
              + CHAR(10) + CHAR(13) + 'Error Message: ' + ERROR_MESSAGE()
              + CHAR(10) + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) + CHAR(13) + 'This is a system generated mail. DO NOT REPLY  ';
              DECLARE @to nvarchar(max) = 'Rahul.Chourasiya@rackspace.com';
              DECLARE @profile_name sysname = 'Jobs';
              EXEC msdb.dbo.sp_send_dbmail @profile_name = @profile_name,
              @recipients = @to, @subject = @subject, @body = @body;

		THROW    
       END CATCH

END
GO
*/

end;

