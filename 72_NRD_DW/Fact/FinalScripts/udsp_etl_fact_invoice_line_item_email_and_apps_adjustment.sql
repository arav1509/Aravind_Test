CREATE  OR REPLACE PROCEDURE `rax-staging-dev`.stage_three_dw.udsp_etl_fact_invoice_line_item_email_and_apps_adjustment()


BEGIN 

create or replace table `rax-staging-dev`.stage_two_dw.stage_fact_invoice_line_item_email_and_apps_adjustment as 
SELECT Fact.Date_Month_Key
      ,Rev.Revenue_Type_Key
      ,Fact.Account_Key
      ,Fact.Team_Key
      ,Fact.Primary_Contact_Key
      ,Fact.Billing_Contact_Key
      ,Fact.Product_Key
      ,Fact.Datacenter_Key
      ,-1 as Device_Key
      ,Fact.Item_Key
      ,Fact.GL_Product_Key
      ,Fact.GL_Account_Key
      ,Fact.Glid_Configuration_Key
      ,-1 as Payment_Term_Key
      ,Fact.Event_Type_Key
      ,Fact.Invoice_Key
      ,Fact.Invoice_Attribute_Key
      ,-1 as Raw_Start_Month_Key
      ,Fact.Invoice_Date_Utc_Key
      ,Fact.Bill_Created_Date_Utc_Key
      ,Fact.Bill_Created_Time_Utc_Key
      ,Fact.Bill_Start_Date_Utc_Key
      ,Fact.Bill_End_Date_Utc_Key
      ,Fact.Prepay_Start_Date_Utc_Key
      ,Fact.Prepay_End_Date_Utc_Key
      ,Currency_Key
	  ,Fact.Transaction_Term
      ,sum(Quantity) as Quantity
      ,Billing_Days_In_Month
      ,sum(Amount_Usd) as Amount_Usd
      ,sum(Amount_Usd) as Extended_Amount_Usd
      ,sum(Unit_Selling_Price_Usd) as Unit_Selling_Price_Usd
      ,sum(Amount_Gbp) as Amount_Gbp
      ,sum(Amount_Gbp) as Extended_Amount_Gbp
      ,sum(Unit_Selling_Price_Gbp) as Unit_Selling_Price_Gbp
      ,sum(Amount_Local) as Amount_Local
      ,sum(Amount_Local) as Extended_Amount_Local
      ,sum(Unit_Selling_Price_Local) as Unit_Selling_Price_Local
      ,Is_Standalone_Fee
	  ,1 As Is_Include_In_Payable
	  ,0 As Is_Prepay
	  ,0 As Is_Amortize_Prepay
      ,Is_Back_Bill
      ,Is_Fastlane
	  ,Fact.Bill_Created_Date_Cst_Key
      ,Fact.Bill_Created_Time_Cst_Key
      ,min(cast(TIMESTAMP_ADD(TIMESTAMP(eventcreatedutc.Time_Full_Date), INTERVAL cast(Event_Created_Time_Utc_Key as int64) second)  as datetime)) as Event_Min_Created_Date_Time_Utc
      ,min(cast(TIMESTAMP_ADD(TIMESTAMP(eventcreatedcst.Time_Full_Date), INTERVAL cast(Event_Created_Time_Cst_Key as int64) second)  as datetime)) as Event_Min_Created_Date_Time_Cst
      ,max(cast(TIMESTAMP_ADD(TIMESTAMP(eventcreatedutc.Time_Full_Date), INTERVAL cast(Event_Created_Time_Utc_Key as int64) second)  as datetime)) as Event_Max_Created_Date_Time_Utc
      ,max(cast(TIMESTAMP_ADD(TIMESTAMP(eventcreatedcst.Time_Full_Date), INTERVAL cast(Event_Created_Time_Cst_Key as int64) second)  as datetime)) as Event_Max_Created_Date_Time_Cst
      ,min(cast(TIMESTAMP_ADD(TIMESTAMP(earnedstartutc.Time_Full_Date	),  INTERVAL cast(Earned_Start_Time_Utc_Key as int64) second)  as datetime)) as Earned_Min_Start_Date_Time_Utc
      ,min(cast(TIMESTAMP_ADD(TIMESTAMP(earnedstartcst.Time_Full_Date	),  INTERVAL cast(Earned_Start_Time_Cst_Key as int64) second)  as datetime)) as Earned_Min_Start_Date_Time_Cst
      ,max(cast(TIMESTAMP_ADD(TIMESTAMP(earnedstartutc.Time_Full_Date	),  INTERVAL cast(Earned_Start_Time_Utc_Key as int64) second)  as datetime)) as Earned_Max_Start_Date_Time_Utc
      ,max(cast(TIMESTAMP_ADD(TIMESTAMP(earnedstartcst.Time_Full_Date	),  INTERVAL cast(Earned_Start_Time_Cst_Key as int64) second)  as datetime)) as Earned_Max_Start_Date_Time_Cst  
	  ,Source_System_Key
	  ,0 As Event_Min_Created_Date_Utc_Key
	  ,0 As Event_Min_Created_Time_Utc_Key
	  ,0 As Event_Max_Created_Date_Utc_Key
	  ,0 As Event_Max_Created_Time_Utc_Key
	  ,0 As Event_Min_Created_Date_Cst_Key
	  ,0 As Event_Min_Created_Time_Cst_Key
	  ,0 As Event_Max_Created_Date_Cst_Key
	  ,0 As Event_Max_Created_Time_Cst_Key
	  ,0 As Earned_Min_Start_Date_Utc_Key
	  ,0 As Earned_Min_Start_Time_Utc_Key
	  ,0 As Earned_Max_Start_Date_Utc_Key
	  ,0 As Earned_Max_Start_Time_Utc_Key
	  ,0 As Earned_Min_Start_Date_Cst_Key
	  ,0 As Earned_Min_Start_Time_Cst_Key
	  ,0 As Earned_Max_Start_Date_Cst_Key
	  ,0 As Earned_Max_Start_Time_Cst_Key
	  ,'udsp_etl_Fact_Invoice_Line_Item_Email_And_Apps_Adjustment' as Record_Created_By
	  ,current_datetime() as Record_Created_Datetime
	  ,'udsp_etl_Fact_Invoice_Line_Item_Email_And_Apps_Adjustment' as Record_Updated_By
	  ,current_datetime() as Record_Updated_Datetime
	  
  from 		`rax-datamart-dev`.corporate_dmart.fact_invoice_event_detail_email_and_apps fact 
  left join `rax-staging-dev`.stage_three_dw.dim_time eventcreatedutc  on fact.event_created_date_utc_key=eventcreatedutc.time_key
  left join `rax-staging-dev`.stage_three_dw.dim_time eventcreatedcst  on fact.event_created_date_cst_key=eventcreatedcst.time_key
  left join `rax-staging-dev`.stage_three_dw.dim_time earnedstartutc  on fact.earned_start_date_utc_key=earnedstartutc.time_key
  left join `rax-staging-dev`.stage_three_dw.dim_time earnedstartcst  on fact.earned_start_date_cst_key=earnedstartcst.time_key
  left join `rax-datamart-dev`.corporate_dmart.dim_invoice inv  on fact.invoice_key=inv.invoice_key
  left join `rax-staging-dev`.stage_three_dw.dim_revenue_type rev  on 'adjustment' = lower(rev.revenue_type_name)
  inner join (
  SELECT DISTINCT --INTO  #Modified_Invoices_Adj
    Bill_No As Bill_Number 
	FROM  `rax-staging-dev`.stage_one.raw_email_apps_inv_event_detail 
	WHERE
    UPPER(Bill_No) LIKE '%A1%'
  ) modadj--#modified_invoices_adj modadj  
  on inv.bill_number = modadj.bill_number 
  Group By Fact.Date_Month_Key
      ,Rev.Revenue_Type_Key
      ,Fact.Account_Key
      ,Fact.Team_Key
      ,Fact.Primary_Contact_Key
      ,Fact.Billing_Contact_Key
      ,Fact.Product_Key
      ,Fact.Datacenter_Key
      ,Fact.Item_Key
      ,Fact.GL_Product_Key
      ,Fact.GL_Account_Key
      ,Fact.Glid_Configuration_Key
      ,Fact.Event_Type_Key
      ,Fact.Invoice_Key
      ,Fact.Invoice_Attribute_Key
      ,Fact.Invoice_Date_Utc_Key
      ,Fact.Bill_Created_Date_Utc_Key
      ,Fact.Bill_Created_Time_Utc_Key
      ,Fact.Bill_Start_Date_Utc_Key
      ,Fact.Bill_End_Date_Utc_Key
      ,Fact.Prepay_Start_Date_Utc_Key
      ,Fact.Prepay_End_Date_Utc_Key
      ,Fact.Currency_Key
	  ,Fact.Transaction_Term
      ,Fact.Billing_Days_In_Month
      ,Fact.Is_Standalone_Fee
      ,Fact.Is_Back_Bill
      ,Fact.Is_Fastlane
	  ,Fact.Bill_Created_Date_Cst_Key
      ,Fact.Bill_Created_Time_Cst_Key
      ,Fact.Source_System_Key  
;
-------------------------------------------------------------------------------------------------------------------
UPDATE `rax-staging-dev`.stage_two_dw.stage_fact_invoice_line_item_email_and_apps_adjustment adjustment
Set 
 Adjustment.Event_Min_Created_Date_Utc_Key   = ifnull(`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Adjustment.Event_Min_Created_Date_Time_Utc),0) 
,Adjustment.Event_Min_Created_Time_Utc_Key	 = IFNULL((extract(Hour from Adjustment.Event_Min_Created_Date_Time_Utc)* 3600) + (extract(MINUTE from Adjustment.Event_Min_Created_Date_Time_Utc)* 60) + (extract(SECOND from Adjustment.Event_Min_Created_Date_Time_Utc)),0)  
,Adjustment.Event_Max_Created_Date_Utc_Key	 = ifnull(`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Adjustment.Event_Max_Created_Date_Time_Utc),0)
,Adjustment.Event_Max_Created_Time_Utc_Key	 = IFNULL((extract(Hour from Adjustment.Event_Max_Created_Date_Time_Utc)* 3600) + (extract(MINUTE from Adjustment.Event_Max_Created_Date_Time_Utc)* 60) + (extract(SECOND from Adjustment.Event_Max_Created_Date_Time_Utc)),0)  
,Adjustment.Event_Min_Created_Date_Cst_Key	 = ifnull(`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Adjustment.Event_Min_Created_Date_Time_Cst),0)  
,Adjustment.Event_Min_Created_Time_Cst_Key	 = IFNULL((extract(Hour from Adjustment.Event_Min_Created_Date_Time_Cst)* 3600) + (extract(MINUTE from Adjustment.Event_Min_Created_Date_Time_Cst)* 60) + (extract(SECOND from Adjustment.Event_Min_Created_Date_Time_Cst)),0)  
,Adjustment.Event_Max_Created_Date_Cst_Key	 = ifnull(`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Adjustment.Event_Max_Created_Date_Time_Cst),0)  
,Adjustment.Event_Max_Created_Time_Cst_Key	 = IFNULL((extract(Hour from Adjustment.Event_Max_Created_Date_Time_Cst)* 3600) + (extract(MINUTE from Adjustment.Event_Max_Created_Date_Time_Cst)* 60) + (extract(SECOND from Adjustment.Event_Max_Created_Date_Time_Cst)),0)  
,Adjustment.Earned_Min_Start_Date_Utc_Key	 = ifnull(`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Adjustment.Earned_Min_Start_Date_Time_Utc),0)  
,Adjustment.Earned_Min_Start_Time_Utc_Key	 = IFNULL((extract(Hour from Adjustment.Earned_Min_Start_Date_Time_Utc)* 3600) + (extract(MINUTE from Adjustment.Earned_Min_Start_Date_Time_Utc)* 60) + (extract(SECOND from Adjustment.Earned_Min_Start_Date_Time_Utc)),0)  
,Adjustment.Earned_Max_Start_Date_Utc_Key	 = ifnull(`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Adjustment.Earned_Max_Start_Date_Time_Utc),0)  
,Adjustment.Earned_Max_Start_Time_Utc_Key	 = IFNULL((extract(Hour from Adjustment.Earned_Max_Start_Date_Time_Utc)* 3600) + (extract(MINUTE from Adjustment.Earned_Max_Start_Date_Time_Utc)* 60) + (extract(SECOND from Adjustment.Earned_Max_Start_Date_Time_Utc)),0)  
,Adjustment.Earned_Min_Start_Date_Cst_Key	 = ifnull(`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Adjustment.Earned_Min_Start_Date_Time_Cst),0) 
,Adjustment.Earned_Min_Start_Time_Cst_Key	 = IFNULL((extract(Hour from Adjustment.Earned_Min_Start_Date_Time_Cst)* 3600) + (extract(MINUTE from Adjustment.Earned_Min_Start_Date_Time_Cst)* 60) + (extract(SECOND from Adjustment.Earned_Min_Start_Date_Time_Cst)),0)  
,Adjustment.Earned_Max_Start_Date_Cst_Key	 = ifnull(`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Adjustment.Earned_Max_Start_Date_Time_Cst),0)  
,Adjustment.Earned_Max_Start_Time_Cst_Key	 = IFNULL((extract(Hour from Adjustment.Earned_Max_Start_Date_Time_Cst)* 3600) + (extract(MINUTE from Adjustment.Earned_Max_Start_Date_Time_Cst)* 60) + (extract(SECOND from Adjustment.Earned_Max_Start_Date_Time_Cst)),0)  
,Adjustment.Record_Created_By                = 'udsp_etl_Fact_Invoice_Line_Item_Email_And_Apps_Adjustment'
,Adjustment.Record_Created_Datetime          = current_datetime()
,Adjustment.Record_Updated_By                = 'udsp_etl_Fact_Invoice_Line_Item_Email_And_Apps_Adjustment'
,Adjustment.Record_Updated_Datetime          = current_datetime()
where true
;


------------
--Update Is_Include_In_Payable and Is_Prepay column for PrePay data
UPDATE `rax-staging-dev`.stage_two_dw.stage_fact_invoice_line_item_email_and_apps_adjustment Adjustment
SET   
    Is_Include_In_Payable = 0,    
	Is_Prepay = 1
WHERE       
(      Adjustment.Prepay_Start_Date_Utc_Key <> 0
   AND Adjustment.Prepay_Start_Date_Utc_Key <> 19700101
   AND Adjustment.Prepay_Start_Date_Utc_Key <> Adjustment.Prepay_End_Date_Utc_Key
   AND Adjustment.Transaction_Term <> 1 
)
;
-------------------------------------------------------------------------------------------------------------------
--Deleting incremental 2 months of data from fact table
DELETE `rax-datamart-dev`.corporate_dmart.fact_invoice_line_item_email_and_apps trg 
where  trg.Invoice_Key in(
select Fact.Invoice_Key
from	`rax-datamart-dev`.corporate_dmart.fact_invoice_line_item_email_and_apps Fact 
	Inner Join `rax-datamart-dev`.corporate_dmart.dim_invoice Invoice 
	On Fact.Invoice_Key = Invoice.Invoice_Key 
	Inner Join (
  SELECT DISTINCT --INTO  #Modified_Invoices_Adj
    Bill_No As Bill_Number 
	FROM  `rax-staging-dev`.stage_one.raw_email_apps_inv_event_detail 
	WHERE
    UPPER(Bill_No) LIKE '%A1%'
  ) modadj --#Modified_Invoices_Adj modadj
	On Invoice.Bill_Number = modadj.Bill_Number   
	WHERE upper(modadj.Bill_Number) LIKE '%A1%'
	);
-------------------------------------------------------------------------------------------------------------------

/*
EXEC msdb..Usp_send_cdosysmail 'no_replyrackspace.com','Rahul.Chourasiyarackspace.com','NRD FACT Invoice Line Item Email And Apps Stage Adjustment load JOB Success',''
END TRY
BEGIN CATCH

       --ROLLBACK TRANSACTION

              DECLARE subject nvarchar(max) = 'NRD Fact Load Failure Notification';
              DECLARE body nvarchar(max) = 'Data Transformation Failed during Fact Invoice Line Item Email And Apps Adjustment Stage Load' 
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
