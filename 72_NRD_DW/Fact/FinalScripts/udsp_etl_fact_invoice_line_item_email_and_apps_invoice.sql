
CREATE or replace PROCEDURE `rax-staging-dev`.stage_three_dw.udsp_etl_fact_invoice_line_item_email_and_apps_invoice()
BEGIN 


DECLARE Exection_Time Datetime;
-----------------------------------------------------------------------------------------------------------------
--Set GL_BeginDate  = convert(varchar(8),Stage_Three_Dw.dbo.udf_FirstDayOfMonth(DATEADD(month, -2, GETDATE())),112)
--Set GL_EndDate    = convert(varchar(8),Stage_Three_Dw.dbo.udf_FirstDayOfNextMonth(getdate()),112)
Set Exection_Time = current_datetime();



	
	
-------------------------------------------------------------------------------------------------------------------
delete from  `rax-staging-dev`.stage_two_dw.stage_fact_invoice_line_item_email_and_apps_invoice where true;
-------------------------------------------------------------------------------------------------------------------


--LOAD INVOICE DATA FROM FACT TABLE INTO STAGE FACT INVOICE TABLe
-------------------------------------------------------------------------------------------------------------------

INSERT INTO `rax-staging-dev`.stage_two_dw.stage_fact_invoice_line_item_email_and_apps_invoice
       ( Date_Month_Key
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
		,Event_Min_Created_Date_Time_Utc
		,Event_Min_Created_Date_Time_Cst
		,Event_Max_Created_Date_Time_Utc
		,Event_Max_Created_Date_Time_Cst
		,Earned_Min_Start_Date_Time_Utc
		,Earned_Min_Start_Date_Time_Cst
		,Earned_Max_Start_Date_Time_Utc
		,Earned_Max_Start_Date_Time_Cst
		,Source_System_Key
		,Event_Min_Created_Date_Utc_Key
        ,Event_Min_Created_Time_Utc_Key
        ,Event_Max_Created_Date_Utc_Key
        ,Event_Max_Created_Time_Utc_Key
		,Event_Min_Created_Date_Cst_Key
        ,Event_Min_Created_Time_Cst_Key
        ,Event_Max_Created_Date_Cst_Key
        ,Event_Max_Created_Time_Cst_Key
        ,Earned_Min_Start_Date_Utc_Key
        ,Earned_Min_Start_Time_Utc_Key
        ,Earned_Max_Start_Date_Utc_Key
        ,Earned_Max_Start_Time_Utc_Key
        ,Earned_Min_Start_Date_Cst_Key
        ,Earned_Min_Start_Time_Cst_Key
        ,Earned_Max_Start_Date_Cst_Key
        ,Earned_Max_Start_Time_Cst_Key)
SELECT Fact.Date_Month_Key
      ,Fact.Revenue_Type_Key
      ,Fact.Account_Key
      ,Fact.Team_Key
      ,Fact.Primary_Contact_Key
      ,Fact.Billing_Contact_Key
      ,Fact.Product_Key
      ,Fact.Datacenter_Key
      ,-1
      ,Fact.Item_Key
      ,Fact.GL_Product_Key
      ,Fact.GL_Account_Key
      ,Fact.Glid_Configuration_Key
      ,-1
      ,Fact.Event_Type_Key
      ,Fact.Invoice_Key
      ,Fact.Invoice_Attribute_Key
      ,-1
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
      ,cast(Is_Standalone_Fee as bool)  as Is_Standalone_Fee
	  ,cast(1  as bool)   As Is_Include_In_Payable
	  ,cast(0  as bool)   As Is_Prepay
	  ,cast(0  as bool)   As Is_Amortize_Prepay
      ,cast(Is_Back_Bill as bool)  as Is_Back_Bill
      ,cast(Is_Fastlane as bool)  as Is_Fastlane
	  ,Fact.Bill_Created_Date_Cst_Key
      ,Fact.Bill_Created_Time_Cst_Key
      ,min(cast(TIMESTAMP_ADD(TIMESTAMP(EventCreatedUtc.Time_Full_Date	), INTERVAL cast(Event_Created_Time_Utc_Key	as int64) second)  as datetime) ) as Event_Min_Created_Date_Time_Utc
      ,min(cast(TIMESTAMP_ADD(TIMESTAMP(EventCreatedCst.Time_Full_Date	), INTERVAL cast(Event_Created_Time_Cst_Key	as int64) second)  as datetime) ) as Event_Min_Created_Date_Time_Cst
      ,max(cast(TIMESTAMP_ADD(TIMESTAMP(EventCreatedUtc.Time_Full_Date	), INTERVAL cast(Event_Created_Time_Utc_Key	as int64) second)  as datetime) ) as Event_Max_Created_Date_Time_Utc
      ,max(cast(TIMESTAMP_ADD(TIMESTAMP(EventCreatedCst.Time_Full_Date	), INTERVAL cast(Event_Created_Time_Cst_Key	as int64) second)  as datetime) ) as Event_Max_Created_Date_Time_Cst
      ,min(cast(TIMESTAMP_ADD(TIMESTAMP(EarnedStartUtc.Time_Full_Date	), INTERVAL cast(Earned_Start_Time_Utc_Key	as int64) second)  as datetime))  as Earned_Min_Start_Date_Time_Utc
      ,min(cast(TIMESTAMP_ADD(TIMESTAMP(EarnedStartCst.Time_Full_Date	), INTERVAL cast(Earned_Start_Time_Cst_Key	as int64) second)  as datetime))  as Earned_Min_Start_Date_Time_Cst
      ,max(cast(TIMESTAMP_ADD(TIMESTAMP(EarnedStartUtc.Time_Full_Date	), INTERVAL cast(Earned_Start_Time_Utc_Key	as int64) second)  as datetime))  as Earned_Max_Start_Date_Time_Utc
      ,max(cast(TIMESTAMP_ADD(TIMESTAMP(EarnedStartCst.Time_Full_Date	), INTERVAL cast(Earned_Start_Time_Cst_Key	as int64) second)  as datetime))  as Earned_Max_Start_Date_Time_Cst
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
  from `rax-datamart-dev`.corporate_dmart.fact_invoice_event_detail_email_and_apps fact 
  left join `rax-staging-dev`.stage_three_dw.dim_time eventcreatedutc  on fact.event_created_date_utc_key=eventcreatedutc.time_key
  left join `rax-staging-dev`.stage_three_dw.dim_time eventcreatedcst  on fact.event_created_date_cst_key=eventcreatedcst.time_key
  left join `rax-staging-dev`.stage_three_dw.dim_time earnedstartutc  on fact.earned_start_date_utc_key=earnedstartutc.time_key
  left join `rax-staging-dev`.stage_three_dw.dim_time earnedstartcst  on fact.earned_start_date_cst_key=earnedstartcst.time_key
  left join `rax-datamart-dev`.corporate_dmart.dim_invoice inv  on fact.invoice_key=inv.invoice_key
  inner join (
  SELECT DISTINCT  --INTO  #Modified_Invoices_Inv
    Bill_No As Bill_Number
FROM  
    `rax-staging-dev`.stage_one.raw_email_apps_inv_event_detail 
WHERE
    upper(Bill_No) NOT LIKE '%A1%'
  ) modinv --#modified_invoices_inv modinv  
  on inv.bill_number = modinv.bill_number 
  Group By
  Fact.Date_Month_Key
      ,Fact.Revenue_Type_Key
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
	UPDATE `rax-staging-dev`.stage_two_dw.stage_fact_invoice_line_item_email_and_apps_invoice Invoice
	Set 
	 Invoice.Event_Min_Created_Date_Utc_Key  =`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Invoice.Event_Min_Created_Date_Time_Utc)
	,Invoice.Event_Min_Created_Time_Utc_Key	 =((extract(Hour from Invoice.Event_Min_Created_Date_Time_Utc)* 360 )+ (extract(MINUTE from Invoice.Event_Min_Created_Date_Time_Utc)* 60) + (extract(SECOND from Invoice.Event_Min_Created_Date_Time_Utc)))
	,Invoice.Event_Max_Created_Date_Utc_Key	 = `rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Invoice.Event_Max_Created_Date_Time_Utc)
	,Invoice.Event_Max_Created_Time_Utc_Key	 = ((extract(Hour from Invoice.Event_Max_Created_Date_Time_Utc)* 360) + (extract(MINUTE from Invoice.Event_Max_Created_Date_Time_Utc)* 60) + (extract(SECOND from Invoice.Event_Max_Created_Date_Time_Utc)) )
	,Invoice.Event_Min_Created_Date_Cst_Key	 = `rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Invoice.Event_Min_Created_Date_Time_Cst)
	,Invoice.Event_Min_Created_Time_Cst_Key	 = ((extract(Hour from Invoice.Event_Min_Created_Date_Time_Cst)* 360 )+ (extract(MINUTE from Invoice.Event_Min_Created_Date_Time_Cst)* 60) + (extract(SECOND from Invoice.Event_Min_Created_Date_Time_Cst)) )
	,Invoice.Event_Max_Created_Date_Cst_Key	 = `rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Invoice.Event_Max_Created_Date_Time_Cst)
	,Invoice.Event_Max_Created_Time_Cst_Key	 = ((extract(Hour from Invoice.Event_Max_Created_Date_Time_Cst)* 360) + (extract(MINUTE from Invoice.Event_Max_Created_Date_Time_Cst)* 60) + (extract(SECOND from Invoice.Event_Max_Created_Date_Time_Cst)) )
	,Invoice.Earned_Min_Start_Date_Utc_Key	 = `rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Invoice.Earned_Min_Start_Date_Time_Utc)
	,Invoice.Earned_Min_Start_Time_Utc_Key	 = ((extract(Hour from Invoice.Earned_Min_Start_Date_Time_Utc)* 360 )+ (extract(MINUTE from Invoice.Earned_Min_Start_Date_Time_Utc)* 60) + (extract(SECOND from Invoice.Earned_Min_Start_Date_Time_Utc)))
	,Invoice.Earned_Max_Start_Date_Utc_Key	 = `rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Invoice.Earned_Max_Start_Date_Time_Utc)
	,Invoice.Earned_Max_Start_Time_Utc_Key	 = ((extract(Hour from Invoice.Earned_Max_Start_Date_Time_Utc)* 360) + (extract(MINUTE from Invoice.Earned_Max_Start_Date_Time_Utc)* 60) + (extract(SECOND from Invoice.Earned_Max_Start_Date_Time_Utc)) )
	,Invoice.Earned_Min_Start_Date_Cst_Key	 = `rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Invoice.Earned_Min_Start_Date_Time_Cst)
	,Invoice.Earned_Min_Start_Time_Cst_Key	 = ((extract(Hour from Invoice.Earned_Min_Start_Date_Time_Cst)* 360 )+ (extract(MINUTE from Invoice.Earned_Min_Start_Date_Time_Cst)* 60) + (extract(SECOND from Invoice.Earned_Min_Start_Date_Time_Cst)) )
	,Invoice.Earned_Max_Start_Date_Cst_Key	 = `rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Invoice.Earned_Max_Start_Date_Time_Cst)
	,Invoice.Earned_Max_Start_Time_Cst_Key	 = ((extract(Hour from Invoice.Earned_Max_Start_Date_Time_Cst)* 360 )+ (extract(MINUTE from Invoice.Earned_Max_Start_Date_Time_Cst)* 60) + (extract(SECOND from Invoice.Earned_Max_Start_Date_Time_Cst)))
	,Invoice.Record_Created_By               = 'udsp_etl_Fact_Invoice_Line_Item_Email_And_Apps_Invoice'
	,Invoice.Record_Created_Datetime         = current_datetime()
	,Invoice.Record_Updated_By               = 'udsp_etl_Fact_Invoice_Line_Item_Email_And_Apps_Invoice'
	,Invoice.Record_Updated_Datetime         = current_datetime()
	where true;
-------------------------------------------------------------------------------------------------------------------
--Update Is_Include_In_Payable and Is_Prepay column
UPDATE `rax-staging-dev`.stage_two_dw.stage_fact_invoice_line_item_email_and_apps_invoice  Invoice
SET   
    Is_Include_In_Payable =cast( 0 as bool),    
	Is_Prepay = cast( 1 as bool)

WHERE       
(      Invoice.Prepay_Start_Date_Utc_Key <> 0
   AND Invoice.Prepay_Start_Date_Utc_Key <> 19700101
   AND Invoice.Prepay_Start_Date_Utc_Key <> Invoice.Prepay_End_Date_Utc_Key
   AND Invoice.Transaction_Term <> 1 
);
-------------------------------------------------------------------------------------------------------------------
--Deleting incremental 2 months of data from fact table
DELETE `rax-datamart-dev`.corporate_dmart.fact_invoice_line_item_email_and_apps trg
where trg.invoice_key in ( select fact.invoice_key
FROM 
	`rax-datamart-dev`.corporate_dmart.fact_invoice_line_item_email_and_apps fact 
	inner join `rax-datamart-dev`.corporate_dmart.dim_invoice invoice 
	on fact.invoice_key = invoice.invoice_key 
	inner join (
		SELECT DISTINCT  --INTO  #Modified_Invoices_Inv
			Bill_No As Bill_Number
		FROM  
			`rax-staging-dev`.stage_one.raw_email_apps_inv_event_detail 
		WHERE
    upper(Bill_No) NOT LIKE '%A1%'
	)modinv --#modified_invoices_inv modinv
	on invoice.bill_number = modinv.bill_number   
	WHERE upper(modinv.bill_number) NOT LIKE '%A1%'
	);
	/*
-------------------------------------------------------------------------------------------------------------------
EXEC msdb..Usp_send_cdosysmail 'no_replyrackspace.com','Rahul.Chourasiyarackspace.com','NRD FACT Invoice Line Item Email And Apps Stage Invoice load JOB Success',''
END TRY
BEGIN CATCH

       --ROLLBACK TRANSACTION

              DECLARE subject nvarchar(max) = 'NRD Fact Load Failure Notification';
              DECLARE body nvarchar(max) = 'Data Transformation Failed during Fact Invoice Line Item Email And Apps Invoice Stage Load' 
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
