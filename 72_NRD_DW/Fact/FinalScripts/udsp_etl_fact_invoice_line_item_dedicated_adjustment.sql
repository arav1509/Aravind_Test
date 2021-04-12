CREATE or replace PROCEDURE `rax-staging-dev`.stage_three_dw.udsp_etl_fact_invoice_line_item_dedicated_adjustment()

BEGIN 
/* =============================================
-- Created By :	Rahul Chourasiya
-- Create date: 8.27.2019
-- Description: Truncate and reloads 2 months of dedicated adjustment records from Fact_Invoice_Event_Detail_Dedicated to Adjustment staging table
-- =============================================*/
-----------------------------------------------------------------------------------------------------------------
--DECLARE GL_BeginDate int
--DECLARE GL_EndDate int
DECLARE HMDB_END Datetime default datetime('2017-09-22');
--DECLARE Min_BRM_Date varchar(23)
DECLARE Exection_Time Datetime default current_datetime() ;
-----------------------------------------------------------------------------------------------------------------



create or replace temp table Modified_Invoices_Adj as
SELECT--INTO #Modified_Invoices_Adj
	 Trx_Number As Bill_Number
FROM `rax-staging-dev`.stage_one.raw_dedicated_inv_event_detail
union all
SELECT
	Bill_Number
FROM
	(
		SELECT DISTINCT --INTO  #Missing_Bill_No_Adj
			Bill_No As Bill_Number,
			Time_Month_Key
		FROM
			 `rax-staging-dev`.stage_one.raw_brm_dedicated_invoice_aggregate_total Agg 
		WHERE
			`Exclude`=0
		AND  (CURRENT_TOTAL)<>0  
		and ( lower(bill_no) not like '%ebs%' and lower(bill_no) not like '%evapt%' and lower(bill_no) not like '%dp_inv%')
		AND NOT EXISTS (SELECT
			Invoice.Bill_Number 
		FROM  
			`rax-datamart-dev`.corporate_dmart.fact_invoice_line_item_dedicated Fact 
			Inner Join `rax-datamart-dev`.corporate_dmart.dim_invoice invoice
			On Fact.Invoice_Key=Invoice.Invoice_Key 
		WHERE Invoice.Bill_Number=Agg.BILL_NO
		GROUP BY Invoice.Bill_Number)
		AND Agg.Time_Month_Key  >= 201502
	)A --#Missing_Bill_No_Adj A with (nolock)
;

create or replace table `rax-staging-dev`.stage_two_dw.stage_fact_invoice_line_item_dedicated_adjustment
as
SELECT Fact.Date_Month_Key
      ,Rev.Revenue_Type_Key
      ,Fact.Account_Key
      ,Fact.Team_Key
      ,Fact.Primary_Contact_Key
      ,Fact.Billing_Contact_Key
      ,Fact.Product_Key
      ,Fact.Datacenter_Key
      ,Fact.Device_Key
      ,Fact.Item_Key
      ,Fact.GL_Product_Key
      ,Fact.GL_Account_Key
      ,Fact.Glid_Configuration_Key
      ,Fact.Payment_Term_Key
      ,Fact.Event_Type_Key
      ,Fact.Invoice_Key
      ,Fact.Invoice_Attribute_Key
      ,Fact.Raw_Start_Month_Key
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
      ,sum(Extended_Amount_Usd) as Extended_Amount_Usd
      ,sum(Unit_Selling_Price_Usd) as Unit_Selling_Price_Usd
      ,sum(Amount_Gbp) as Amount_Gbp
      ,sum(Extended_Amount_Gbp) as Extended_Amount_Gbp
      ,sum(Unit_Selling_Price_Gbp) as Unit_Selling_Price_Gbp
      ,sum(Amount_Local) as Amount_Local
      ,sum(Extended_Amount_Local) as Extended_Amount_Local
      ,sum(Unit_Selling_Price_Local) as Unit_Selling_Price_Local
      ,Is_Standalone_Fee
	  ,0 As Is_Prepay
	  ,0 As Is_Amortize_Prepay
      ,Is_Back_Bill
      ,Is_Fastlane
	  ,Fact.Bill_Created_Date_Cst_Key
      ,Fact.Bill_Created_Time_Cst_Key
	  ,min(cast(TIMESTAMP_ADD(TIMESTAMP(EventCreatedUtc.Time_Full_Date), INTERVAL cast( Event_Created_Time_Utc_Key as int64) second)  as datetime))as Event_Min_Created_Date_Time_Utc
      ,min(cast(TIMESTAMP_ADD(TIMESTAMP(EventCreatedCst.Time_Full_Date), INTERVAL cast( Event_Created_Time_Cst_Key as int64) second)  as datetime)) as Event_Min_Created_Date_Time_Cst
      ,min(cast(TIMESTAMP_ADD(TIMESTAMP(EventCreatedUtc.Time_Full_Date), INTERVAL cast( Event_Created_Time_Utc_Key as int64) second)  as datetime)) as Event_Max_Created_Date_Time_Utc
      ,min(cast(TIMESTAMP_ADD(TIMESTAMP(EventCreatedCst.Time_Full_Date), INTERVAL cast( Event_Created_Time_Cst_Key as int64) second)  as datetime)) as Event_Max_Created_Date_Time_Cst
      ,min(cast(TIMESTAMP_ADD(TIMESTAMP(EarnedStartUtc.Time_Full_Date ), INTERVAL cast( Earned_Start_Time_Utc_Key as int64) second)  as datetime)) as Earned_Min_Start_Date_Time_Utc
      ,min(cast(TIMESTAMP_ADD(TIMESTAMP(EarnedStartCst.Time_Full_Date ), INTERVAL cast( Earned_Start_Time_Cst_Key as int64) second)  as datetime)) as Earned_Min_Start_Date_Time_Cst
      ,min(cast(TIMESTAMP_ADD(TIMESTAMP(EarnedStartUtc.Time_Full_Date ), INTERVAL cast( Earned_Start_Time_Utc_Key as int64) second)  as datetime)) as Earned_Max_Start_Date_Time_Utc
      ,min(cast(TIMESTAMP_ADD(TIMESTAMP(EarnedStartCst.Time_Full_Date ), INTERVAL cast( Earned_Start_Time_Cst_Key as int64) second)  as datetime)) as Earned_Max_Start_Date_Time_Cst
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
	  ,'udsp_etl_Fact_Invoice_Line_Item_Dedicated_Adjustment' as Record_Created_By
	  ,current_datetime() as Record_Created_Datetime
	  ,'udsp_etl_Fact_Invoice_Line_Item_Dedicated_Adjustment' as Record_Updated_By
	  ,current_datetime() as Record_Updated_Datetime
  from `rax-datamart-dev`.corporate_dmart.fact_invoice_event_detail_dedicated fact 
  left join `rax-staging-dev`.stage_three_dw.dim_time eventcreatedutc  on fact.event_created_date_utc_key=eventcreatedutc.time_key
  left join `rax-staging-dev`.stage_three_dw.dim_time eventcreatedcst  on fact.event_created_date_cst_key=eventcreatedcst.time_key
  left join `rax-staging-dev`.stage_three_dw.dim_time earnedstartutc  on fact.earned_start_date_utc_key=earnedstartutc.time_key
  left join `rax-staging-dev`.stage_three_dw.dim_time earnedstartcst  on fact.earned_start_date_cst_key=earnedstartcst.time_key
  left join `rax-datamart-dev`.corporate_dmart.dim_billing_events billingevents  on fact.event_type_key=billingevents.event_type_key
  left join `rax-datamart-dev`.corporate_dmart.dim_item item  on fact.item_key=item.item_key
  left join `rax-datamart-dev`.corporate_dmart.dim_invoice inv  on fact.invoice_key=inv.invoice_key
  left join `rax-staging-dev`.stage_three_dw.dim_revenue_type rev  on 'adjustment' = rev.revenue_type_name
  inner join  Modified_Invoices_Adj modinv  on inv.bill_number = modinv.bill_number 
  Where -- Is_Transaction_Completed = 1 AND
   ( lower(BillingEvents.Event_Type) like '%adjustment%' or lower(BillingEvents.Event_Type)  like '%refund%' or lower(Item.Item_Name) like '%adjustment%')
  Group By
  Fact.Date_Month_Key
      ,Rev.Revenue_Type_Key
      ,Fact.Account_Key
      ,Fact.Team_Key
      ,Fact.Primary_Contact_Key
      ,Fact.Billing_Contact_Key
      ,Fact.Product_Key
      ,Fact.Datacenter_Key
      ,Fact.Device_Key
      ,Fact.Item_Key
      ,Fact.GL_Product_Key
      ,Fact.GL_Account_Key
      ,Fact.Glid_Configuration_Key
      ,Fact.Payment_Term_Key
      ,Fact.Event_Type_Key
      ,Fact.Invoice_Key
      ,Fact.Invoice_Attribute_Key
      ,Fact.Raw_Start_Month_Key
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
-------------------------------------------------------------------------------------------------------------------

;

UPDATE `rax-staging-dev`.stage_two_dw.stage_fact_invoice_line_item_dedicated_adjustment  Adjustment
Set 
 Adjustment.Event_Min_Created_Date_Utc_Key   = cast( ifnull(FORMAT_DATE("%Y%m%d",date(Adjustment.Event_Min_Created_Date_Time_Utc) ) ,'19000101') as int64)
,Adjustment.Event_Min_Created_Time_Utc_Key	 = IFNULL((extract(Hour   from Adjustment.Event_Min_Created_Date_Time_Utc)* 3600) +(extract(MINUTE from Adjustment.Event_Min_Created_Date_Time_Utc)* 60) +(extract(SECOND from Adjustment.Event_Min_Created_Date_Time_Utc)),0)
,Adjustment.Event_Max_Created_Date_Utc_Key	 = cast( ifnull(FORMAT_DATE("%Y%m%d",date(Adjustment.Event_Max_Created_Date_Time_Utc) ) ,'19000101') as int64)
,Adjustment.Event_Max_Created_Time_Utc_Key	 = IFNULL((extract(Hour   from  Adjustment.Event_Max_Created_Date_Time_Utc)* 3600)+(extract(MINUTE from  Adjustment.Event_Max_Created_Date_Time_Utc)* 60)+(extract(SECOND from  Adjustment.Event_Max_Created_Date_Time_Utc)),0)
,Adjustment.Event_Min_Created_Date_Cst_Key	 = cast( ifnull(FORMAT_DATE("%Y%m%d",date(Adjustment.Event_Min_Created_Date_Time_Cst) ) ,'19000101')as int64)
,Adjustment.Event_Min_Created_Time_Cst_Key	 = IFNULL((extract(Hour   from  Adjustment.Event_Min_Created_Date_Time_Cst)* 3600) + (extract(MINUTE from  Adjustment.Event_Min_Created_Date_Time_Cst)* 60) +(extract(SECOND from  Adjustment.Event_Min_Created_Date_Time_Cst)),0	)
,Adjustment.Event_Max_Created_Date_Cst_Key	 = cast( ifnull(FORMAT_DATE("%Y%m%d",date(Adjustment.Event_Max_Created_Date_Time_Cst) ) ,'19000101') as int64)
,Adjustment.Event_Max_Created_Time_Cst_Key	 = IFNULL((extract(Hour   from  Adjustment.Event_Max_Created_Date_Time_Cst)* 3600) + (extract(MINUTE from  Adjustment.Event_Max_Created_Date_Time_Cst)* 60) +(extract(SECOND from  Adjustment.Event_Max_Created_Date_Time_Cst)),0	)
,Adjustment.Earned_Min_Start_Date_Utc_Key	 = cast( ifnull(FORMAT_DATE("%Y%m%d",date(Adjustment.Earned_Min_Start_Date_Time_Utc) ) ,'19000101') as int64)
,Adjustment.Earned_Min_Start_Time_Utc_Key	 = IFNULL((extract(Hour   from  Adjustment.Earned_Min_Start_Date_Time_Utc)* 3600) + (extract(MINUTE from  Adjustment.Earned_Min_Start_Date_Time_Utc)* 60) +(extract(SECOND from  Adjustment.Earned_Min_Start_Date_Time_Utc)),0	)
,Adjustment.Earned_Max_Start_Date_Utc_Key	 = cast( ifnull(FORMAT_DATE("%Y%m%d",date(Adjustment.Earned_Max_Start_Date_Time_Utc) ) ,'19000101') as int64)
,Adjustment.Earned_Max_Start_Time_Utc_Key	 = IFNULL((extract(Hour   from  Adjustment.Earned_Max_Start_Date_Time_Utc)* 3600) + (extract(MINUTE from  Adjustment.Earned_Max_Start_Date_Time_Utc)* 60) +(extract(SECOND from  Adjustment.Earned_Max_Start_Date_Time_Utc)),0	)
,Adjustment.Earned_Min_Start_Date_Cst_Key	 = cast( ifnull(FORMAT_DATE("%Y%m%d",date(Adjustment.Earned_Min_Start_Date_Time_Cst) ) ,'19000101')as int64)
,Adjustment.Earned_Min_Start_Time_Cst_Key	 = IFNULL((extract(Hour   from  Adjustment.Earned_Min_Start_Date_Time_Cst)* 3600) + (extract(MINUTE from  Adjustment.Earned_Min_Start_Date_Time_Cst)* 60) +(extract(SECOND from  Adjustment.Earned_Min_Start_Date_Time_Cst)),0	)
,Adjustment.Earned_Max_Start_Date_Cst_Key	 = cast( ifnull(FORMAT_DATE("%Y%m%d",date(Adjustment.Earned_Max_Start_Date_Time_Cst) ) ,'19000101')as int64)
,Adjustment.Earned_Max_Start_Time_Cst_Key	 = IFNULL((extract(Hour   from  Adjustment.Earned_Max_Start_Date_Time_Cst)* 3600) + (extract(MINUTE from  Adjustment.Earned_Max_Start_Date_Time_Cst)* 60) +(extract(SECOND from  Adjustment.Earned_Max_Start_Date_Time_Cst)),0	)
,Adjustment.Record_Created_By                = 'udsp_etl_Fact_Invoice_Line_Item_Dedicated_Adjustment'
,Adjustment.Record_Created_Datetime          = current_datetime()
,Adjustment.Record_Updated_By                = 'udsp_etl_Fact_Invoice_Line_Item_Dedicated_Adjustment'
,Adjustment.Record_Updated_Datetime          = current_datetime()
where true
;

------------------------------------------------------------------------------------------------------------------
-- Update Is_Prepay and Is_Amortize_Prepay
UPDATE `rax-staging-dev`.stage_two_dw.stage_fact_invoice_line_item_dedicated_adjustment Adjustment
SET      
	Is_Prepay = 1
where       
(      adjustment.prepay_start_date_utc_key not in ( 0, 19700101, 19000101)
   and adjustment.prepay_start_date_utc_key <> adjustment.prepay_end_date_utc_key
   and adjustment.quantity <> 1 
);
-------------------------------------------------------------------------------------------------------------------

/*EXEC msdb..Usp_send_cdosysmail 'no_replyrackspace.com','Rahul.Chourasiyarackspace.com','NRD FACT Invoice Line Item Dedicated Stage Adjustment load JOB Success',''
END TRY
BEGIN CATCH

       --ROLLBACK TRANSACTION

              DECLARE subject nvarchar(max) = 'NRD Fact Load Failure Notification';
              DECLARE body nvarchar(max) = 'Data Transformation Failed during Fact Invoice Line Item Dedicated Stating Adjustment Load' 
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
