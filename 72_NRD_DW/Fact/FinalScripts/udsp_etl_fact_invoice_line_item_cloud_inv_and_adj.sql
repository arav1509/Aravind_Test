CREATE or replace PROCEDURE `rax-staging-dev`.stage_three_dw.udsp_etl_fact_invoice_line_item_cloud_inv_and_adj()
BEGIN 
/* =============================================
-- Created By :	hari4586
-- Create date: 10.03.2019
-- Description: Deletes and reloads 2 months of cloud invoiced records from Fact_Invoice_Event_Detail_Cloud

       Modified By    Date     Description    
1) hari4586 21-10-2019 Added adjustment stage layer and inserting adjustment through the stage layer (Stage_Credits_BRM table)
-- =============================================*/
-----------------------------------------------------------------------------------------------------------------

DECLARE GL_BeginDate int64;
DECLARE GL_EndDate int64;
DECLARE HMDB_END Datetime;
DECLARE Min_BRM_Date Datetime;
DECLARE Exection_Time Datetime; 
-----------------------------------------------------------------------------------------------------------------
Set GL_BeginDate = `rax-staging-dev`.bq_functions.udf_time_key_nohyphen(`rax-staging-dev.bq_functions.udf_firstdayofmonth`(date_sub(current_date, interval 2 month)));
Set GL_EndDate =`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(`rax-staging-dev.bq_functions.udf_firstdayofmonth`(date_add(current_date, interval 1 month)));
SET HMDB_END = datetime('2015-02-02');
Set Min_BRM_Date =datetime('2017-09-22');
Set Exection_Time = current_datetime();
--------------------------------------------




 delete from `rax-staging-dev`.stage_two_dw.stage_fact_invoice_line_item_cloud_invoice where true;
 

INSERT INTO`rax-staging-dev`.stage_two_dw.stage_fact_invoice_line_item_cloud_invoice
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
		,Quantity
		,Billing_Days_In_Month
		,Amount_Usd
		,Amount_Normalized_USD
		,Extended_Amount_Usd
		,Unit_Selling_Price_Usd
		,Amount_Gbp
		,Amount_Normalized_Gbp
		,Extended_Amount_Gbp
		,Unit_Selling_Price_Gbp
		,Amount_Local
		,Amount_Normalized_Local
		,Extended_Amount_Local
		,Unit_Selling_Price_Local
		,Is_Normalize
		,Is_Standalone_Fee
		,Is_Prepay
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
      ,-1 as Datacenter_Key
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
      ,-1 as Prepay_Start_Date_Utc_Key
      ,-1 as Prepay_End_Date_Utc_Key
      ,Currency_Key
	  ,cast(Fact.Transaction_Term as numeric) as Transaction_Term
      ,cast(sum(Quantity)as numeric) as Quantity
      ,Billing_Days_In_Month
      ,cast(sum(Amount_Usd) as numeric) as Amount_Usd
	    ,cast(sum(Amount_Usd) as numeric) as Amount_Normalized_USD
      ,0 as Extended_Amount_Usd
      ,cast(sum(Unit_Selling_Price_Usd) as numeric) as Unit_Selling_Price_Usd
      ,cast(sum(Amount_Gbp)as numeric)  as Amount_GBP
	    ,cast(sum(Amount_Gbp)as numeric)  as Amount_Normalized_Gbp
      ,0 as Extended_Amount_Gbp
      ,cast(sum(Unit_Selling_Price_Gbp) as numeric)as Unit_Selling_Price_Gbp
      ,cast(sum(Amount_Local) as numeric)as Amount_Local
	    ,cast(sum(Amount_Local)as numeric) as Amount_Normalized_Local
      ,0 as Extended_Amount_Local
      ,cast(sum(Unit_Selling_Price_Local)as numeric) as Unit_Selling_Price_Local
      ,ifnull(xref_product.normalize,0) as Is_Normalize
	  ,cast(Is_Standalone_Fee as int64) as Is_Standalone_Fee
	  ,0 As Is_Prepay 
      ,cast(Is_Back_Bill  as int64) as Is_Back_Bill
      ,cast(Is_Fastlane as int64) as Is_Fastlane
	  ,Fact.Bill_Created_Date_Cst_Key
      ,Fact.Bill_Created_Time_Cst_Key
	  ,min(cast(TIMESTAMP_ADD(TIMESTAMP(EventCreatedUtc.Time_Full_Date), INTERVAL cast(Event_Created_Time_Utc_Key  as int64) second)  as datetime)) as Event_Min_Created_Date_Time_Utc 
	  ,min(cast(TIMESTAMP_ADD(TIMESTAMP(EventCreatedCst.Time_Full_Date), INTERVAL cast(Event_Created_Time_Cst_Key  as int64) second)  as datetime)) as Event_Min_Created_Date_Time_Cst 
	  ,max(cast(TIMESTAMP_ADD(TIMESTAMP(EventCreatedUtc.Time_Full_Date), INTERVAL cast(Event_Created_Time_Utc_Key  as int64) second)  as datetime)) as Event_Max_Created_Date_Time_Utc 
	  ,max(cast(TIMESTAMP_ADD(TIMESTAMP(EventCreatedCst.Time_Full_Date), INTERVAL cast(Event_Created_Time_Cst_Key  as int64) second)  as datetime)) as Event_Max_Created_Date_Time_Cst 
	  ,min(cast(TIMESTAMP_ADD(TIMESTAMP(EarnedStartUtc.Time_Full_Date ), INTERVAL cast(Earned_Start_Time_Utc_Key   as int64) second)  as datetime)) as Earned_Min_Start_Date_Time_Utc 
	  ,min(cast(TIMESTAMP_ADD(TIMESTAMP(EarnedStartCst.Time_Full_Date ), INTERVAL cast(Earned_Start_Time_Cst_Key   as int64) second)  as datetime)) as Earned_Min_Start_Date_Time_Cst 
	  ,max(cast(TIMESTAMP_ADD(TIMESTAMP(EarnedStartUtc.Time_Full_Date ), INTERVAL cast(Earned_Start_Time_Utc_Key   as int64) second)  as datetime)) as Earned_Max_Start_Date_Time_Utc 
	  ,max(cast(TIMESTAMP_ADD(TIMESTAMP(EarnedStartCst.Time_Full_Date ), INTERVAL cast(Earned_Start_Time_Cst_Key   as int64) second)  as datetime)) as Earned_Max_Start_Date_Time_Cst 
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
  from `rax-datamart-dev`.corporate_dmart.fact_invoice_event_detail_cloud fact 
  left join `rax-staging-dev`.stage_three_dw.dim_time eventcreatedutc  on fact.event_created_date_utc_key=eventcreatedutc.time_key
  left join `rax-staging-dev`.stage_three_dw.dim_time eventcreatedcst  on fact.event_created_date_cst_key=eventcreatedcst.time_key
  left join `rax-staging-dev`.stage_three_dw.dim_time earnedstartutc  on fact.earned_start_date_utc_key=earnedstartutc.time_key
  left join `rax-staging-dev`.stage_three_dw.dim_time earnedstartcst  on fact.earned_start_date_cst_key=earnedstartcst.time_key
  left join `rax-datamart-dev`.corporate_dmart.dim_billing_events billingevents  on fact.event_type_key=billingevents.event_type_key
  left join `rax-datamart-dev`.corporate_dmart.dim_item item  on fact.item_key=item.item_key
  left join `rax-datamart-dev`.corporate_dmart.dim_invoice inv  on fact.invoice_key=inv.invoice_key
  left join `rax-staging-dev`.stage_three_dw.dim_product dp  on fact.product_key=dp.product_key
  left join `rax-staging-dev`.stage_two_dw.stage_cloud_hosting_products xref_product  on dp.product_resource_code_nk=cast(xref_product.product_id as string)
  inner join  (
					SELECT DISTINCT -- #Modified_Invoices_Cloud
						BILL_NO AS Invoice
					FROM  
						`rax-staging-dev`.stage_one.raw_invitemeventdetail_daily_stage
					union all
					SELECT
						Invoice
					FROM
						(
							SELECT DISTINCT  --INTO   #missing_BILL_NO
								BILL_NO		AS Invoice,
								Time_Month_Key

							FROM
								`rax-staging-dev`.stage_one.raw_brm_invoice_aggregate_total A 
							WHERE
								`Exclude`=0
							AND  (CURRENT_TOTAL)<>0  
							AND ( upper(BILL_NO) not like '%EBS%' AND upper(BILL_NO) not like '%EVAPT%')
							AND NOT EXISTS (SELECT
								distinct Invoice.Bill_Number Invoice
							FROM
								`rax-datamart-dev`.corporate_dmart.fact_invoice_line_item_cloud fact 
								inner join `rax-datamart-dev`.corporate_dmart.dim_invoice invoice 
								on fact.invoice_key=invoice.invoice_key
								where invoice.bill_number=a.bill_no
								--group by invoice.bill_number
							  )
							AND A.Time_Month_Key  >= `rax-staging-dev`.bq_functions.udf_yearmonth_nohyphen(HMDB_END)

						) A--#missing_BILL_NO A
  )modinv --#modified_invoices_cloud 
    on inv.bill_number = modinv.invoice
  Where  --Is_Transaction_Completed = 1 AND
    ( lower(BillingEvents.Event_Type) not like '%adjustment%' and lower(BillingEvents.Event_Type) not like '%refund%' and lower(ifnull(Item.Item_Name,'NULL')) <> 'adjustment')
  Group By
  Fact.Date_Month_Key
      ,Fact.Revenue_Type_Key
      ,Fact.Account_Key
      ,Fact.Team_Key
      ,Fact.Primary_Contact_Key
      ,Fact.Billing_Contact_Key
      ,Fact.Product_Key
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
      ,Fact.Currency_Key
	  ,Fact.Transaction_Term
      ,Fact.Billing_Days_In_Month
      ,ifnull(xref_product.normalize,0)
	  ,Fact.Is_Standalone_Fee
      ,Fact.Is_Back_Bill
      ,Fact.Is_Fastlane
	  ,Fact.Bill_Created_Date_Cst_Key
      ,Fact.Bill_Created_Time_Cst_Key
      ,Fact.Source_System_Key 
	  ;
-------------
UPDATE `rax-staging-dev`.stage_two_dw.stage_fact_invoice_line_item_cloud_invoice Invoice
Set 
 Invoice.Event_Min_Created_Date_Utc_Key  = cast(ifnull(FORMAT_DATE("%Y%m%d",date(Invoice.Event_Min_Created_Date_Time_Utc) ) ,'19000101') as int64)
,Invoice.Event_Min_Created_Time_Utc_Key	 = cast(IFNULL((extract(Hour from Invoice.Event_Min_Created_Date_Time_Utc)* 3600) +(extract(MINUTE from Invoice.Event_Min_Created_Date_Time_Utc)* 60) +(extract(SECOND from Invoice.Event_Min_Created_Date_Time_Utc)),0) as int64)
,Invoice.Event_Max_Created_Date_Utc_Key	 = cast(ifnull(FORMAT_DATE("%Y%m%d",date(Invoice.Event_Max_Created_Date_Time_Utc) ) ,'19000101') as int64)
,Invoice.Event_Max_Created_Time_Utc_Key	 = cast(IFNULL((extract(Hour from Invoice.Event_Max_Created_Date_Time_Utc)* 3600) +(extract(MINUTE from Invoice.Event_Max_Created_Date_Time_Utc)* 60) +(extract(SECOND from Invoice.Event_Max_Created_Date_Time_Utc)),0) as int64)
,Invoice.Event_Min_Created_Date_Cst_Key	 = cast(ifnull(FORMAT_DATE("%Y%m%d",date(Invoice.Event_Min_Created_Date_Time_Cst) ) ,'19000101')as int64)
,Invoice.Event_Min_Created_Time_Cst_Key	 = cast(IFNULL((extract(Hour from Invoice.Event_Min_Created_Date_Time_Cst)* 3600) +(extract(MINUTE from Invoice.Event_Min_Created_Date_Time_Cst)* 60) +(extract(SECOND from Invoice.Event_Min_Created_Date_Time_Cst)),0) as int64)
,Invoice.Event_Max_Created_Date_Cst_Key	 = cast(ifnull(FORMAT_DATE("%Y%m%d",date(Invoice.Event_Max_Created_Date_Time_Cst) ) ,'19000101')as int64)
,Invoice.Event_Max_Created_Time_Cst_Key	 = cast(IFNULL((extract(Hour from Invoice.Event_Max_Created_Date_Time_Cst)* 3600) +(extract(MINUTE from Invoice.Event_Max_Created_Date_Time_Cst)* 60) +(extract(SECOND from Invoice.Event_Max_Created_Date_Time_Cst)),0) as int64)
,Invoice.Earned_Min_Start_Date_Utc_Key	 = cast(ifnull(FORMAT_DATE("%Y%m%d",date(Invoice.Earned_Min_Start_Date_Time_Utc) ) ,'19000101')as int64)
,Invoice.Earned_Min_Start_Time_Utc_Key	 = cast(IFNULL((extract(Hour from Invoice.Earned_Min_Start_Date_Time_Utc)* 3600) +(extract(MINUTE from Invoice.Earned_Min_Start_Date_Time_Utc)* 60) +(extract(SECOND from Invoice.Earned_Min_Start_Date_Time_Utc)),0) as int64)
,Invoice.Earned_Max_Start_Date_Utc_Key	 = cast(ifnull(FORMAT_DATE("%Y%m%d",date(Invoice.Earned_Max_Start_Date_Time_Utc) ) ,'19000101')as int64)
,Invoice.Earned_Max_Start_Time_Utc_Key	 = cast(IFNULL((extract(Hour from Invoice.Earned_Max_Start_Date_Time_Utc)* 3600) +(extract(MINUTE from Invoice.Earned_Max_Start_Date_Time_Utc)* 60) +(extract(SECOND from Invoice.Earned_Max_Start_Date_Time_Utc)),0) as int64)
,Invoice.Earned_Min_Start_Date_Cst_Key	 = cast(ifnull(FORMAT_DATE("%Y%m%d",date(Invoice.Earned_Min_Start_Date_Time_Cst) ) ,'19000101')as int64)
,Invoice.Earned_Min_Start_Time_Cst_Key	 = cast(IFNULL((extract(Hour from Invoice.Earned_Min_Start_Date_Time_Cst)* 3600) +(extract(MINUTE from Invoice.Earned_Min_Start_Date_Time_Cst)* 60) +(extract(SECOND from Invoice.Earned_Min_Start_Date_Time_Cst)),0) as int64)
,Invoice.Earned_Max_Start_Date_Cst_Key	 = cast(ifnull(FORMAT_DATE("%Y%m%d",date(Invoice.Earned_Max_Start_Date_Time_Cst) ) ,'19000101')as int64)
,Invoice.Earned_Max_Start_Time_Cst_Key	 = cast(IFNULL((extract(Hour from Invoice.Earned_Max_Start_Date_Time_Cst)* 3600) +(extract(MINUTE from Invoice.Earned_Max_Start_Date_Time_Cst)* 60) +(extract(SECOND from Invoice.Earned_Max_Start_Date_Time_Cst)),0) as int64)
,Invoice.Record_Created_By               = 'udsp_etl_Fact_Invoice_Line_Item_Cloud_Inv_and_Adj'
,Invoice.Record_Created_Datetime         = Exection_Time
,Invoice.Record_Updated_By               = 'udsp_etl_Fact_Invoice_Line_Item_Cloud_Inv_and_Adj'
,Invoice.Record_Updated_Datetime         = Exection_Time
where true
;

--------------------UPDATE NORMALIZED AMOUNT IN Stage_Fact_Invoice_Line_Item_Cloud_Invoice STARTS HERE------------------------



create or replace table `rax-staging-dev`.stage_two_dw.account_bdom as 
select distinct a.account_no,bnfo.ACTG_CYCLE_DOM,A.Poid_ID0 as account_object_ID --into Stage_Two_NRD_Deploymenttest.dbo.ACCOUNT_BDOM 
from	`rax-landing-qa`.brm_ods.billinfo_t bnfo  
   Inner Join `rax-landing-qa`.brm_ods.account_t A  On bnfo.account_obj_id0=A.Poid_id0;
   
-----------------------------------------------------------------------------------------------------------
-- Duplicate issue
 UPDATE `rax-staging-dev`.stage_two_dw.stage_fact_invoice_line_item_cloud_invoice  trg
SET    previous_invoice_date = date_sub(cast(DT.time_full_date as date),interval 1 month), 
       Previous_Invoice_Date_No_Time = date_sub(cast(DT.time_full_date as date),interval 1 month), 
       Previous_Invoice_Date_Calc = Cast(CASE 
										WHEN  cast(extract(day from DT.time_full_date) as int64) = cast(ACTG_CYCLE_DOM as int64)  
											OR 
											extract(day from `rax-staging-dev`.bq_functions.udf_lastdayofmonth(date_sub(cast(DT.time_full_date as date),interval 1 month)) ) < ACTG_CYCLE_DOM 
										THEN date_sub(cast(DT.time_full_date as date),interval 1 month)
										ELSE 
										PARSE_DATE("%m/%d/%Y",cast(concat(cast(extract(month from date_sub(cast(DT.time_full_date as date),interval cast(ACTG_CYCLE_DOM as int64)  day) )  as string)
												, '/' , cast(ACTG_CYCLE_DOM as string) , '/' 
												,cast(extract(year from date_sub(cast(DT.time_full_date as date),interval cast(ACTG_CYCLE_DOM as int64)  day)) as string)
												) as string) )
												END AS DATETIME)
												, 
Updated_DesiredBillingDate = cast(ACTG_CYCLE_DOM as  int64), 
DesiredBillingInvoice_Date = DT.time_full_date 
from `rax-staging-dev`.stage_two_dw.stage_fact_invoice_line_item_cloud_invoice stg 
left join `rax-datamart-dev`.corporate_dmart.dim_invoice inv  
       on stg.invoice_key = inv.invoice_key 
left join `rax-staging-dev`.stage_two_dw.account_bdom bdom 
       on bdom.account_no = inv.billing_application_account_number 
left join `rax-staging-dev`.stage_three_dw.dim_time dt 
       on dt.time_key = stg.invoice_date_utc_key
       where trg.invoice_key=stg.invoice_key
       and trg.invoice_date_utc_key = stg.invoice_date_utc_key;

-----------------------------------------------------------------------------------------------------------
UPDATE `rax-staging-dev`.stage_two_dw.stage_fact_invoice_line_item_cloud_invoice
SET
    Updated_DesiredBillingDate=Time_Day_Number
FROM
     `rax-staging-dev`.stage_two_dw.stage_fact_invoice_line_item_cloud_invoice Stg
INNER JOIN
  `rax-staging-dev`.stage_three_dw.dim_time B
ON Stg.Date_Month_Key= cast(substr( cast(B.Time_Key as string),1,6) as  int64)
WHERE
    Time_Last_Day_Month_Flag=1
AND Stg.Updated_DesiredBillingDate > Time_Day_Number;
-----------------------------------------------------------------------------------------------------------
-- Duplicate issue
UPDATE  `rax-staging-dev`.stage_two_dw.stage_fact_invoice_line_item_cloud_invoice  trg
SET
    trg.DesiredBillingInvoice_Date=PARSE_DATE("%m/%d/%Y",cast(concat(cast(extract(month from DT.Time_FULL_Date )  as string)
												, '/' , cast(a.Updated_DesiredBillingDate as string) , '/' 
												,cast(extract(year from DT.Time_FULL_Date) as string)
												) as string) )
FROM
  `rax-staging-dev`.stage_two_dw.stage_fact_invoice_line_item_cloud_invoice    A
  left join `rax-staging-dev`.stage_three_dw.dim_time DT on DT.Time_Key= A.Invoice_DATE_UTC_KEY
WHERE
    PARSE_DATE("%m/%d/%Y",cast(concat(cast(extract(month from DT.Time_FULL_Date )  as string)
												, '/' , cast(A.Updated_DesiredBillingDate as string) , '/' 
												,cast(extract(year from DT.Time_FULL_Date) as string)
												) as string) ) <> trg.DesiredBillingInvoice_Date;
	
-----------------------------------------------------------------------------------------------------------
-- Duplicate issue
UPDATE `rax-staging-dev`.stage_two_dw.stage_fact_invoice_line_item_cloud_invoice trg
SET
  trg.Previous_Invoice_Date=		date_sub(cast(A.DesiredBillingInvoice_Date as date),interval 1 month) ,
  trg.Previous_Invoice_Date_No_Time=date_sub(cast(A.DesiredBillingInvoice_Date as date),interval 1 month)      
FROM
     `rax-staging-dev`.stage_two_dw.stage_fact_invoice_line_item_cloud_invoice    A
         left join  `rax-staging-dev`.stage_three_dw.dim_time DT on DT.Time_Key= A.Invoice_DATE_UTC_KEY
WHERE
    PARSE_DATE("%m/%d/%Y",cast(concat(cast(extract(month from DT.Time_FULL_Date )  as string)
												, '/' , cast(A.Updated_DesiredBillingDate as string) , '/' 
												,cast(extract(year from DT.Time_FULL_Date) as string)
												) as string) ) <> trg.DesiredBillingInvoice_Date;
												
-------------------------------------------------------------------------------------------------------------------------
-- Duplicate issue
UPDATE `rax-staging-dev`.stage_two_dw.stage_fact_invoice_line_item_cloud_invoice trg
SET 
trg.Prior_Month_Day_number= extract(day from `rax-staging-dev`.bq_functions.udf_lastdayofmonth(date_sub(cast(DT.time_full_date as date),interval 1 month)) )     ,
trg.Days_In_Current_Bill_Period =  Case 
									when CAST(DATETIME_DIFF(A.Previous_Invoice_Date_calc,DT.Time_FULL_Date,DAY) as INT64) < CAST(DATETIME_DIFF(A.Previous_Invoice_Date,A.DesiredBillingInvoice_Date,DAY) as INT64)       
										then CAST(DATETIME_DIFF(A.Previous_Invoice_Date,A.DesiredBillingInvoice_Date,DAY) as INT64) 
									else CAST(DATETIME_DIFF (A.Previous_Invoice_Date_calc,DT.Time_FULL_Date,DAY) as INT64) 
								end
from `rax-staging-dev`.stage_two_dw.stage_fact_invoice_line_item_cloud_invoice    A
	left join `rax-staging-dev`.stage_three_dw.dim_time  DT on DT.Time_Key= A.Invoice_DATE_UTC_KEY
  WHERE TRUE;		 

----------------------------------------------------------------------------------------------------------------------------

UPDATE `rax-staging-dev`.stage_two_dw.stage_fact_invoice_line_item_cloud_invoice fact
SET
fact.Amount_Normalized_USD=ROUND(cast((ifnull(Amount_USD,0)/Days_In_Current_Bill_Period)*30.42 AS NUMERIC),2),
fact.Amount_Normalized_Gbp=ROUND(cast((ifnull(Amount_Gbp,0)/Days_In_Current_Bill_Period)*30.42 AS NUMERIC),2),
fact.Amount_Normalized_Local=ROUND(cast((ifnull(Amount_Local,0)/Days_In_Current_Bill_Period)*30.42 AS NUMERIC),2)
WHERE Is_Normalize =1;

---------------------------------------------------------------------------------------------------------------
--------------------UPDATE NORMALIZED AMOUNT IN Stage_Fact_Invoice_Line_Item_Cloud_Invoice ENDS HERE------------------------
--Loading Adjustment data into temp table #Cloud_Inv_Event_Detail

SELECT * --INTO #TMP_ACC_CON_ADJ 
FROM( SELECT Account_Key,Account_Number,Global_Account_Type,Account_Team_Name,Account_Primary_Contact_ID
				,Account_Billing_Contact_ID,DC.Contact_Key AS Primary_contact_Key,DC2.Contact_Key AS Billing_Contact_Key
			FROM `rax-staging-dev`.stage_three_dw.dim_account DA 
				LEFT JOIN `rax-staging-dev`.stage_three_dw.dim_contact DC  ON DA.Account_Primary_Contact_ID = DC.Contact_NK AND DC.Contact_Current_Record = 1 AND 	 UPPER(DC.Contact_Source_Name) = 'CMS'
				LEFT JOIN `rax-staging-dev`.stage_three_dw.dim_contact DC2  ON DA.Account_Billing_Contact_ID = DC2.Contact_NK AND DC2.Contact_Current_Record = 1 AND UPPER(DC2.Contact_Source_Name) = 'CMS'
		WHERE	DA.Current_record = 1 
	)
	;
	
DELETE FROM `rax-staging-dev`.stage_two_dw.stage_fact_invoice_line_item_cloud_adjustment WHERE TRUE;

INSERT INTO `rax-staging-dev`.stage_two_dw.stage_fact_invoice_line_item_cloud_adjustment
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
		,Currency_Key
		,Transaction_Term
		,Quantity
		,Billing_Days_In_Month
		,Amount_Usd
		,Amount_Normalized_USD
		,Extended_Amount_Usd
		,Unit_Selling_Price_Usd
		,Amount_Gbp
		,Amount_Normalized_Gbp
		,Extended_Amount_Gbp
		,Unit_Selling_Price_Gbp
		,Amount_Local
		,Amount_Normalized_Local
		,Extended_Amount_Local
		,Unit_Selling_Price_Local
		,Is_Normalize
		,Is_Standalone_Fee
		,Is_Prepay
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
        ,Earned_Max_Start_Time_Cst_Key
		,Record_Created_By
		,Record_Created_Datetime
		,Record_Updated_By
		,Record_Updated_Datetime)
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
		,Currency_Key
		,Transaction_Term
		,cast(sum(Quantity)  as NUMERIC )as Quantity
		,Billing_Days_In_Month
		,sum(Amount_Usd) as Amount_USD
		,sum(Amount_Usd) as Amount_Normalized_USD
		,sum(Extended_Amount_Usd) as Extended_Amount_Usd
		,sum(Unit_Selling_Price_Usd) as Unit_Selling_Price_Usd
		,sum(Amount_Gbp) as Amount_Gbp
		,sum(Amount_Gbp) as Amount_Normalized_Gbp
		,sum(Extended_Amount_Gbp) as Extended_Amount_Gbp
		,sum(Unit_Selling_Price_Gbp) as Unit_Selling_Price_Gbp
		,sum(Amount_Local) as Amount_Local
		,sum(Amount_Local) as Amount_Normalized_Local
		,sum(Extended_Amount_Local) as Extended_Amount_Local
		,sum(Unit_Selling_Price_Local) as Unit_Selling_Price_Local
		,Is_Normalize
		,Is_Standalone_Fee
		,Is_Prepay
		,Is_Back_Bill
		,Is_Fastlane
		,cast(Bill_Created_Date_Cst_Key as int64)  as Bill_Created_Date_Cst_Key
		,Bill_Created_Time_Cst_Key
		,cast(Event_Min_Created_Date_Time_Utc as datetime) as  Event_Min_Created_Date_Time_Utc
		,cast(Event_Min_Created_Date_Time_Cst  as datetime) as Event_Min_Created_Date_Time_Cst
		,cast(Event_Max_Created_Date_Time_Utc  as datetime) as Event_Max_Created_Date_Time_Utc
		,cast(Event_Max_Created_Date_Time_Cst  as datetime) as Event_Max_Created_Date_Time_Cst
		,cast(Earned_Min_Start_Date_Time_Utc   as datetime) as Earned_Min_Start_Date_Time_Utc
		,cast(Earned_Min_Start_Date_Time_Cst   as datetime) as Earned_Min_Start_Date_Time_Cst
		,cast(Earned_Max_Start_Date_Time_Utc   as datetime) as Earned_Max_Start_Date_Time_Utc
		,cast(Earned_Max_Start_Date_Time_Cst   as datetime) as Earned_Max_Start_Date_Time_Cst
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
        ,Earned_Max_Start_Time_Cst_Key
		,Record_Created_By
		,Record_Created_Datetime
		,Record_Updated_By
		,Record_Updated_Datetime
FROM (SELECT
		Time_Month_Key as Date_Month_Key
		,R.Revenue_Type_Key
		,CASE WHEN `rax-staging-dev`.bq_functions.udf_is_numeric(Src.account)=0 THEN -1 ELSE ifnull(A.Account_Key,0) END As Account_Key
		,CASE WHEN upper(IFNULL(A.ACCOUNT_TEAM_NAME,'NULL'))  IN ('NULL','UNASSIGNED') THEN -1 ELSE ifnull(Account_Team.Team_Key,0) END AS Team_Key
		,CASE WHEN upper(ifnull(A.Account_Primary_Contact_ID,'NULL')) IN ('NULL','-99','UNKNOWN') THEN -1 ELSE ifnull(A.Primary_contact_Key,0) END AS Primary_Contact_Key
		,CASE WHEN upper(ifnull(A.Account_Billing_Contact_ID,'NULL')) IN ('NULL', 'UNKNOWN') THEN -1 ELSE ifnull(A.Billing_Contact_Key,0) END AS Billing_Contact_Key
		,-1 AS Product_Key
		,-1 AS Datacenter_Key
		,-1 AS Device_Key
		,CASE WHEN Src.Item_Tag IS NULL THEN -1 ELSE  ifnull(Item.Item_Key,0) END AS Item_Key
		,CASE WHEN `rax-staging-dev`.bq_functions.udf_is_numeric(Src.glsub7_product) = 0 OR Src.glsub7_product  = '000' THEN -1 ELSE ifnull(Glp.GL_Product_Key,0) END AS GL_Product_Key
		,CASE WHEN `rax-staging-dev`.bq_functions.udf_is_numeric(Src.glsub3_acct_subprod) = 0 OR Src.glsub3_acct_subprod  = '000' THEN -1  ELSE ifnull(GlA.GL_Account_Key,0) END AS GL_Account_Key
		,CASE 
			WHEN 
				CONCAT(cast(Src.EBI_GL_ID AS STRING),'-',cast(Src.GL_Segment AS STRING),'-',cast(Src.GLAcct_record_type AS STRING),'-',cast(Src.GLAcct_attribute AS STRING)) IS NULL THEN -1 ELSE ifnull(GlConfig.GLid_Configuration_Key,0) 
		 END AS GLid_Configuration_Key
		,-1 AS Payment_Term_Key
		,CASE WHEN SRC.Event_POID_Type IS NULL THEN -1 ELSE ifnull(Be.Event_Type_Key,0) END AS Event_Type_Key
		,CASE WHEN ifnull(upper(SRC.Invoice_Nk),'NULL') IN ('NULL','N/A') THEN -1 ELSE ifnull(Inv.Invoice_Key,0) END AS Invoice_Key
		,-1 AS Invoice_Attribute_Key
		,-1 AS Raw_Start_Month_Key
		,cast(ifnull(FORMAT_DATE("%Y%m%d",date(Src.Credit_Effective_Date) ) ,'19000101') as int64) AS Invoice_Date_Utc_Key
		,cast(ifnull(FORMAT_DATE("%Y%m%d",date(Src.Credit_Effective_Date) ) ,'19000101') as int64) AS Bill_Created_Date_Utc_Key
		,cast(IFNULL((extract(Hour from Src.Credit_Effective_Date)* 3600) +(extract(MINUTE from Src.Credit_Effective_Date)* 60) +(extract(SECOND from Src.Credit_Effective_Date)),0) as int64) AS Bill_Created_Time_Utc_Key
		,-1 AS Bill_Start_Date_Utc_Key
		,-1 AS Bill_End_Date_Utc_Key
		,-1 AS Prepay_Start_Date_Utc_Key
		,-1 AS Prepay_End_Date_Utc_Key
		,CASE WHEN Src.CURRENCY_ID IS NULL THEN -1 ELSE ifnull(Cur.Currency_Key,0) END AS Currency_Key
		,0 AS Transaction_Term
		,Src.Quantity AS Quantity
		,Src.Billing_Days_In_Month
		,ifnull(Src.Total_USD,0) AS Amount_Usd
		,0 AS Extended_Amount_Usd
		,0 AS Unit_Selling_Price_Usd
		,ifnull(Src.Total_GBP,0) Amount_Gbp
		,0 AS Extended_Amount_Gbp
		,0 AS Unit_Selling_Price_Gbp
		,ifnull(SRC.Total,0) AS Amount_Local
		,0 AS Extended_Amount_Local
		,0 AS Unit_Selling_Price_Local
		,0 AS Is_Normalize
		,0 AS Is_Standalone_Fee
		,0 AS Is_Prepay
		,0 AS Is_Back_Bill
		,0 AS Is_Fastlane
		,cast(ifnull(FORMAT_DATE("%Y%m%d",date(`rax-staging-dev`.bq_functions.get_utc_to_cst_time(date(Src.Credit_Effective_Date)) )) ,'19000101') as int64) AS Bill_Created_Date_Cst_Key
		,cast(IFNULL((extract(Hour from `rax-staging-dev`.bq_functions.get_utc_to_cst_time(date(Src.Credit_Effective_Date)))* 3600) +(extract(MINUTE from `rax-staging-dev`.bq_functions.get_utc_to_cst_time(date(Src.Credit_Effective_Date)))* 60) +(extract(SECOND from `rax-staging-dev`.bq_functions.get_utc_to_cst_time(date(Src.Credit_Effective_Date)))),0) as int64) AS Bill_Created_Time_Cst_Key
		,'1970-01-01' AS Event_Min_Created_Date_Time_Utc
		,'1970-01-01' AS Event_Min_Created_Date_Time_Cst
		,'1970-01-01' AS Event_Max_Created_Date_Time_Utc
		,'1970-01-01' AS Event_Max_Created_Date_Time_Cst
		,'1970-01-01' AS Earned_Min_Start_Date_Time_Utc
		,'1970-01-01' AS Earned_Min_Start_Date_Time_Cst
		,'1970-01-01' AS Earned_Max_Start_Date_Time_Utc
		,'1970-01-01' AS Earned_Max_Start_Date_Time_Cst
		,25 AS Source_System_Key
		,-1 AS Event_Min_Created_Date_Utc_Key
        ,-1 AS Event_Min_Created_Time_Utc_Key
        ,-1 AS Event_Max_Created_Date_Utc_Key
        ,-1 AS Event_Max_Created_Time_Utc_Key
		,-1 AS Event_Min_Created_Date_Cst_Key
        ,-1 AS Event_Min_Created_Time_Cst_Key
        ,-1 AS Event_Max_Created_Date_Cst_Key
        ,-1 AS Event_Max_Created_Time_Cst_Key
        ,-1 AS Earned_Min_Start_Date_Utc_Key
        ,-1 AS Earned_Min_Start_Time_Utc_Key
        ,-1 AS Earned_Max_Start_Date_Utc_Key
        ,-1 AS Earned_Max_Start_Time_Utc_Key
        ,-1 AS Earned_Min_Start_Date_Cst_Key
        ,-1 AS Earned_Min_Start_Time_Cst_Key
        ,-1 AS Earned_Max_Start_Date_Cst_Key
        ,-1 AS Earned_Max_Start_Time_Cst_Key
		,'udsp_etl_Fact_Invoice_Line_Item_Cloud_Inv_and_Adj' AS Record_Created_By
		,current_datetime() AS Record_Created_Datetime
		,'udsp_etl_Fact_Invoice_Line_Item_Cloud_Inv_and_Adj' AS Record_Updated_By
		,current_datetime() AS Record_Updated_Datetime
FROM
	`rax-staging-dev`.stage_two_dw.stage_credits_brm Src  
	Inner Join  (
		SELECT--INTO	#Modified_Adjustment_Cloud
			distinct item_no As Bill_Number
		FROM `rax-staging-dev`.stage_one.raw_credits_brm_daily_stage
	) modinv --#Modified_Adjustment_Cloud modinv 
	ON src.item_no =modinv.bill_number
	Left Join `rax-staging-dev`.stage_three_dw.dim_revenue_type R  ON 'ADJUSTMENT'= upper(R.revenue_type_name)
	LEFT JOIN 
	(
		SELECT * --INTO #TMP_ACC_CON_ADJ 
		FROM( SELECT Account_Key,Account_Number,Global_Account_Type,Account_Team_Name,Account_Primary_Contact_ID
						,Account_Billing_Contact_ID,DC.Contact_Key AS Primary_contact_Key,DC2.Contact_Key AS Billing_Contact_Key
					FROM `rax-staging-dev`.stage_three_dw.dim_account DA 
						LEFT JOIN `rax-staging-dev`.stage_three_dw.dim_contact DC  ON DA.Account_Primary_Contact_ID = DC.Contact_NK AND DC.Contact_Current_Record = 1 AND 	 UPPER(DC.Contact_Source_Name) = 'CMS'
						LEFT JOIN `rax-staging-dev`.stage_three_dw.dim_contact DC2  ON DA.Account_Billing_Contact_ID = DC2.Contact_NK AND DC2.Contact_Current_Record = 1 AND UPPER(DC2.Contact_Source_Name) = 'CMS'
				WHERE	DA.Current_record = 1 
			)
	) A --#TMP_ACC_CON_ADJ A 
	ON	Src.Account=A.Account_Number AND Src.Global_Account_Type = A.Global_Account_Type
	LEFT JOIN `rax-staging-dev`.stage_three_dw.dim_team account_Team  
				ON	A.Account_Team_name=Account_Team.Team_Name AND Account_Team.Current_Record=1
	LEFT JOIN `rax-datamart-dev`.corporate_dmart.dim_item Item 
				ON Src.Item_Tag = Item.Item_Tag AND Item.Current_Record = 1
	LEFT JOIN `rax-datamart-dev`.corporate_dmart.dim_gl_product GlP  
				ON Src.glsub7_product=GlP.GL_Product_Code AND GlP.Current_Record = 1 AND upper(GlP.Source_System_Name)='BRM'
	LEFT JOIN `rax-datamart-dev`.corporate_dmart.dim_gl_account GlA  
				ON Src.glsub3_acct_subprod=GlA.GL_Account_Id AND GlA.Current_Record = 1 AND upper(GlA.Source_System_Name)='BRM'
	LEFT JOIN `rax-datamart-dev`.corporate_dmart.dim_glid_configuration GlConfig  
				ON upper(GLConfig.GLid_Configuration_Nk)=upper(concat(cast(Src.EBI_GL_ID AS string),'-',cast(Src.GL_Segment AS string),'-',cast(Src.GLAcct_record_type AS string),'-',cast(Src.GLAcct_attribute AS string)))
	LEFT JOIN `rax-staging-dev`.stage_three_dw.dim_currency Cur  
				ON Src.Currency_ID=Cur.Currency_Iso_Numeric_Code
	LEFT JOIN `rax-datamart-dev`.corporate_dmart.dim_invoice Inv  
				ON src.Invoice_Nk = Inv.Invoice_Nk
	LEFT JOIN `rax-datamart-dev`.corporate_dmart.dim_billing_events Be  
				ON Src.Event_POID_Type=Be.Event_Type AND Be.current_record = 1
) a
GROUP BY
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
		,Currency_Key
		,Transaction_Term
		,Billing_Days_In_Month
		,Is_Normalize
		,Is_Standalone_Fee
		,Is_Prepay
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
        ,Earned_Max_Start_Time_Cst_Key
		,Record_Created_By
		,Record_Created_Datetime
		,Record_Updated_By
		,Record_Updated_Datetime
		
		;
------------------------------------------------------------------------------------------------------------------

DELETE corporate_dmart.fact_invoice_line_item_cloud 
where invoice_key in(
 select trg.invoice_key 
FROM corporate_dmart.fact_invoice_line_item_cloud trg 
Left Join corporate_dmart.dim_invoice inv  on trg.invoice_key=inv.invoice_key
  inner join  (
					SELECT DISTINCT -- #Modified_Invoices_Cloud
						BILL_NO AS Invoice
					FROM  
						`rax-staging-dev`.stage_one.raw_invitemeventdetail_daily_stage
					union all
					SELECT
						Invoice
					FROM
						(
							SELECT DISTINCT  --INTO   #missing_BILL_NO
								BILL_NO		AS Invoice,
								Time_Month_Key

							FROM
								`rax-staging-dev`.stage_one.raw_brm_invoice_aggregate_total A 
							WHERE
								`Exclude`=0
							AND  (CURRENT_TOTAL)<>0  
							AND ( upper(BILL_NO) not like '%EBS%' AND upper(BILL_NO) not like '%EVAPT%')
							AND NOT EXISTS (SELECT
								distinct Invoice.Bill_Number Invoice
							FROM
								`rax-datamart-dev`.corporate_dmart.fact_invoice_line_item_cloud fact 
								inner join `rax-datamart-dev`.corporate_dmart.dim_invoice invoice 
								on fact.invoice_key=invoice.invoice_key
								where invoice.bill_number=a.bill_no
								--group by invoice.bill_number
							  )
							AND A.Time_Month_Key  >= `rax-staging-dev`.bq_functions.udf_yearmonth_nohyphen(HMDB_END)

						) A--#missing_BILL_NO A
  ) modinv --#modified_invoices_cloud modinv 
  on inv.bill_number = modinv.invoice
  left join corporate_dmart.dim_billing_events billingevents  on trg.event_type_key=billingevents.event_type_key
  left join corporate_dmart.dim_item item  on trg.item_key=item.item_key
  where 
  (
	lower(billingevents.event_type) not like '%adjustment%' and lower(billingevents.event_type) not like '%refund%' and lower(ifnull(item.item_name,'null')) <> 'adjustment')
  );
-------------------------------------------------------------------------------------------------------------------
DELETE `rax-datamart-dev`.corporate_dmart.fact_invoice_line_item_cloud
where Invoice_Key in(
select Fact.Invoice_Key
  from `rax-datamart-dev`.corporate_dmart.fact_invoice_line_item_cloud fact 
  left join `rax-datamart-dev`.corporate_dmart.dim_invoice inv  on fact.invoice_key=inv.invoice_key
  inner join  (
					SELECT DISTINCT -- #Modified_Invoices_Cloud
						BILL_NO AS Invoice
					FROM  
						`rax-staging-dev`.stage_one.raw_invitemeventdetail_daily_stage
					union all
					SELECT
						Invoice
					FROM
						(
							SELECT DISTINCT  --INTO   #missing_BILL_NO
								BILL_NO		AS Invoice,
								Time_Month_Key

							FROM
								`rax-staging-dev`.stage_one.raw_brm_invoice_aggregate_total A 
							WHERE
								`Exclude`=0
							AND  (CURRENT_TOTAL)<>0  
							AND ( upper(BILL_NO) not like '%EBS%' AND upper(BILL_NO) not like '%EVAPT%')
							AND NOT EXISTS (SELECT
								distinct Invoice.Bill_Number Invoice
							FROM
								`rax-datamart-dev`.corporate_dmart.fact_invoice_line_item_cloud fact 
								inner join `rax-datamart-dev`.corporate_dmart.dim_invoice invoice 
								on fact.invoice_key=invoice.invoice_key
								where invoice.bill_number=a.bill_no
								--group by invoice.bill_number
							  )
							AND A.Time_Month_Key  >= `rax-staging-dev`.bq_functions.udf_yearmonth_nohyphen(HMDB_END)

						) A--#missing_BILL_NO A
  ) modinv --#modified_adjustment_cloud modinv 
  on inv.bill_number = modinv.invoice
  left join `rax-datamart-dev`.corporate_dmart.dim_billing_events billingevents  on fact.event_type_key=billingevents.event_type_key
  left join `rax-datamart-dev`.corporate_dmart.dim_item item  on fact.item_key=item.item_key
  where 
  (
  lower(billingevents.event_type) like '%adjustment%' or lower(billingevents.event_type)  like '%refund%' or lower(ifnull(item.item_name,'null')) like '%adjustment%')
  );
-------------------------------------------------------------------------------------------------------------------
--Inserting Invoice data into Fact table from Stage table
INSERT INTO `rax-datamart-dev`.corporate_dmart.fact_invoice_line_item_cloud
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
		   ,Amount_Normalized_USD
           ,Extended_Amount_Usd
           ,Unit_Selling_Price_Usd
           ,Amount_Gbp
		   ,Amount_Normalized_Gbp
           ,Extended_Amount_Gbp
           ,Unit_Selling_Price_Gbp
           ,Amount_Local
		   ,Amount_Normalized_Local
           ,Extended_Amount_Local
           ,Unit_Selling_Price_Local
		   ,Is_Normalize
           ,Is_Standalone_Fee
		   ,Is_Prepay
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
 ,Amount_Normalized_USD
 ,Extended_Amount_Usd
 ,Unit_Selling_Price_Usd
 ,Amount_Gbp
 ,Amount_Normalized_Gbp
 ,Extended_Amount_Gbp
 ,Unit_Selling_Price_Gbp
 ,Amount_Local
 ,Amount_Normalized_Local
 ,Extended_Amount_Local
 ,Unit_Selling_Price_Local
 ,Is_Normalize
 ,Is_Standalone_Fee
 ,Is_Prepay
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
 from `rax-staging-dev`.stage_two_dw.stage_fact_invoice_line_item_cloud_invoice invoice 
 inner join `rax-staging-dev`.stage_three_dw.dim_record_source drc 
 on invoice.record_created_by = drc.record_source_name;
 -------------------------------------------------------------------------------------------------------------------
INSERT INTO `rax-datamart-dev`.corporate_dmart.fact_invoice_line_item_cloud
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
		   ,Amount_Normalized_USD
           ,Extended_Amount_Usd
           ,Unit_Selling_Price_Usd
           ,Amount_Gbp
		   ,Amount_Normalized_Gbp
           ,Extended_Amount_Gbp
           ,Unit_Selling_Price_Gbp
           ,Amount_Local
		   ,Amount_Normalized_Local
           ,Extended_Amount_Local
           ,Unit_Selling_Price_Local
		   ,Is_Normalize
           ,Is_Standalone_Fee
		   ,Is_Prepay
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
 ,Amount_Normalized_USD
 ,Extended_Amount_Usd
 ,Unit_Selling_Price_Usd
 ,Amount_Gbp
 ,Amount_Normalized_Gbp
 ,Extended_Amount_Gbp
 ,Unit_Selling_Price_Gbp
 ,Amount_Local
 ,Amount_Normalized_Local
 ,Extended_Amount_Local
 ,Unit_Selling_Price_Local
 ,Is_Normalize
 ,Is_Standalone_Fee
 ,Is_Prepay
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
 from `rax-staging-dev`.stage_two_dw.stage_fact_invoice_line_item_cloud_adjustment adjustment 
 inner join `rax-staging-dev`.stage_three_dw.dim_record_source drc 
 on adjustment.record_created_by = drc.record_source_name
 ;
-------------------------------------------------------------------------------------------------------------------
/*
		EXEC msdb..Usp_send_cdosysmail 'no_replyrackspace.com','Rahul.Chourasiyarackspace.com,Harish.Gowthamrackspace.com','NRD FACT Invoice Line Item Cloud Invoice and Adjustment load JOB SUCCESS',''
END TRY
BEGIN CATCH

       --ROLLBACK TRANSACTION

              DECLARE subject nvarchar(max) = 'NRD Fact Load Failure Notification';
              DECLARE body nvarchar(max) = 'Data Transformation Failed during Fact Invoice Line Item Cloud Invoice and Adjustment Load' 
              + CHAR(10) + CHAR(13) + 'Error Number:  ' + CAST(ERROR_NUMBER() AS nvarchar(50))
              + CHAR(10) + CHAR(13) + 'Error Severity:  ' + CAST(ERROR_SEVERITY() AS nvarchar(50))
              + CHAR(10) + CHAR(13) + 'Error State:  ' + CAST(ERROR_STATE() AS nvarchar(50))
              + CHAR(10) + CHAR(13) + 'Error Procedure:  ' + CAST(ERROR_PROCEDURE() AS nvarchar(100))
              + CHAR(10) + CHAR(13) + 'Error Line:  ' + CAST(ERROR_LINE() AS nvarchar(50))
              + CHAR(10) + CHAR(13) + 'Error Message: ' + ERROR_MESSAGE()
              + CHAR(10) + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) + CHAR(13) + 'This is a system generated mail. DO NOT REPLY  ';
              DECLARE to nvarchar(max) = 'Harish.Gowthamrackspace.com';
              DECLARE profile_name sysname = 'Jobs';
              EXEC msdb.dbo.sp_send_dbmail profile_name = profile_name,
              recipients = to, subject = subject, body = body;

		THROW    
       END CATCH

END

GO
*/

END;
