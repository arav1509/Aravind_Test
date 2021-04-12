create or replace view `rax-datamart-dev`.corporate_dmart.fact_invoice_event_detail_emailapps_tmp_inc_stage_data 
as
--Stage: Incremental Stage data  
SELECT   src.master_unique_id  --INTO #Tmp_Inc_Stage_Data   
,src.Bill_End_Date  
,src.Invoice_Date
,src.Bill_Created_Date  
,src.Bill_Start_Date  
,'1900-01-01' as Event_Created_Date  
,'1900-01-01'  AS Event_Start_Date  
,'1900-01-01'  AS Event_End_Date  
,'1900-01-01'  AS Event_Earned_Start_Date  
,'1900-01-01'  AS Event_Earned_End_Date  
, IFNULL(SRC.Quantity,0) AS Quantity  
,src.Total  
,'/event/activity/Rax/SA3' AS Event_Type  
,-1 AS Fastlane_Impact_Is_Backbill  
,-1 AS Fastlane_Impact_Sub_Grp_Code  
,SRC.Oracle_Product_Id  
,SRC.service_id  
,src.EBI_GL_ID  
,src.GL_Segment  
,src.Account  
,src.CURRENCY  
,src.Invoice_Nk  
,src.Invoice_Attribute_Nk  
,`rax-staging-dev`.bq_functions.ufn_getdaysinmonth(date(Src.Invoice_Date) ) Billing_Days_In_Month  
,0 AS Is_Standalone_Fee  
,CASE WHEN date('1900-01-01') < Src.Bill_Start_Date then 1  ELSE 0  END  AS Is_Back_Bill  
,0 AS Is_Fastlane     
,'SA3' AS Item_Tag  
,SRC.Oracle_Location
,SRC.GL_Account  
,extract(Year from Src.Invoice_Date) AS Rate_Year  
,extract(Month from Src.Invoice_Date) AS Rate_Month  
,`rax-staging-dev`.bq_functions.get_utc_to_cst_time(Src.bill_start_date) Bill_Start_Date_Cst  
,`rax-staging-dev`.bq_functions.get_utc_to_cst_time(Src.bill_end_Date) Bill_End_Date_Cst  
,`rax-staging-dev`.bq_functions.get_utc_to_cst_time(Src.bill_mod_Date) Bill_Mod_Date_Cst  
,'19000101' AS Item_Effective_Date_Cst  
,'19000101' AS Item_Mod_Date_Cst  
,'19000101' AS Event_Start_Date_Cst  
,'19000101' AS Event_End_Date_Cst  
,'19000101' AS Event_Mod_Date_Cst  
,'19000101' AS Event_Created_Date_Cst  
,'19000101' AS Event_Earned_Start_Date_Cst  
,'19000101' AS Event_Earned_End_Date_Cst  
,`rax-staging-dev`.bq_functions.get_utc_to_cst_time(Src.Bill_Created_Date) Bill_Created_Date_Cst  
,ifnull(`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Src.Invoice_Date),0) AS Invoice_Date_Utc_Key  
,ifnull(`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Src.Bill_Created_Date),0) AS Bill_Created_Date_Utc_Key  
,IFNULL((extract(Hour from Src.Bill_Created_Date)* 3600) + (extract(MINUTE from Src.Bill_Created_Date)* 60) + (extract(SECOND from Src.Bill_Created_Date)),0)  AS Bill_Created_Time_Utc_Key   
,ifnull(`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Src.Bill_Start_Date),0) AS Bill_Start_Date_Utc_Key  
,ifnull(`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Src.Bill_End_Date),0) AS Bill_End_Date_Utc_Key  
,19000101 AS Event_Created_Date_Utc_Key  
,0 AS Event_Created_Time_Utc_Key   
,19000101  AS Event_Start_Date_Utc_Key  
,0 AS Event_Start_Time_Utc_Key  
,19000101  AS Event_End_Date_Utc_Key  
,0 AS Event_End_Time_Utc_Key  
,19000101  AS Earned_Start_Date_Utc_Key  
,0 AS Earned_Start_Time_Utc_Key  
,19000101 AS Earned_End_Date_Utc_Key  
,0 AS Earned_End_Time_Utc_Key   
,1 AS Is_Transaction_Successful   
,ifnull(`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Src.Pre_Pay_Start_Date),0) AS Pre_Pay_Start_Date_Utc_Key 
,ifnull(`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Src.Pre_Pay_End_Date),0) AS Prepay_End_Date_Utc_Key 
,'MailTrust' AS Global_Account_Type  
,'udsp_etl_Load_Fact_Invoice_Event_Detail_EmailApps_Daily_Load'  AS Record_Source_Name  
,PREPAY_TERM
	FROM     
	(  
			SELECT	master_unique_id
					,Bill_End_Date
					,Bill_Created_Date
					,Bill_Start_Date
					,Invoice_Date
					,Quantity
					,Total
					,Oracle_Product_Id
					,service_id  
					,EBI_GL_ID  
					,GL_Segment  
					,Account  
					,CURRENCY  
					,Invoice_Nk  
					,Invoice_Attribute_Nk
					,Oracle_Location
					,GL_Account
					,bill_mod_Date
					,Pre_Pay_Start_Date
					,Pre_Pay_End_Date 
					,PREPAY_TERM
				FROM `rax-staging-dev`.stage_two_dw.stage_email_apps_inv_event_detail STGSRC   
			WHERE (`rax-staging-dev`.bq_functions.udf_is_numeric(STGSRC.Account)=1 AND STGSRC.Load_Date>= (select date_sub( date(max(Load_Date)), interval 1 day) from `rax-staging-dev`.stage_two_dw.stage_email_apps_inv_event_detail )) --AND src.BILL_END_DATE = '2019-08-15  
			UNION  ALL
			SELECT RECSRC.master_unique_id
					,RECSRC.Bill_End_Date
					,RECSRC.Bill_Created_Date
					,RECSRC.Bill_Start_Date
					,RECSRC.Invoice_Date
					,RECSRC.Quantity
					,RECSRC.Total
					,RECSRC.Oracle_Product_Id
					,RECSRC.service_id  
					,RECSRC.EBI_GL_ID  
					,RECSRC.GL_Segment  
					,RECSRC.Account  
					,RECSRC.CURRENCY  
					,RECSRC.Invoice_Nk  
					,RECSRC.Invoice_Attribute_Nk
					,RECSRC.Oracle_Location
					,RECSRC.GL_Account
					,RECSRC.bill_mod_Date
					,RECSRC.Pre_Pay_Start_Date
					,RECSRC.Pre_Pay_End_Date
					,PREPAY_TERM    
				FROM `rax-staging-dev`.stage_two_dw.stage_email_apps_inv_event_detail RECSRC   
			JOIN `rax-staging-dev`.stage_two_dw.recycle_fact_invoice_event_detail_email_and_apps Recycle  
				ON Recycle.Transaction_key = RECSRC.master_unique_id  
	) SRC  
