create or replace view `rax-datamart-dev`.corporate_dmart.fact_invoice_event_detail_emailapps_tmp_fact_temp_data
as
SELECT master_unique_id AS Transaction_key  --INTO #Tmp_FACT_TEMP_DATA  
,`rax-staging-dev`.bq_functions.udf_yearmonth_nohyphen(Src.Invoice_Date) AS Date_Month_Key  
,19 AS Revenue_Type_Key  
, CASE WHEN `rax-staging-dev`.bq_functions.udf_is_numeric(Src.Account)=0 THEN -1 ELSE ifnull(A.Account_Key,0) END As Account_Key  
,CASE WHEN upper(ifnull(A.Account_Team_Name,'NULL'))  IN ('null','unassigned') THEN -1 ELSE ifnull(Account_Team.Team_Key,0) END AS Team_Key  
,CASE WHEN upper(ifnull(A.Account_Primary_Contact_ID,'NULL')) IN ('NULL','-99','UNKNOWN') THEN -1 ELSE ifnull(A.Primary_contact_Key,0) END AS Primary_Contact_Key  
,CASE WHEN upper(ifnull(A.Account_Billing_Contact_ID,'NULL')) IN ('NULL', 'UNKNOWN') THEN -1 ELSE ifnull(A.Billing_Contact_Key,0) END AS Billing_Contact_Key  
,CASE WHEN `rax-staging-dev`.bq_functions.udf_is_numeric(cast(SRC.Service_ID as string)) = 0  THEN -1 ELSE ifnull(P.Product_Key,0) END AS Product_Key  
,CASE WHEN Src.Item_Tag IS NULL THEN -1 ELSE  ifnull(Item.Item_Key,0) END AS Item_Key  
,CASE WHEN `rax-staging-dev`.bq_functions.udf_is_numeric(GL.glsub7_product) = 0 OR GL.glsub7_product  = '000' THEN -1 ELSE ifnull(Glp.GL_Product_Key,0) END AS GL_Product_Key  
,CASE WHEN `rax-staging-dev`.bq_functions.udf_is_numeric(GL.glsub3_acct_subprod) = 0 OR GL.glsub3_acct_subprod  = '000' THEN -1  ELSE ifnull(GlA.GL_Account_Key,0) END AS GL_Account_Key  
,CASE 
	WHEN CONCAT(cast(Src.EBI_GL_ID AS STRING),'-',cast(Src.GL_Segment AS STRING),'-',cast(GL.GLAcct_record_type AS STRING),'-',cast(GL.GLAcct_attribute AS STRING)) IS NULL 
		THEN -1  
	ELSE ifnull(GlConfig.GLid_Configuration_Key,0) 
 END AS GLid_Configuration_Key  
,CASE WHEN SRC.EVENT_TYPE IS NULL THEN -1 ELSE ifnull(Be.Event_Type_Key,0) END AS Event_Type_Key  
,CASE WHEN UPPER(ifnull(SRC.Invoice_Nk,'NULL')) IN ('NULL','N/A') THEN -1 ELSE ifnull(Inv.Invoice_Key,0) END AS Invoice_Key  
,CASE WHEN UPPER(ifnull(SRC.Invoice_Attribute_NK,'NULL')) IN ('NULL','N/A') THEN -1 ELSE ifnull(Invattr.Invoice_Attribute_Key,0) END AS Invoice_Attribute_Key  
,CASE WHEN UPPER(ifnull(SRC.Oracle_Location,'NULL')) IN ('NULL','N/A') THEN -1 ELSE ifnull(Datacenter_Key,0) END AS Datacenter_Key  
,Src.Invoice_Date_Utc_Key  
,Src.Bill_Created_Date_Utc_Key  
,Src.Bill_Created_Time_Utc_Key  
,ifnull(`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Src.Bill_Created_Date_Cst),0) AS Bill_Created_Date_Cst_Key   
,IFNULL((extract(Hour from Src.Bill_Created_Date_Cst)* 3600) + (extract(MINUTE from Src.Bill_Created_Date_Cst)* 60) + (extract(SECOND from Src.Bill_Created_Date_Cst)),0)  AS Bill_Created_Time_Cst_Key   
,Src.Bill_Start_Date_Utc_Key  
,Src.Bill_End_Date_Utc_Key  
,Src.Event_Created_Date_Utc_Key  
,Src.Event_Created_Time_Utc_Key  
,19000101 AS Event_Created_Date_Cst_Key    
,IFNULL((extract(Hour from cast(Src.Event_Created_date_Cst as datetime))* 3600) + (extract(MINUTE from cast(Src.Event_Created_date_Cst as datetime))* 60) + (extract(SECOND from cast(Src.Event_Created_date_Cst as datetime))),0)  AS Event_Created_Time_Cst_Key      
,Src.Event_Start_Date_Utc_Key  
,Src.Event_Start_Time_Utc_Key  
,ifnull(`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(cast(Src.Event_Start_Date_Cst as date)),0) AS Event_Start_Date_Cst_Key   
,IFNULL((extract(Hour from cast(Src.Event_Start_Date_Cst as datetime))* 3600) + (extract(MINUTE from cast(Src.Event_Start_Date_Cst as datetime))* 60) + (extract(SECOND from cast(Src.Event_Start_Date_Cst as datetime))),0)  AS Event_Start_Time_Cst_Key    
,Src.Event_End_Date_Utc_Key  
,Src.Event_End_Time_Utc_Key  
,ifnull(`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(cast(Src.Event_End_Date_Cst as date)),0) AS Event_End_Date_Cst_Key   
,IFNULL((extract(Hour from cast(Src.Event_End_Date_Cst as datetime))* 3600) + (extract(MINUTE from cast(Src.Event_End_Date_Cst as datetime))* 60) + (extract(SECOND from cast(Src.Event_End_Date_Cst as datetime))),0) AS Event_End_Time_Cst_Key   
,Src.Earned_Start_Date_Utc_Key  
,Src.Earned_Start_Time_Utc_Key  
,ifnull(`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(cast(Src.Event_End_Date_Cst as date)),0) AS Earned_Start_Date_Cst_Key   
,IFNULL((extract(Hour from cast(Src.Event_End_Date_Cst as datetime))* 3600) + (extract(MINUTE from cast(Src.Event_End_Date_Cst as datetime))* 60) + (extract(SECOND from cast(Src.Event_End_Date_Cst as datetime))),0)  AS Earned_Start_Time_Cst_Key    
,Src.Earned_End_Date_Utc_Key  
,Src.Earned_End_Time_Utc_Key  
,ifnull(`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(cast(Src.Event_Earned_End_Date_Cst as date)),0) AS Earned_End_Date_Cst_Key 
,Src.Pre_Pay_Start_Date_Utc_Key AS Pre_Pay_Start_Date_Utc_Key  
,Src.Prepay_End_Date_Utc_Key AS Prepay_End_Date_Utc_Key  
,IFNULL((extract(Hour from cast(Src.Event_Earned_End_Date_Cst as datetime))* 3600) + (extract(MINUTE from cast(Src.Event_Earned_End_Date_Cst as datetime))* 60) + (extract(SECOND from cast(Src.Event_Earned_End_Date_Cst as datetime))),0)  AS Earned_End_Time_Cst_Key   
,CASE WHEN SRC.CURRENCY IS NULL THEN -1 ELSE ifnull(Cur.Currency_Key,0) END AS Currency_Key  
,Src.Quantity AS Quantity  
,Src.Billing_Days_In_Month  
,ifnull(ROUND(cast(Src.Total*USDRate.Exchange_Rate_Exchange_Rate_Value AS decimal),6),0) AS Total_Amount_Usd  
,ifnull(ROUND(cast(Src.Total*GBPRate.Exchange_Rate_Exchange_Rate_Value AS decimal),6),0) AS Total_Amount_Gbp  
,ifnull(ROUND(cast(Src.Total AS decimal),6),0) AS Total_Amount_Local  
,Src.Is_Standalone_Fee  
,Src.Is_Back_Bill  
,Src.Is_Fastlane        
,Is_Transaction_Successful AS Is_Transaction_Completed   
,CURRENT_DATETIME() AS Record_Created_Datetime  
,DRS.Record_Source_KEY AS  Record_Created_By  
,CURRENT_DATETIME() AS Record_Updated_Datetime  
,DRS.Record_Source_KEY AS Record_Updated_By  
,25 AS Source_System_Key  
,SRC.PREPAY_TERM as PREPAY_TERM	
		from   `rax-datamart-dev`.corporate_dmart.fact_invoice_event_detail_emailapps_tmp_inc_stage_data src--  #tmp_inc_stage_data src    
			left  join `rax-staging-dev`.stage_one.raw_brm_glid_account_config gl    
                              on src.ebi_gl_id= gl.glid_rec_id and src.gl_segment= gl.glseg_name and gl.glacct_record_type in (2,8) and gl.glacct_attribute= 1  
                inner join `rax-staging-dev`.stage_two_dw.stage_brm_glid_acct_atttribute atr   
                              on gl.glacct_attribute= atr.attribute_id  
                inner join `rax-staging-dev`.stage_two_dw.stage_brm_glid_acct_report_type typ   
                              on       gl.glacct_record_type=typ.report_type_id  
 			 left join (
							SELECT * --INTO #TMP_ACC_CON 
							FROM (	select 	account_key,account_number,global_account_type,account_team_name,
											account_primary_contact_id,account_billing_contact_id
											,dc.contact_key as primary_contact_key,
											dc2.contact_key as billing_contact_key
									from 	  `rax-staging-dev`.stage_three_dw.dim_account da 
									left join `rax-staging-dev`.stage_three_dw.dim_contact dc  on da.account_primary_contact_id = dc.contact_nk and dc.contact_current_record = 1 	 and lower(dc.contact_source_name )	 = 'mailtrust'
									left join `rax-staging-dev`.stage_three_dw.dim_contact dc2  on da.account_billing_contact_id = dc2.contact_nk and dc2.contact_current_record = 1 and lower(dc2.contact_source_name) = 'mailtrust'
									where da.current_record = 1 
								)
						) a --#tmp_acc_con a 
			 on   src.account=a.account_number and src.global_account_type = a.global_account_type 
                left join `rax-staging-dev`.stage_three_dw.dim_team account_team    
                              on       a.account_team_name=account_team.team_name and account_team.current_record=1  
                left join `rax-staging-dev`.stage_three_dw.dim_product p    
                              on cast(src.service_id as string)=p.product_resource_code_nk and p.product_current_record_flag=1   
                              and p.product_record_source_system_name='mailtrust'  
                left join `rax-datamart-dev`.corporate_dmart.dim_gl_product glp    
                              on src.oracle_product_id=glp.gl_product_code and glp.current_record = 1 and glp.source_system_name='brm'  
                left join `rax-datamart-dev`.corporate_dmart.dim_gl_account gla    
                              on src.gl_account=gla.gl_account_id and gla.current_record = 1 and gla.source_system_name='brm'  
                left join `rax-datamart-dev`.corporate_dmart.dim_glid_configuration glconfig     
				on  lower(cast(glconfig.glid_configuration_nk as  string))=lower(concat(cast(src.ebi_gl_id as string),'-',cast(src.gl_segment as string),'-',cast(gl.glacct_record_type as string),'-',cast(gl.glacct_attribute as string) ) )
                left join `rax-datamart-dev`.corporate_dmart.dim_billing_events be    
                              on '/event/activity/rax/sa3'= lower(be.event_type) and be.current_record = 1  
                left join `rax-staging-dev`.stage_three_dw.dim_currency cur    
                              on src.currency=cur.currency_iso_code  
                left join `rax-datamart-dev`.corporate_dmart.dim_item item  on 'sa3' = lower(item.item_tag) and item.current_record = 1    
			 left join `rax-staging-dev`.stage_three_dw.dim_datacenter datacenter1 on src.oracle_location=datacenter1.datacenter_abbr		and datacenter1.datacenter_current_record_flag = 1
                left join `rax-datamart-dev`.corporate_dmart.dim_invoice inv    
                              on src.invoice_nk = inv.invoice_nk  
                left join `rax-datamart-dev`.corporate_dmart.dim_invoice_attribute invattr    
                              on invattr.invoice_attribute_nk=src.invoice_attribute_nk  
                left join `rax-staging-dev`.stage_three_dw.report_exchange_rate usdrate   
                              on cur.currency_iso_code=usdrate.exchange_rate_from_currency_code   
                                     and lower(usdrate.exchange_rate_to_currency_code)='usd'  
                                     and usdrate.exchange_rate_year=src.rate_year  
                                     and usdrate.exchange_rate_month=src.rate_month  
                                     and lower(usdrate.source_system_name)='oracle'  
                left join `rax-staging-dev`.stage_three_dw.report_exchange_rate gbprate   
                              on cur.currency_iso_code=gbprate.exchange_rate_from_currency_code   
                                     and lower(gbprate.exchange_rate_to_currency_code)='gbp'  
                                     and gbprate.exchange_rate_year=src.rate_year  
                                     and gbprate.exchange_rate_month=src.rate_month  
                                     and lower(gbprate.source_system_name)='oracle'  
                left join `rax-staging-dev`.stage_three_dw.dim_record_source drs on src.record_source_name = drs.record_source_name  and drs.record_source_current_record_flag = 1  

;
