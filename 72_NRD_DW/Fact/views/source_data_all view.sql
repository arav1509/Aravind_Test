create or replace view `rax-datamart-dev`.corporate_dmart.source_data_all as
	select --INTO #Source_Data_All
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
							   ,Glid_Configuration_Key
							   ,Payment_Term_Key
							   ,Event_Type_Key
							   ,Invoice_Key
							   ,Invoice_Attribute_Key
							   ,Raw_Start_Month_Key
							   ,Invoice_Date_Utc_Key
							   ,Bill_Created_Date_Utc_Key
							   ,Bill_Created_Time_Utc_Key
							   ,Bill_Created_Date_Cst_Key
							   ,Bill_Created_Time_Cst_Key
							   ,Bill_Start_Date_Utc_Key
							   ,Bill_End_Date_Utc_Key
							   ,Prepay_Start_Date_Utc_Key
							   ,Prepay_End_Date_Utc_Key
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
							   ,Extended_Amount_Usd
							   ,Unit_Selling_Price_Usd
							   ,Amount_Gbp
							   ,Extended_Amount_Gbp
							   ,Unit_Selling_Price_Gbp
							   ,Amount_Local
							   ,Extended_Amount_Local
							   ,Unit_Selling_Price_Local
							   ,Is_Standalone_Fee
							   ,Is_Back_Bill
							   ,Is_Fastlane
							   ,Is_Transaction_Completed
							   ,Transaction_key
							   ,Record_Created_Datetime
							   ,Record_Created_By
							   ,Record_Updated_Datetime
							   ,Record_Updated_By
							   ,Source_System_Key
						from (
						Select
							Time_Month_Key As Date_Month_Key,
							19 As revenue_Type_Key,
							CASE WHEN `rax-staging-dev.bq_functions.udf_is_numeric`(Src.account)=0 THEN -1 ELSE IFNULL(A.Account_Key,0) END As Account_Key,
							CASE WHEN IFNULL(A.Account_Team_Name,'NULL')  IN ('NULL','Unassigned') THEN -1 ELSE  IFNULL(Account_Team.Team_Key,0) END As Team_Key,
							CASE WHEN IFNULL(A.Account_Primary_Contact_ID,'NULL') IN ('NULL','-99','UNKNOWN') THEN -1 ELSE IFNULL(A.Primary_contact_Key,0) END As Primary_Contact_Key,
							CASE WHEN IFNULL(A.Account_Billing_Contact_ID,'NULL') IN ('NULL', 'UNKNOWN') THEN -1 ELSE IFNULL(A.Billing_Contact_Key,0) END As Billing_Contact_Key,
							CASE WHEN `rax-staging-dev.bq_functions.udf_is_numeric`(cast(Src.Product_Poid as string))=0 THEN -1 ELSE IFNULL(P.Product_Key,0) END as Product_Key,
							CASE WHEN Src.Oracle_Location IS NULL THEN -1 ELSE  IFNULL(coalesce(DataCenter1.Datacenter_Key,DataCenter2.Datacenter_Key),0) END As Datacenter_Key,
							CASE WHEN `rax-staging-dev.bq_functions.udf_is_numeric`(Src.Server)=0 THEN -1 ELSE IFNULL(D.Device_Key,0) END as Device_Key,
							CASE WHEN Src.Item_Tag IS NULL THEN -1 ELSE  IFNULL(Item.Item_Key,0) END as Item_Key,
							CASE WHEN `rax-staging-dev.bq_functions.udf_is_numeric`(Src.Oracle_Product_ID)=0 OR Src.Oracle_Product_ID='000' THEN -1 ELSE IFNULL(Glp.GL_Product_Key,0) END as GL_Product_Key,
							CASE WHEN `rax-staging-dev.bq_functions.udf_is_numeric`(Src.GL_Account)=0 OR Src.GL_Account='000' THEN -1 ELSE IFNULL(GlA.GL_Account_Key,0) END as GL_Account_Key,
							CASE WHEN CONCAT(CAST(Src.Glid_Rec_Id AS STRING),'-',CAST(Src.GL_Segment AS STRING),'-',CAST(Src.GLAcct_Record_Type AS STRING),'-',CAST(Src.GLAcct_Attribute AS STRING)) IS NULL THEN -1 ELSE IFNULL(GlConfig.GLid_Configuration_Key,0) END as Glid_Configuration_Key,
							CASE WHEN `rax-staging-dev.bq_functions.udf_is_numeric`(cast(Src.payment_term as string))=0 THEN -1 ELSE IFNULL(Pt.Payment_Term_Key,0) END as Payment_Term_Key,
							CASE WHEN Src.Event_Type IS NULL THEN -1 ELSE IFNULL(Be.Event_Type_Key,0) END as Event_Type_Key,
							CASE WHEN Src.invoice_nk IS NULL THEN -1 ELSE IFNULL(Inv.Invoice_Key,0) END As Invoice_Key,
							CASE WHEN Src.invoice_attribute_nk IS NULL THEN -1 ELSE IFNULL(InvAttr.Invoice_Attribute_Key,0) END AS Invoice_Attribute_Key,
							`rax-staging-dev`.bq_functions.udf_yearmonth_nohyphen(PARSE_date("%b-%y",Trx_Raw_Start_Date )) Raw_Start_Month_Key,
							ifnull(FORMAT_DATE("%Y%m%d",date(Src.Invoice_Date) ) ,'19000101') As Invoice_Date_Utc_Key,
							ifnull(FORMAT_DATE("%Y%m%d",date(Src.Bill_Created_Date) ) ,'19000101') As Bill_Created_Date_Utc_Key,
							IFNULL(
									(extract(Hour from Src.Bill_Created_Date)* 3600) +
									(extract(MINUTE from Src.Bill_Created_Date)* 60) +
									(extract(SECOND from Src.Bill_Created_Date))
									,0
								) As Bill_Created_Time_Utc_Key,
							ifnull(FORMAT_DATE("%Y%m%d",date(Src.Bill_Created_Date_Cst) ) ,'19000101') As Bill_Created_Date_Cst_Key,
							IFNULL(
									(extract(Hour from Src.Bill_Created_Date_Cst)* 3600) +
									(extract(MINUTE from Src.Bill_Created_Date_Cst)* 60) +
									(extract(SECOND from Src.Bill_Created_Date_Cst))
									,0
								) As Bill_Created_Time_Cst_Key,
							ifnull(FORMAT_DATE("%Y%m%d",date(Src.Bill_Start_Date) ) ,'19000101') As Bill_Start_Date_Utc_Key,
							ifnull(FORMAT_DATE("%Y%m%d",date(Src.Bill_End_Date) ) ,'19000101')As Bill_End_Date_Utc_Key,
							ifnull(FORMAT_DATE("%Y%m%d",date(Src.Ded_Prepay_Start) ) ,'19000101') As Prepay_Start_Date_Utc_Key,
							ifnull(FORMAT_DATE("%Y%m%d",date(Src.Ded_Prepay_End) ) ,'19000101')As Prepay_End_Date_Utc_Key,
							ifnull(FORMAT_DATE("%Y%m%d",date(Src.Event_Create_Date) ) ,'19000101') As Event_Created_Date_Utc_Key,
							IFNULL(
									(extract(Hour from Src.Event_Create_Date)* 3600) +
									(extract(MINUTE from Src.Event_Create_Date)* 60) +
									(extract(SECOND from Src.Event_Create_Date))
									,0
								) As Event_Created_Time_Utc_Key,
							ifnull(FORMAT_DATE("%Y%m%d",date(Src.Event_Created_Date_Cst) ) ,'19000101') As Event_Created_Date_Cst_Key,
							IFNULL(
									(extract(Hour from Src.Event_Created_Date_Cst)* 3600) +
									(extract(MINUTE from Src.Event_Created_Date_Cst)* 60) +
									(extract(SECOND from Src.Event_Created_Date_Cst))
									,0
								) As Event_Created_Time_Cst_Key,
							ifnull(FORMAT_DATE("%Y%m%d",date(Src.Event_Start_Dt) ) ,'19000101') As Event_Start_Date_Utc_Key,
							IFNULL(
									(extract(Hour from Src.Event_Start_Dt)* 3600) +
									(extract(MINUTE from Src.Event_Start_Dt)* 60) +
									(extract(SECOND from Src.Event_Start_Dt))
									,0
								)  As Event_Start_Time_Utc_Key,
							ifnull(FORMAT_DATE("%Y%m%d",date(Src.Event_Start_Date_Cst) ) ,'19000101') As Event_Start_Date_Cst_Key,
							IFNULL(
									(extract(Hour from Src.Event_Start_Date_Cst)* 3600) +
									(extract(MINUTE from Src.Event_Start_Date_Cst)* 60) +
									(extract(SECOND from Src.Event_Start_Date_Cst))
									,0
								) As Event_Start_Time_Cst_Key,
							ifnull(FORMAT_DATE("%Y%m%d",date(Src.Event_Start_Date_Cst) ) ,'19000101') As Event_End_Date_Utc_Key,
							IFNULL(
									(extract(Hour from Src.EVENT_End_Date)* 3600) +
									(extract(MINUTE from Src.EVENT_End_Date)* 60) +
									(extract(SECOND from Src.EVENT_End_Date))
									,0
								) As Event_End_Time_Utc_Key,
							ifnull(FORMAT_DATE("%Y%m%d",date(Src.Event_Start_Date_Cst) ) ,'19000101') As Event_End_Date_Cst_Key,
							IFNULL(
									(extract(Hour from Src.Event_End_Date_Cst)* 3600) +
									(extract(MINUTE from Src.Event_End_Date_Cst)* 60) +
									(extract(SECOND from Src.Event_End_Date_Cst))
									,0
								) As Event_End_Time_Cst_Key,
							ifnull(FORMAT_DATE("%Y%m%d",date(Src.EVENT_earned_start_dtt) ) ,'19000101') As Earned_Start_Date_Utc_Key,
							IFNULL(
									(extract(Hour from Src.EVENT_earned_start_dtt)* 3600) +
									(extract(MINUTE from Src.EVENT_earned_start_dtt)* 60) +
									(extract(SECOND from Src.EVENT_earned_start_dtt))
									,0
								) As Earned_Start_Time_Utc_Key,
							ifnull(FORMAT_DATE("%Y%m%d",date(Src.Earned_Start_Date_Cst) ) ,'19000101')  As Earned_Start_Date_Cst_Key,
							IFNULL(
									(extract(Hour from Src.Earned_Start_Date_Cst)* 3600) +
									(extract(MINUTE from Src.Earned_Start_Date_Cst)* 60) +
									(extract(SECOND from Src.Earned_Start_Date_Cst))
									,0
								) As Earned_Start_Time_Cst_Key,
							ifnull(FORMAT_DATE("%Y%m%d",date(Src.EVENT_earned_end_dtt) ) ,'19000101') As Earned_End_Date_Utc_Key,
							IFNULL(
									(extract(Hour from Src.EVENT_earned_end_dtt)* 3600) +
									(extract(MINUTE from Src.EVENT_earned_end_dtt)* 60) +
									(extract(SECOND from Src.EVENT_earned_end_dtt))
									,0
								) As Earned_End_Time_Utc_Key,
							ifnull(FORMAT_DATE("%Y%m%d",date(Src.Earned_End_Date_Cst) ) ,'19000101') As Earned_End_Date_Cst_Key,
							IFNULL(
									(extract(Hour from Src.Earned_End_Date_Cst)* 3600) +
									(extract(MINUTE from Src.Earned_End_Date_Cst)* 60) +
									(extract(SECOND from Src.Earned_End_Date_Cst))
									,0
								) As Earned_End_Time_Cst_Key,				
							CASE WHEN cast(Src.Currency_ID as string)IS NULL THEN '-1' ELSE IFNULL(cast(Cur.Currency_Key as string),'0') END AS Currency_Key,
							IFNULL(Src.Transaction_Term,'0') AS Transaction_Term,
							IFNULL(Src.Trx_Qty,0) As Quantity,
							IFNULL(Src.Billing_Days_In_Month,0) Billing_Days_In_Month,
							IFNULL(Src.total*USDRate.Exchange_Rate_Exchange_Rate_Value,0) AS Amount_Usd,
							IFNULL(Src.Extended_Amount*USDRate.Exchange_Rate_Exchange_Rate_Value,0) As Extended_Amount_Usd,
							IFNULL(Src.Unit_Selling_Price*USDRate.Exchange_Rate_Exchange_Rate_Value,0) As Unit_Selling_Price_Usd,
							IFNULL(Src.total*GBPRate.Exchange_Rate_Exchange_Rate_Value,0) As Amount_Gbp,
							IFNULL(Src.Extended_Amount*GBPRate.Exchange_Rate_Exchange_Rate_Value,0) As Extended_Amount_Gbp,
							IFNULL(Src.Unit_Selling_Price*GBPRate.Exchange_Rate_Exchange_Rate_Value,0) As Unit_Selling_Price_Gbp,
							IFNULL(Src.total,0) As Amount_Local,
							IFNULL(Src.Extended_Amount,0) As Extended_Amount_Local,
							IFNULL(Src.Unit_Selling_Price,0) Unit_Selling_Price_Local,
							case when Src.DED_Usage_Type like '%One%Time%' then 1 else 0 End As Is_Standalone_Fee,
							CASE WHEN cast(Src.EVENT_earned_end_dtt as date)<>'1900-01-01' AND 
							CAST(src.EVENT_earned_end_dtt as date) <> '1970-01-01' AND Src.EVENT_earned_end_dtt<=Src.Bill_Start_Date then 1
							WHEN Src.FASTLANE_INV_Is_Backbill<>0 then 1
							WHEN Src.FASTLANE_INV_SUB_GRP_CODE like '%Backbill%' then 1
							ELSE 0 END AS Is_Back_Bill,
							Case when (Src.Event_Type like '%fastlane%' or Src.Service_Obj_type like '%fastlane%') then 1 else 0 end As Is_Fastlane,
							IFNULL(Is_Transaction_Successful,0) as Is_Transaction_Completed,
							master_unique_id as Transaction_key,
							CURRENT_DATETIME() AS Record_Created_Datetime,
							'udsp_etl_Load_Fact_Invoice_Event_Detail_Dedicated_Daily_Load' AS Record_Created_By,
							CURRENT_DATETIME() AS Record_Updated_Datetime,
							'udsp_etl_Load_Fact_Invoice_Event_Detail_Dedicated_Daily_Load' AS Record_Updated_By,
							25 as source_system_key
						From 
							`rax-datamart-dev`.corporate_dmart.inc_stage_data Src 
							left join (
							select account_key,account_number,global_account_type,account_team_name,
									account_primary_contact_id,account_billing_contact_id
									,dc.contact_key as primary_contact_key,dc2.contact_key as billing_contact_key
							from `rax-staging-dev`.stage_three_dw.dim_account da 
							left join `rax-staging-dev`.stage_three_dw.dim_contact dc  on da.account_primary_contact_id = dc.contact_nk and dc.contact_current_record = 1 and 	lower(dc.contact_source_name) = 'cms'
							left join `rax-staging-dev`.stage_three_dw.dim_contact dc2  on da.account_billing_contact_id = dc2.contact_nk and dc2.contact_current_record = 1 and 	lower(dc2.contact_source_name) = 'cms'
							where da.current_record = 1 
							) a--tmp_acc_con a 
							on src.account=a.account_number and ltrim(rtrim(lower(src.global_account_type))) =ltrim(rtrim(lower(a.global_account_type )))
							left join `rax-staging-dev`.stage_three_dw.dim_team  account_team  
										on a.account_team_name = account_team.team_name and account_team.current_record = 1 
							left join `rax-staging-dev`.stage_three_dw.dim_product p  
										on cast(src.product_poid as string)=p.product_resource_code_nk	and p.product_current_record_flag = 1 and lower(p.product_record_source_system_name) = 'brm'
							left join `rax-staging-dev`.stage_three_dw.dim_currency cur  
										on src.currency_id=cast(cur.currency_iso_numeric_code as string)
							left join `rax-staging-dev`.stage_three_dw.dim_datacenter datacenter1  
										on src.oracle_location=datacenter1.datacenter_abbr and datacenter1.datacenter_current_record_flag = 1
							left join `rax-staging-dev`.stage_three_dw.dim_datacenter datacenter2  
										on src.oracle_location=datacenter2.datacenter_name and datacenter2.datacenter_current_record_flag = 1
							left join `rax-staging-dev`.stage_three_dw.report_exchange_rate usdrate 
										on src.currency_abbrev=usdrate.exchange_rate_from_currency_code 
											and src.timeyear=cast(usdrate.exchange_rate_year as string)
											and src.timemonth=cast(usdrate.exchange_rate_month as string)
											and lower(usdrate.exchange_rate_to_currency_code)='usd'
											and lower(usdrate.source_system_name)='oracle'
							left join `rax-staging-dev`.stage_three_dw.report_exchange_rate gbprate 
										on src.currency_abbrev=gbprate.exchange_rate_from_currency_code 
										and src.timeyear=cast(gbprate.exchange_rate_year  as string)
										and src.timemonth=cast(gbprate.exchange_rate_month as string)
										and lower(gbprate.exchange_rate_to_currency_code)='gbp'
										and lower(gbprate.source_system_name)='oracle'
							left join (
										select device_key,cast(device_number as string) as device_number,current_record --into #dim_device
										from `rax-staging-dev`.stage_three_dw.dim_device where current_record = 1
									) d --temp_dim_device d  
										on src.server=d.device_number and d.current_record = 1
							left join `rax-datamart-dev`.corporate_dmart.dim_gl_product glp 
										on src.oracle_product_id=glp.gl_product_code and glp.current_record = 1	and lower(glp.source_system_name)='brm'
							left join `rax-datamart-dev`.corporate_dmart.dim_gl_account gla  
										on src.gl_account=gla.gl_account_id and gla.current_record = 1 and lower(gla.source_system_name)='brm'
							left join `rax-datamart-dev`.corporate_dmart.dim_glid_configuration glconfig  
										on src.glid_config= glconfig.glid_configuration_nk	and lower(glconfig.source_system_name)='brm'
							left join `rax-datamart-dev`.corporate_dmart.dim_payment_terms pt 	
										on src.payment_term=pt.payment_term_nk	and lower(pt.source_system_name) = 'brm'
							left join `rax-datamart-dev`.corporate_dmart.dim_billing_events be  
										on src.event_type=be.event_type and be.current_record = 1
							
							left join `rax-datamart-dev`.corporate_dmart.dim_item item 
										on src.item_tag=item.item_tag and item.current_record=1
							
							left join `rax-datamart-dev`.corporate_dmart.dim_invoice inv  
										on src.invoice_nk=inv.invoice_nk
							left join `rax-datamart-dev`.corporate_dmart.dim_invoice_attribute invattr  
										on src.invoice_attribute_nk=invattr.invoice_attribute_nk
						)a;
