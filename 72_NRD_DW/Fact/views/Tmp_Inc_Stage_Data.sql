create or replace view `rax-datamart-dev`.corporate_dmart.Tmp_Inc_Stage_Data as            
--Stage: Incremental Stage data
SELECT   src.detail_unique_record_id --                INTO #Tmp_Inc_Stage_Data 
                ,src.Bill_End_Date
                ,src.Bill_Created_Date
                ,src.Bill_Start_Date
                ,src.Event_Created_Date
                ,src.Event_Start_Date
                ,src.Event_End_Date
                ,src.Event_Earned_Start_Date
                ,src.Event_Earned_End_Date
                ,src.Quantity
                ,CASE WHEN LOWER(SRC.EVENT_TYPE) = '/event/billing/cycle/tax'
					THEN IFNULL( SRC.TAX_AMOUNT, SRC.EBI_AMOUNT) 
					ELSE SRC.EBI_AMOUNT          
				END AS Ebi_Amount
                ,src.Rate
                ,src.Event_Type
                ,src.Fastlane_Impact_Is_Backbill
                ,src.Fastlane_Impact_Sub_Grp_Code
                ,src.EBI_GL_ID
                ,src.GL_Segment
                ,src.Account_Id
                ,src.Product_Poid_Id0
                ,src.Ebi_Currency_ID
                ,src.Item_Type
                ,src.Invoice_Nk
                ,src.Invoice_Attribute_Nk
                ,src.Service_Obj_type
                ,`rax-staging-dev`.bq_functions.ufn_getdaysinmonth(date_sub(Bill_End_Date,interval 1 month)) as Billing_Days_In_Month 
                ,CASE WHEN lower(Src.Event_Type) like '%one%time%' then 1 else 0 End AS Is_Standalone_Fee
                ,CASE 	WHEN cast(Src.Event_Earned_End_Date as date)<>date('1900-01-01') and CAST(EVENT_EARNED_END_DATE as date) <> date('1970-01-01') and Src.Event_Earned_End_Date<=Src.Bill_Start_Date then 1
                        WHEN Src.Fastlane_Impact_Is_Backbill<>0 then 1
                        WHEN lower(Src.Fastlane_Impact_Sub_Grp_Code) like '%backbill%' then 1
                        ELSE 0 END AS Is_Back_Bill
                ,CASE WHEN  (lower(src.EVENT_Type) like '%fastlane%' OR lower(src.SERVICE_OBJ_TYPE) like '%fastlane%')THEN 1 ELSE 0 END AS Is_Fastlane   
                ,SRC.Item_tag AS Item_Tag
                ,EXTRACT(year from Bill_End_Date) AS Rate_Year
                ,EXTRACT(month from Bill_End_Date) AS Rate_Month
                , `rax-staging-dev`.bq_functions.get_utc_to_cst_time(Src.bill_start_date) Bill_Start_Date_Cst
                , `rax-staging-dev`.bq_functions.get_utc_to_cst_time(Src.bill_end_Date) Bill_End_Date_Cst
                , `rax-staging-dev`.bq_functions.get_utc_to_cst_time(Src.bill_mod_Date) Bill_Mod_Date_Cst
                , `rax-staging-dev`.bq_functions.get_utc_to_cst_time(Src.Item_Effective_Date) Item_Effective_Date_Cst
                , `rax-staging-dev`.bq_functions.get_utc_to_cst_time(Src.Item_Mod_date) Item_Mod_Date_Cst
                , `rax-staging-dev`.bq_functions.get_utc_to_cst_time(Src.EVENT_start_date) Event_Start_Date_Cst
                , `rax-staging-dev`.bq_functions.get_utc_to_cst_time(Src.Event_End_Date) Event_End_Date_Cst
                , `rax-staging-dev`.bq_functions.get_utc_to_cst_time(Src.Event_Mod_Date) Event_Mod_Date_Cst
                , `rax-staging-dev`.bq_functions.get_utc_to_cst_time(Src.Event_Created_Date) Event_Created_Date_Cst
                , `rax-staging-dev`.bq_functions.get_utc_to_cst_time(Src.Event_Earned_Start_Date) Event_Earned_Start_Date_Cst
                , `rax-staging-dev`.bq_functions.get_utc_to_cst_time(Src.EVENT_earned_end_date) Event_Earned_End_Date_Cst
                , `rax-staging-dev`.bq_functions.get_utc_to_cst_time(Src.Bill_Created_Date) Bill_Created_Date_Cst
                ,ifnull(`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Src.Bill_End_Date),0) AS Invoice_Date_Utc_Key
                ,ifnull(`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Src.Bill_Created_Date),0) AS Bill_Created_Date_Utc_Key
                ,IFNULL((extract(Hour from Src.Bill_Created_Date)* 3600) + (extract(MINUTE from Src.Bill_Created_Date)* 60) + (extract(SECOND from Src.Bill_Created_Date)),0) AS Bill_Created_Time_Utc_Key 
                ,ifnull(`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Src.Bill_Start_Date),0) AS Bill_Start_Date_Utc_Key
                ,ifnull(`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Src.Bill_End_Date),0) AS Bill_End_Date_Utc_Key
                ,ifnull(`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Src.Event_Created_Date),0) AS Event_Created_Date_Utc_Key
                ,IFNULL((extract(Hour from Src.Event_Created_Date)* 3600) + (extract(MINUTE from Src.Event_Created_Date)* 60) + (extract(SECOND from Src.Event_Created_Date)),0) AS Event_Created_Time_Utc_Key 
                ,ifnull(`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Src.Event_Start_Date),0)  AS Event_Start_Date_Utc_Key
                ,IFNULL((extract(Hour from Src.Event_Start_Date)* 3600) + (extract(MINUTE from Src.Event_Start_Date)* 60) + (extract(SECOND from Src.Event_Start_Date)),0) AS Event_Start_Time_Utc_Key
                ,ifnull(`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Src.Event_End_Date),0) AS Event_End_Date_Utc_Key
                ,IFNULL((extract(Hour from Src.Event_End_Date)* 3600) + (extract(MINUTE from Src.Event_End_Date)* 60) + (extract(SECOND from Src.Event_End_Date)),0) AS Event_End_Time_Utc_Key
                ,ifnull(`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Src.Event_Earned_Start_Date),0)  AS Earned_Start_Date_Utc_Key
                ,IFNULL((extract(Hour from Src.Event_Earned_Start_Date)* 3600) + (extract(MINUTE from Src.Event_Earned_Start_Date)* 60) + (extract(SECOND from Src.Event_Earned_Start_Date)),0) AS Earned_Start_Time_Utc_Key
                ,ifnull(`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Src.Event_Earned_End_Date),0)  AS Earned_End_Date_Utc_Key
                ,IFNULL((extract(Hour from Src.Event_Earned_End_Date)* 3600) + (extract(MINUTE from Src.Event_Earned_End_Date)* 60) + (extract(SECOND from Src.Event_Earned_End_Date)),0) AS Earned_End_Time_Utc_Key 
                ,Is_Transaction_Successful 
                ,Global_Account_Type
                ,'udsp_etl_Load_Fact_Invoice_Event_Detail_Cloud_Daily_Load'  AS Record_Source_Name
            FROM                    
                (
                            SELECT detail_unique_record_id
											,Bill_End_Date
											,Bill_Created_Date
											,Bill_Start_Date
											,Event_Created_Date
											,Event_Start_Date
											,Event_End_Date
											,Event_Earned_Start_Date
											,Event_Earned_End_Date
											,Quantity
											,EVENT_TYPE
											,TAX_AMOUNT
											,EBI_AMOUNT
											,Rate
											,Fastlane_Impact_Is_Backbill
											,Fastlane_Impact_Sub_Grp_Code
											,EBI_GL_ID
											,GL_Segment
											,Account_Id
											,Product_Poid_Id0
											,Ebi_Currency_ID
											,Item_Type
											,Invoice_Nk
											,Invoice_Attribute_Nk
											,Service_Obj_type
											,Item_Tag
											,bill_mod_Date
											,Item_Effective_Date
											,Item_Mod_date
											,Event_Mod_Date
											,Is_Transaction_Successful
											,Global_Account_Type
                                            FROM `rax-staging-dev`.stage_two_dw.stage_invitemeventdetail
                            WHERE (`rax-staging-dev`.bq_functions.udf_is_numeric(Account_Id)=1 AND date(tblload_dtt)>= ( select date_sub(date(max(tblload_dtt)),interval 2 day) FROM `rax-staging-dev`.stage_two_dw.stage_invitemeventdetail) 
							) --AND src.BILL_END_DATE = '2019-08-15
                            UNION ALL
                            SELECT RECSRC.detail_unique_record_id
											,RECSRC.Bill_End_Date
											,RECSRC.Bill_Created_Date
											,RECSRC.Bill_Start_Date
											,RECSRC.Event_Created_Date
											,RECSRC.Event_Start_Date
											,RECSRC.Event_End_Date
											,RECSRC.Event_Earned_Start_Date
											,RECSRC.Event_Earned_End_Date
											,RECSRC.Quantity
											,RECSRC.EVENT_TYPE
											,RECSRC.TAX_AMOUNT
											,RECSRC.EBI_AMOUNT
											,RECSRC.Rate
											,RECSRC.Fastlane_Impact_Is_Backbill
											,RECSRC.Fastlane_Impact_Sub_Grp_Code
											,RECSRC.EBI_GL_ID
											,RECSRC.GL_Segment
											,RECSRC.Account_Id
											,RECSRC.Product_Poid_Id0
											,RECSRC.Ebi_Currency_ID
											,RECSRC.Item_Type
											,RECSRC.Invoice_Nk
											,RECSRC.Invoice_Attribute_Nk
											,RECSRC.Service_Obj_type
											,RECSRC.Item_Tag
											,RECSRC.bill_mod_Date
											,RECSRC.Item_Effective_Date
											,RECSRC.Item_Mod_date
											,RECSRC.Event_Mod_Date
											,RECSRC.Is_Transaction_Successful
											,RECSRC.Global_Account_Type 
                                            from `rax-staging-dev`.stage_two_dw.stage_invitemeventdetail recsrc 
                                            join `rax-staging-dev`.stage_two_dw.recycle_fact_invoice_event_detail_cloud recycle  
											ON Recycle.Transaction_key = RECSRC.detail_unique_record_id
            ) SRC;


SELECT * --INTO #TMP_ACC_CONN 
FROM (	SELECT 	Account_Key,Account_Number,Global_Account_Type,Account_Team_Name,
				Account_Primary_Contact_ID,Account_Billing_Contact_ID
				,DC.Contact_Key AS Primary_contact_Key,
				DC2.Contact_Key AS Billing_Contact_Key
		FROM `rax-staging-dev`.stage_three_dw.dim_account DA
		LEFT JOIN `rax-staging-dev`.stage_three_dw.dim_contact DC  ON DA.Account_Primary_Contact_ID = DC.Contact_NK AND DC.Contact_Current_Record = 1 AND upper(DC.Contact_Source_Name) = 'CMS'
		LEFT JOIN `rax-staging-dev`.stage_three_dw.dim_contact DC2  ON DA.Account_Billing_Contact_ID = DC2.Contact_NK AND DC2.Contact_Current_Record = 1 AND upper(DC2.Contact_Source_Name) = 'CMS'
		WHERE DA.Current_record = 1 
	);
