create or replace view `rax-datamart-dev`.corporate_dmart.inc_stage_data as
 SELECT SRC_Final.master_unique_id-- INTO #Inc_Stage_Data
                                  ,SRC_Final.Account
                                  ,SRC_Final.Invoice_Date
                                  ,SRC_Final.Bill_Created_Date
                                  ,SRC_Final.Bill_Start_Date
                                  ,SRC_Final.Bill_End_Date
                                  ,SRC_Final.Ded_Prepay_Start
                                  ,SRC_Final.Ded_Prepay_End
                                  ,SRC_Final.Product_Poid
                                  ,SRC_Final.Event_Create_Date
                                  ,SRC_Final.Event_Start_Dt
                                  ,SRC_Final.Event_End_Date
                                  ,SRC_Final.EVENT_earned_start_dtt
                                  ,SRC_Final.EVENT_earned_end_dtt
                                  ,SRC_Final.Trx_Qty
                                  ,SRC_Final.Billing_Days_In_Month
                                  ,SRC_Final.total
                                  ,SRC_Final.Extended_Amount
                                  ,SRC_Final.Unit_Selling_Price
                                   ,SRC_Final.DED_Usage_Type
                                  ,SRC_Final.FASTLANE_INV_Is_Backbill
                                  ,SRC_Final.FASTLANE_INV_SUB_GRP_CODE
                                  ,SRC_Final.Event_Type
                                  ,SRC_Final.Glid_Rec_Id
                                  ,SRC_Final.GL_Segment
                                  ,SRC_Final.GLAcct_Record_Type
                                  ,SRC_Final.GLAcct_Attribute
                                  ,SRC_Final.Payment_Term
                                  ,Trx_Raw_Start_Date        
                                  ,SRC_Final.Currency_ID
                                  ,IFNULL(SRC_Final.Service_Obj_type,'Not Available') AS Service_Obj_type
                                  ,SRC_Final.item_tag AS Item_Tag
                                  ,SRC_Final.Item_Type
								  ,SRC_Final.Oracle_Location
                                  ,SRC_Final.Invoice_nk
                                  ,SRC_Final.Invoice_Attribute_nk
                                  ,SRC_Final.Currency_Abbrev
                                  ,SRC_Final.Time_Month_Key
                                  ,SRC_Final.Server
                                  ,SRC_Final.Oracle_Product_ID
                                  ,GL_Account   
                                  ,CONCAT(CAST(SRC_Final.Glid_Rec_Id AS STRING),'-',CAST(SRC_Final.GL_Segment AS STRING),'-',CAST(SRC_Final.GLAcct_Record_Type AS STRING),'-',CAST(SRC_Final.GLAcct_Attribute AS STRING)) AS GLID_CONFIG      
                                  ,substr(cast(Time_Month_Key as string),1,4) AS TimeYear
                                  ,substr(cast(Time_Month_Key as string),5,2) AS TimeMonth
                                  , `rax-staging-dev`.bq_functions.get_utc_to_cst_time(SRC_Final.Bill_Created_Date) AS Bill_Created_Date_Cst
                                  ,`rax-staging-dev`.bq_functions.get_utc_to_cst_time(SRC_Final.Event_Create_Date)			AS Event_Created_Date_Cst
                                  ,`rax-staging-dev`.bq_functions.get_utc_to_cst_time(SRC_Final.Event_Start_Dt)				AS Event_Start_Date_Cst
                                  ,`rax-staging-dev`.bq_functions.get_utc_to_cst_time(SRC_Final.Event_End_Date)				AS Event_End_Date_Cst
                                  ,`rax-staging-dev`.bq_functions.get_utc_to_cst_time(SRC_Final.EVENT_earned_start_dtt)	 AS Earned_Start_Date_Cst
                                  ,`rax-staging-dev`.bq_functions.get_utc_to_cst_time(SRC_Final.EVENT_earned_end_dtt)		AS Earned_End_Date_Cst
								  ,SRC_Final.Trx_Term Transaction_Term
								  ,SRC_Final.global_account_type
								  ,SRC_Final.Is_Transaction_Successful
                     FROM
						 (		SELECT master_unique_id,Account
										,Invoice_Date
										,Bill_Start_Date
										,Bill_End_Date
										,Ded_Prepay_Start
										,Ded_Prepay_End
										,Product_Poid
										,Event_Start_Dt
										,Event_End_Date
										,EVENT_earned_start_dtt
										,EVENT_earned_end_dtt
										,Trx_Qty
										,Billing_Days_In_Month
										,total
										,Extended_Amount
										,Unit_Selling_Price
										,DED_Usage_Type
										,FASTLANE_INV_Is_Backbill
										,FASTLANE_INV_SUB_GRP_CODE
										,Event_Type
										,GL_Segment
										,GLAcct_Record_Type
										,GLAcct_Attribute
										,Payment_Term
										,Trx_Raw_Start_Date        
										,Currency_ID
										,Service_Obj_type
										,item_tag 
										,Item_Type
										,Oracle_Location
										,Invoice_nk
										,Invoice_Attribute_nk
										,Currency_Abbrev
										,Server
										,Oracle_Product_ID
										,GL_Account
										,Glid_Rec_Id
										,Time_Month_Key
										,Bill_Created_Date
										,Event_Create_Date
										,Trx_Term
										,global_account_type
										,Is_Transaction_Successful
									FROM `rax-staging-dev`.stage_two_dw.stage_dedicated_inv_event_detail src
									where etldtt>= (select date_sub(max(cast(etldtt as date)), INTERVAL 1 DAY) from `rax-staging-dev`.stage_two_dw.stage_dedicated_inv_event_detail) 
							UNION all 
								 SELECT SRC.master_unique_id
										,SRC.Account
										,SRC.Invoice_Date
										,SRC.Bill_Start_Date
										,SRC.Bill_End_Date
										,SRC.Ded_Prepay_Start
										,SRC.Ded_Prepay_End
										,SRC.Product_Poid
										,SRC.Event_Start_Dt
										,SRC.Event_End_Date
										,SRC.EVENT_earned_start_dtt
										,SRC.EVENT_earned_end_dtt
										,SRC.Trx_Qty
										,SRC.Billing_Days_In_Month
										,SRC.total
										,SRC.Extended_Amount
										,SRC.Unit_Selling_Price
										,SRC.DED_Usage_Type
										,SRC.FASTLANE_INV_Is_Backbill
										,SRC.FASTLANE_INV_SUB_GRP_CODE
										,SRC.Event_Type
										,SRC.GL_Segment
										,SRC.GLAcct_Record_Type
										,SRC.GLAcct_Attribute
										,SRC.Payment_Term
										,SRC.Trx_Raw_Start_Date        
										,SRC.Currency_ID
										,SRC.Service_Obj_type
										,SRC.item_tag AS Item_Tag
										,SRC.Item_Type
										,SRC.Oracle_Location
										,SRC.Invoice_nk
										,SRC.Invoice_Attribute_nk
										,SRC.Currency_Abbrev
										,SRC.Server
										,SRC.Oracle_Product_ID
										,SRC.GL_Account
										,SRC.Glid_Rec_Id
										,SRC.Time_Month_Key
										,SRC.Bill_Created_Date
										,SRC.Event_Create_Date
										,SRC.Trx_Term
										,SRC.global_account_type
										,SRC.Is_Transaction_Successful
									FROM  `rax-staging-dev`.stage_two_dw.stage_dedicated_inv_event_detail src
									inner join  `rax-staging-dev`.stage_two_dw.recycle_fact_invoice_event_detail_dedicated recycle 
									on src.master_unique_id=recycle.transaction_key 
									--AND ISNUMERIC(Account)=1 -- Commented upon discussion with Uday on 07-Nov-2019, as it restricts few bill numbers which is there in ABO server
						) SRC_FINAL;
