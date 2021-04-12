create or replace view `rax-datamart-dev`.corporate_dmart.Tmp_FACT_TEMP_DATA as
SELECT detail_unique_record_id AS Transaction_key --INTO #Tmp_FACT_TEMP_DATA
,`rax-staging-dev`.bq_functions.udf_yearmonth_nohyphen(Src.Bill_End_Date) AS Date_Month_Key
,19 AS Revenue_Type_Key
,CASE WHEN `rax-staging-dev`.bq_functions.udf_is_numeric(Src.ACCOUNT_ID)=0 THEN -1 ELSE ifnull(A.Account_Key,0) END As Account_Key
,CASE WHEN UPPER(ifnull(A.Account_Team_Name,'NULL'))  IN ('NULL','UNASSIGNED') THEN -1 ELSE ifnull(Account_Team.Team_Key,0) END AS Team_Key
,CASE WHEN  UPPER(ifnull(A.Account_Primary_Contact_ID,'NULL')) IN ('NULL','-99','UNKNOWN') THEN -1 ELSE ifnull(A.Primary_Contact_Key,0) END AS Primary_Contact_Key
,CASE WHEN  UPPER(ifnull(A.Account_Billing_Contact_ID,'NULL')) IN ('NULL', 'UNKNOWN') THEN -1 ELSE ifnull(A.Billing_Contact_Key,0) END AS Billing_Contact_Key
,CASE WHEN `rax-staging-dev`.bq_functions.udf_is_numeric(cast(SRC.PRODUCT_POID_ID0 as string)) = 0  THEN -1 ELSE ifnull(P.Product_Key,0) END AS Product_Key
,CASE WHEN Src.Item_Tag IS NULL THEN -1 ELSE  ifnull(Item.Item_Key,0) END AS Item_Key
,CASE WHEN `rax-staging-dev`.bq_functions.udf_is_numeric(cast(GL.glsub7_product as string)) = 0 OR GL.glsub7_product  = '000' THEN -1 ELSE ifnull(Glp.GL_Product_Key,0) END AS GL_Product_Key
,CASE WHEN `rax-staging-dev`.bq_functions.udf_is_numeric(cast(GL.glsub3_acct_subprod as string)) = 0 OR GL.glsub3_acct_subprod  = '000' THEN -1  ELSE ifnull(GlA.GL_Account_Key,0) END AS GL_Account_Key
,CASE WHEN CONCAT(cast(Src.EBI_GL_ID AS STRING),'-',cast(Src.GL_Segment AS STRING),'-',cast(GL.GLAcct_record_type AS STRING),'-',cast(GL.GLAcct_attribute AS STRING)) IS NULL THEN -1 ELSE ifnull(GlConfig.GLid_Configuration_Key,0) END AS GLid_Configuration_Key
,CASE WHEN SRC.EVENT_TYPE IS NULL THEN -1 ELSE ifnull(Be.Event_Type_Key,0) END AS Event_Type_Key
,CASE WHEN UPPER(ifnull(SRC.Invoice_Nk,'NULL')) IN ('NULL','N/A') THEN -1 ELSE ifnull(Inv.Invoice_Key,0) END AS Invoice_Key
,CASE WHEN UPPER(ifnull(SRC.Invoice_Attribute_NK,'NULL')) IN ('NULL','N/A') THEN -1 ELSE ifnull(Invattr.Invoice_Attribute_Key,0) END AS Invoice_Attribute_Key
,Src.Invoice_Date_Utc_Key
,Src.Bill_Created_Date_Utc_Key
,Src.Bill_Created_Time_Utc_Key
,ifnull(`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Src.Bill_Created_Date_Cst),0) AS Bill_Created_Date_Cst_Key 
,IFNULL((extract(Hour from Src.Bill_Created_Date_Cst)* 3600) + (extract(MINUTE from Src.Bill_Created_Date_Cst)* 60) + (extract(SECOND from Src.Bill_Created_Date_Cst)),0) AS Bill_Created_Time_Cst_Key 
,Src.Bill_Start_Date_Utc_Key
,Src.Bill_End_Date_Utc_Key
,Src.Event_Created_Date_Utc_Key
,Src.Event_Created_Time_Utc_Key
,ifnull(`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Src.Event_Created_Date_Cst),0) AS Event_Created_Date_Cst_Key  
,IFNULL((extract(Hour from Src.Event_Created_Date_Cst)* 3600) + (extract(MINUTE from Src.Event_Created_Date_Cst)* 60) + (extract(SECOND from Src.Event_Created_Date_Cst)),0) AS Event_Created_Time_Cst_Key 
,Src.Event_Start_Date_Utc_Key
,Src.Event_Start_Time_Utc_Key
,ifnull(`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Src.Event_Start_Date_Cst),0) AS Event_Start_Date_Cst_Key 
,IFNULL((extract(Hour from Src.Event_Start_Date_Cst)* 3600) + (extract(MINUTE from Src.Event_Start_Date_Cst)* 60) + (extract(SECOND from Src.Event_Start_Date_Cst)),0) AS Event_Start_Time_Cst_Key  
,Src.Event_End_Date_Utc_Key
,Src.Event_End_Time_Utc_Key
,ifnull(`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Src.Event_End_Date_Cst),0) AS Event_End_Date_Cst_Key 
,IFNULL((extract(Hour from Src.Event_End_Date_Cst)* 3600) + (extract(MINUTE from Src.Event_End_Date_Cst)* 60) + (extract(SECOND from Src.Event_End_Date_Cst)),0) AS Event_End_Time_Cst_Key 
,Src.Earned_Start_Date_Utc_Key
,Src.Earned_Start_Time_Utc_Key
,ifnull(`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Src.Event_Earned_Start_Date_Cst),0) AS Earned_Start_Date_Cst_Key 
,IFNULL((extract(Hour from Src.Event_Earned_Start_Date_Cst)* 3600) + (extract(MINUTE from Src.Event_Earned_Start_Date_Cst)* 60) + (extract(SECOND from Src.Event_Earned_Start_Date_Cst)),0) AS Earned_Start_Time_Cst_Key  
,Src.Earned_End_Date_Utc_Key
,Src.Earned_End_Time_Utc_Key
,ifnull(`rax-staging-dev`.bq_functions.udf_time_key_nohyphen(Src.Event_Earned_End_Date_Cst),0) AS Earned_End_Date_Cst_Key 
,IFNULL((extract(Hour from Src.Event_Earned_End_Date_Cst)* 3600) + (extract(MINUTE from Src.Event_Earned_End_Date_Cst)* 60) + (extract(SECOND from Src.Event_Earned_End_Date_Cst)),0) AS Earned_End_Time_Cst_Key 
,CASE WHEN SRC.EBI_CURRENCY_ID IS NULL THEN -1 ELSE ifnull(Cur.Currency_Key,0) END AS Currency_Key
,Src.Quantity AS Quantity
,Src.Billing_Days_In_Month
,ifnull(ROUND(cast(Src.Ebi_Amount*USDRate.Exchange_Rate_Exchange_Rate_Value AS NUMERIC),6),0) AS Total_Amount_Usd
,ifnull(ROUND(cast(cast(Src.Rate as NUMERIC)*USDRate.Exchange_Rate_Exchange_Rate_Value AS NUMERIC),6),0) AS Unit_Selling_Price_Usd
,ifnull(ROUND(cast(Src.Ebi_Amount*GBPRate.Exchange_Rate_Exchange_Rate_Value AS NUMERIC),6),0) AS Total_Amount_Gbp
,ifnull(ROUND(cast(cast(Src.Rate as NUMERIC)*GBPRate.Exchange_Rate_Exchange_Rate_Value AS NUMERIC),6),0) AS Unit_Selling_Price_Gbp
,ifnull(ROUND(cast(Src.Ebi_Amount AS NUMERIC),6),0) AS Total_Amount_Local
,ifnull(ROUND(cast(cast(Src.Rate as NUMERIC) AS NUMERIC),6),0) AS Unit_Selling_Price_Local
,Src.Is_Standalone_Fee
,Src.Is_Back_Bill
,Src.Is_Fastlane      
,Is_Transaction_Successful AS Is_Transaction_Completed 
,CURRENT_DATETIME() AS Record_Created_Datetime
,DRS.Record_Source_KEY AS  Record_Created_By
,CURRENT_DATETIME() AS Record_Updated_Datetime
,DRS.Record_Source_KEY AS Record_Updated_By
,25 AS Source_System_Key
From 
    `rax-datamart-dev`.corporate_dmart.Tmp_Inc_Stage_Data Src 
    LEFT  JOIN `rax-staging-dev`.stage_one.raw_brm_glid_account_config GL  
                                    ON Src.EBI_GL_ID= GL.glid_rec_id AND Src.GL_Segment= GL.GLSeg_name AND GL.GLAcct_record_type In (2,8) AND GL.GLAcct_attribute= 1
    INNER JOIN `rax-staging-dev`.stage_two_dw.stage_brm_glid_acct_atttribute atr 
                                    ON GL.GLAcct_attribute= atr.Attribute_id
    INNER JOIN `rax-staging-dev`.stage_two_dw.stage_brm_glid_acct_report_type typ 
                                    ON         GL.GLAcct_record_type=typ.Report_Type_id
    --LEFT JOIN ebi-etl.Stage_Three_Dw.Dim_Account A  
    --                                ON         Src.Account_Id=A.Account_Number AND SRC.Global_Account_Type = A.Global_Account_Type AND A.Current_Record=1 
    --LEFT JOIN ebi-etl.Stage_Three_Dw.Dim_Contact Primary_Contact 
    --                                ON A.Account_Primary_Contact_Id=Primary_Contact.Contact_Nk AND Primary_Contact.Contact_Current_Record=1
    --LEFT JOIN ebi-etl.Stage_Three_Dw.Dim_Contact Billing_Contact 
    --                                ON A.Account_Billing_Contact_Id=Billing_Contact.Contact_Nk AND Billing_Contact.Contact_Current_Record=1
	LEFT JOIN (
		SELECT * --INTO #TMP_ACC_CONN 
		FROM (	SELECT 	Account_Key,Account_Number,Global_Account_Type,Account_Team_Name,
						Account_Primary_Contact_ID,Account_Billing_Contact_ID
						,DC.Contact_Key AS Primary_contact_Key,
						DC2.Contact_Key AS Billing_Contact_Key
				FROM `rax-staging-dev`.stage_three_dw.dim_account DA
				LEFT JOIN `rax-staging-dev`.stage_three_dw.dim_contact DC  ON DA.Account_Primary_Contact_ID = DC.Contact_NK AND DC.Contact_Current_Record = 1 AND upper(DC.Contact_Source_Name) = 'CMS'
				LEFT JOIN `rax-staging-dev`.stage_three_dw.dim_contact DC2  ON DA.Account_Billing_Contact_ID = DC2.Contact_NK AND DC2.Contact_Current_Record = 1 AND upper(DC2.Contact_Source_Name) = 'CMS'
				WHERE DA.Current_record = 1 
			)
	) A--#TMP_ACC_CONN A 
	ON Src.Account_Id=A.Account_Number AND LTRIM(RTRIM(UPPER(SRC.Global_Account_Type))) = LTRIM(RTRIM(UPPER(A.Global_Account_Type)))
    left join `rax-staging-dev`.stage_three_dw.dim_team account_team  
                                    on                a.account_team_name=account_team.team_name and account_team.current_record=1
    left join `rax-staging-dev`.stage_three_dw.dim_product p  
                                    on cast(src.product_poid_id0 as string)=p.product_resource_code_nk and p.product_current_record_flag=1 
                                    and lower(p.product_record_source_system_name)='brm'
    left join `rax-datamart-dev`.corporate_dmart.dim_gl_product glp  
                                    on gl.glsub7_product=glp.gl_product_code and glp.current_record = 1 and glp.source_system_name='brm'
    left join `rax-datamart-dev`.corporate_dmart.dim_gl_account gla  
                                    on gl.glsub3_acct_subprod=gla.gl_account_id and gla.current_record = 1 and gla.source_system_name='brm'
    left join `rax-datamart-dev`.corporate_dmart.dim_glid_configuration glconfig  
                                    on lower(glconfig.glid_configuration_nk)=concat(cast(src.ebi_gl_id as string),'-',cast(src.gl_segment as string),'-',cast(gl.glacct_record_type as string),'-',cast(gl.glacct_attribute as string))
    left join `rax-datamart-dev`.corporate_dmart.dim_billing_events be  
                                    on src.event_type=be.event_type and be.current_record = 1
    left join `rax-staging-dev`.stage_three_dw.dim_currency cur  
                                    on src.ebi_currency_id=cur.currency_iso_numeric_code
    left join `rax-datamart-dev`.corporate_dmart.dim_item item  on src.item_tag = item.item_tag and item.current_record = 1  
    -- left join stage_two_dw.xref_item_tag it   --stage_two_dw
                                    -- on src.event_type = it.event_type and ifnull(src.service_obj_type,'not available')=it.service_type and src.item_type=it.item_type
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

