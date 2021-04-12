CREATE or replace PROCEDURE    `rax-staging-dev`.stage_three_dw.udsp_etl_load_fact_invoice_event_detail_emailapps_daily_load()
BEGIN  

--- Tmp_Inc_Stage_Data fact_invoice_event_detail_emailapps_tmp_inc_stage_data   

                      
SELECT * --INTO #TMP_ACC_CON 
FROM (	select 	account_key,account_number,global_account_type,account_team_name,
				account_primary_contact_id,account_billing_contact_id
				,dc.contact_key as primary_contact_key,
				dc2.contact_key as billing_contact_key
		from 	  `rax-staging-dev`.stage_three_dw.dim_account da 
		left join `rax-staging-dev`.stage_three_dw.dim_contact dc  on da.account_primary_contact_id = dc.contact_nk and dc.contact_current_record = 1 	 and lower(dc.contact_source_name )	 = 'mailtrust'
		left join `rax-staging-dev`.stage_three_dw.dim_contact dc2  on da.account_billing_contact_id = dc2.contact_nk and dc2.contact_current_record = 1 and lower(dc2.contact_source_name) = 'mailtrust'
		where da.current_record = 1 
	);
--#Tmp_FACT_TEMP_DATA   `rax-datamart-dev`.corporate_dmart.fact_invoice_event_detail_emailapps_tmp_fact_temp_data
 
       SELECT * --INTO #Fact_Temp_Data_Zero_Key_Records_EmailApps  
        FROM `rax-datamart-dev`.corporate_dmart.fact_invoice_event_detail_emailapps_tmp_fact_temp_data --#Tmp_FACT_TEMP_DATA  
       WHERE (    Account_Key = 0  
				 OR Team_Key = 0  
				 OR Primary_Contact_Key = 0  
				 OR Billing_Contact_Key = 0  
				 OR Invoice_Key = 0  
				 OR Invoice_Attribute_Key = 0  
				 OR Item_Key = 0  
				 OR GL_Product_Key = 0  
				 OR GL_Account_Key = 0  
				 OR Glid_Configuration_Key = 0  
				 OR Event_Type_Key = 0  
				 OR Currency_Key = 0  
         );  
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------  
--Segregating bad records ends here  
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------  
  
/*==================================================================================================================================================================================================================         
       FACT LOAD : DELETING THE MODIFIED MASTER-UNIQUE-IDS FROM SOURCE AND RE-INSERTING FROM THE TEMP FACT TABLE.  
                           BY THIS RECORDS AVAILABLE in INCREMENTAL STAGE WILL BE INSERTED INTO FACT TABLE  
======================================================================================================================================================================================================================*/                
      

        DELETE `rax-datamart-dev`.corporate_dmart.fact_invoice_event_detail_email_and_apps fact   
        where     fact.transaction_key in(
		select Stg.transaction_key 
        from `rax-datamart-dev`.corporate_dmart.fact_invoice_event_detail_emailapps_tmp_fact_temp_data Stg--#Tmp_FACT_TEMP_DATA Stg   
		);  
  
       INSERT INTO `rax-datamart-dev`.corporate_dmart.fact_invoice_event_detail_email_and_apps
          (Date_Month_Key  
          ,Revenue_Type_Key  
          ,Account_Key  
          ,Team_Key  
          ,Primary_Contact_Key  
          ,Billing_Contact_Key  
          ,Product_Key  
		  ,Datacenter_Key
          ,Item_Key  
          ,GL_Product_Key  
          ,GL_Account_Key  
          ,Glid_Configuration_Key  
          ,Event_Type_Key  
          ,Invoice_Key  
          ,Invoice_Attribute_Key  
          ,Invoice_Date_Utc_Key 
		  ,Prepay_Start_Date_Utc_Key 
		  ,Prepay_End_Date_Utc_Key
          ,Bill_Created_Date_Utc_Key  
          ,Bill_Created_Time_Utc_Key  
          ,Bill_Created_Date_Cst_Key  
          ,Bill_Created_Time_Cst_Key  
          ,Bill_Start_Date_Utc_Key  
          ,Bill_End_Date_Utc_Key  
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
          ,Unit_Selling_Price_Usd  
          ,Amount_Gbp  
          ,Unit_Selling_Price_Gbp  
          ,Amount_Local  
          ,Unit_Selling_Price_Local  
          ,Is_Standalone_Fee  
          ,Is_Back_Bill  
          ,Is_Fastlane  
          ,Is_Transaction_Completed  
          ,Transaction_Key  
          ,Record_Created_Datetime  
          ,Record_Created_Source_Key  
          ,Record_Updated_Datetime  
          ,Record_Updated_Source_Key  
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
          ,Item_Key  
          ,GL_Product_Key  
          ,GL_Account_Key  
          ,Glid_Configuration_Key  
          ,Event_Type_Key  
          ,Invoice_Key  
          ,Invoice_Attribute_Key  
          ,Invoice_Date_Utc_Key  
		  ,Pre_Pay_Start_Date_Utc_Key
		  ,Prepay_End_Date_Utc_Key
          ,Bill_Created_Date_Utc_Key  
          ,Bill_Created_Time_Utc_Key  
          ,Bill_Created_Date_Cst_Key  
          ,Bill_Created_Time_Cst_Key  
          ,Bill_Start_Date_Utc_Key  
          ,Bill_End_Date_Utc_Key  
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
          ,cast(PREPAY_TERM  as NUMERIC )AS Transaction_Term  
          ,Quantity  
          ,Billing_Days_In_Month  
          ,Total_Amount_Usd  
          ,0  
          ,Total_Amount_Gbp  
          ,0  
          ,Total_Amount_Local  
          ,0  
          ,cast(Is_Standalone_Fee  as bool) as Is_Standalone_Fee
          ,cast(Is_Back_Bill  as bool) as Is_Back_Bill
          ,cast(Is_Fastlane  as bool) as Is_Fastlane
          ,cast(Is_Transaction_Completed  as bool) as Is_Transaction_Completed
          ,Transaction_key  
          ,cast(Record_Created_Datetime as timestamp) as Record_Created_Datetime
          ,Record_Created_By  
          ,cast(Record_Updated_Datetime as timestamp) as Record_Updated_Datetime
          ,Record_Updated_By  
          ,Source_System_Key  
       FROM `rax-datamart-dev`.corporate_dmart.fact_invoice_event_detail_emailapps_tmp_fact_temp_data --#Tmp_FACT_TEMP_DATA
	   ;
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------  
-- Insertion of records into Fact table ends here   
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------  
  
/*==================================================================================================================================================================================================================   
 FACT LOAD : RE-DIRECTING BAD RECORDS TO RECYCLE TABLE BEGINS   
======================================================================================================================================================================================================================*/    
   
   
       -- Load the records having Zero Keys into Recycle Table  
       delete from  `rax-staging-dev`.stage_two_dw.recycle_fact_invoice_event_detail_email_and_apps where true;  
 
       INSERT INTO `rax-staging-dev`.stage_two_dw.recycle_fact_invoice_event_detail_email_and_apps
         (Date_Month_Key  
          ,Revenue_Type_Key  
          ,Account_Key  
          ,Team_Key  
          ,Primary_Contact_Key  
          ,Billing_Contact_Key  
          ,Product_Key  
		  ,Datacenter_Key
          ,Item_Key  
          ,GL_Product_Key  
          ,GL_Account_Key  
          ,Glid_Configuration_Key  
          ,Event_Type_Key  
          ,Invoice_Key  
          ,Invoice_Attribute_Key  
          ,Invoice_Date_Utc_Key 
		  ,Prepay_Start_Date_Utc_Key 
		  ,Prepay_End_Date_Utc_Key
          ,Bill_Created_Date_Utc_Key  
          ,Bill_Created_Time_Utc_Key  
          ,Bill_Created_Date_Cst_Key  
          ,Bill_Created_Time_Cst_Key  
          ,Bill_Start_Date_Utc_Key  
          ,Bill_End_Date_Utc_Key  
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
          ,Unit_Selling_Price_Usd  
          ,Amount_Gbp  
          ,Unit_Selling_Price_Gbp  
          ,Amount_Local  
          ,Unit_Selling_Price_Local  
          ,Is_Standalone_Fee  
          ,Is_Back_Bill  
          ,Is_Fastlane  
          ,Is_Transaction_Completed  
          ,Transaction_Key  
          ,Record_Created_Datetime  
          ,Record_Created_Source_Key  
          ,Record_Updated_Datetime  
          ,Record_Updated_Source_Key  
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
          ,Item_Key  
          ,GL_Product_Key  
          ,GL_Account_Key  
          ,Glid_Configuration_Key  
          ,Event_Type_Key  
          ,Invoice_Key  
          ,Invoice_Attribute_Key  
          ,Invoice_Date_Utc_Key  
		  ,Pre_Pay_Start_Date_Utc_Key
		  ,Prepay_End_Date_Utc_Key
          ,Bill_Created_Date_Utc_Key  
          ,Bill_Created_Time_Utc_Key  
          ,Bill_Created_Date_Cst_Key  
          ,Bill_Created_Time_Cst_Key  
          ,Bill_Start_Date_Utc_Key  
          ,Bill_End_Date_Utc_Key  
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
          ,cast(PREPAY_TERM  as  numeric ) AS Transaction_Term  
          ,Quantity  
          ,Billing_Days_In_Month  
          ,Total_Amount_Usd  
          ,0  
          ,Total_Amount_Gbp  
          ,0  
          ,Total_Amount_Local  
          ,0  
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
       FROM (
	   SELECT * --INTO #Fact_Temp_Data_Zero_Key_Records_EmailApps  
        FROM `rax-datamart-dev`.corporate_dmart.fact_invoice_event_detail_emailapps_tmp_fact_temp_data --#Tmp_FACT_TEMP_DATA  
       WHERE (    Account_Key = 0  
				 OR Team_Key = 0  
				 OR Primary_Contact_Key = 0  
				 OR Billing_Contact_Key = 0  
				 OR Invoice_Key = 0  
				 OR Invoice_Attribute_Key = 0  
				 OR Item_Key = 0  
				 OR GL_Product_Key = 0  
				 OR GL_Account_Key = 0  
				 OR Glid_Configuration_Key = 0  
				 OR Event_Type_Key = 0  
				 OR Currency_Key = 0  
         )
	   )--#Fact_Temp_Data_Zero_Key_Records_EmailApps
	   ;
	   /*
  
  
 DECLARE @subject nVARCHAR(max) = 'NRD Fact Load Failure Notification';  
 DECLARE @body nVARCHAR(max) = 'Data Transformation Failed during Fact Table Load'   
 + CHAR(10) + CHAR(13) + 'Error Number:  ' + CAST(ERROR_NUMBER() AS nVARCHAR(50))  
 + CHAR(10) + CHAR(13) + 'Error Severity:  ' + CAST(ERROR_SEVERITY() AS nVARCHAR(50))  
 + CHAR(10) + CHAR(13) + 'Error State:  ' + CAST(ERROR_STATE() AS nVARCHAR(50))  
 + CHAR(10) + CHAR(13) + 'Error Procedure:  ' + CAST(ERROR_PROCEDURE() AS nVARCHAR(100))  
 + CHAR(10) + CHAR(13) + 'Error Line:  ' + CAST(ERROR_LINE() AS nVARCHAR(50))  
 + CHAR(10) + CHAR(13) + 'Error Message: ' + ERROR_MESSAGE()  
 + CHAR(10) + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) + CHAR(13) + 'This is a system generated mail. DO NOT REPLY  ';  
 DECLARE @to nVARCHAR(max) = 'anil.kumar@rackspace.com';  
 DECLARE @profile_name sysname = 'Jobs';  
 EXEC msdb.dbo.sp_send_dbmail @profile_name = @profile_name,  
 @recipients = @to, @subject = @subject, @body = @body;  
  
  THROW      
       END CATCH  
  */
  
  
END;

