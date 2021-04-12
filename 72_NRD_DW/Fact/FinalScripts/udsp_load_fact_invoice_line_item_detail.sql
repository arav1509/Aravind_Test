
CREATE or replace PROCEDURE `rax-staging-dev`.stage_three_dw.udsp_load_fact_invoice_line_item_detail()
begin
/***********************************************************************************
Audit  
Version     Modified By          Date                       Description 
1.1          Vish5196           10/25/2019              DATA-6123 Initial creation
1.2          Vish5196           10/28/2019              Modify Insert logic  
1.3          Vish5196           10/30/2019              Truncate and load logic
Source:
Fact_Invoice_Line_Item_Cloud
Fact_Invoice_Line_Item_Dedicated
Fact_Invoice_Line_Item_Email_And_Apps
Destination : 
dbo.Fact_Invoice_Line_Item_Detail
************************************************************************************/
-- DECLARE VARIABLES 

DECLARE Begindate INT64;
DECLARE Enddate   INT64;
---------------------------------------------

SET Begindate= `rax-staging-dev`.bq_functions.udf_yearmonth_nohyphen(date_trunc(date_sub(current_date(), interval 2 year), year));
SET Enddate=   `rax-staging-dev`.bq_functions.udf_yearmonth_nohyphen(current_date());
------------------------------------------------------------------
               /*Archive Data*/
------------------------------------------------------------------
-- check if data needs to be archived. 

IF EXISTS (Select count(*) FROM `rax-datamart-dev`.corporate_dmart.fact_invoice_line_item_detail
WHERE date_month_key < Begindate
) 
then


	INSERT INTO `rax-datamart-dev`.corporate_dmart.fact_invoice_line_item_detail_history 
	SELECT * 
	FROM `rax-datamart-dev`.corporate_dmart.fact_invoice_line_item_detail
	WHERE date_month_key < Begindate;
 end if;
 


--delete from  Stage_Two_DW.dbo.Fact_Invoice_Line_Item_Detail_tmp 
delete from   `rax-datamart-dev`.corporate_dmart.fact_invoice_line_item_detail where true;
-- Insert data into tmp table. 


INSERT INTO `rax-datamart-dev`.corporate_dmart.fact_invoice_line_item_detail
SELECT *  FROM  
( 
SELECT 
    Date_Month_Key  ,
	Revenue_Type_Key  ,
	Account_Key  ,
	Team_Key  ,
	Primary_Contact_Key  ,
	Billing_Contact_Key  ,
	Product_Key  ,
	Datacenter_Key  ,
	Device_Key  ,
	Item_Key  ,
	GL_Product_Key  ,
	GL_Account_Key  ,
	GLid_Configuration_Key  ,
	Payment_Term_Key  ,
	Event_Type_Key  ,
	Invoice_Key  ,
	Invoice_Attribute_Key  ,
	Invoice_Date_Utc_Key  ,
	Bill_Created_Date_Utc_Key  ,
	Bill_Created_Time_Utc_Key  ,
	Bill_Start_Date_Utc_Key  ,
	Bill_End_Date_Utc_Key  ,
	Prepay_Start_Date_Utc_Key  ,
	Prepay_End_Date_Utc_Key  ,
	Event_Min_Created_Date_Utc_Key  ,
	Event_Min_Created_Time_Utc_Key  ,
	Event_Max_Created_Date_Utc_Key  ,
	Event_Max_Created_Time_Utc_Key  ,
	Earned_Min_Start_Date_Utc_Key  ,
	Earned_Min_Start_Time_Utc_Key  ,
	Earned_Max_Start_Date_Utc_Key  ,
	Earned_Max_Start_Time_Utc_Key  ,
	Currency_Key  ,
	Transaction_Term  ,
	Quantity ,
	Billing_Days_In_Month  ,
	Amount_Usd ,
	Amount_Normalized_Usd ,   
	Unit_Selling_Price_Usd ,
	Amount_Gbp ,
	Amount_Normalized_Gbp ,   
	Unit_Selling_Price_Gbp ,
	Amount_Local ,
	Amount_Normalized_Local ,  
	Unit_Selling_Price_Local ,
	Is_Standalone_Fee  ,
	Is_Normalize  ,  
	Is_Prepay  ,
	Is_Back_Bill  ,
	Is_Fastlane  ,
	Bill_Created_Date_Cst_Key  ,
	Bill_Created_Time_Cst_Key  ,
	Event_Min_Created_Date_Cst_Key  ,
	Event_Min_Created_Time_Cst_Key  ,
	Event_Max_Created_Date_Cst_Key  ,
	Event_Max_Created_Time_Cst_Key  ,
	Earned_Min_Start_Date_Cst_Key  ,
	Earned_Min_Start_Time_Cst_Key  ,
	Earned_Max_Start_Date_Cst_Key  ,
	Earned_Max_Start_Time_Cst_Key  ,
	Record_Created_Source_Key  ,
	Record_Created_Datetime  ,
	Record_Updated_Source_Key  ,
	Record_Updated_Datetime  ,
	Source_System_Key  
FROM `rax-datamart-dev`.corporate_dmart.fact_invoice_line_item_cloud 
WHERE Date_Month_Key >=Begindate and Date_Month_Key <= Enddate
UNION  all 
SELECT         Date_Month_Key,
               Revenue_Type_Key, 
			   Account_Key, 
               Team_Key, 
	           Primary_Contact_Key, 
			   Billing_Contact_Key,
			   Product_Key, 
	           Datacenter_Key, 
			   Device_Key, 
			   Item_Key, 
			   GL_Product_Key, 
			   GL_Account_Key, 
	           GLid_Configuration_Key, 
			   Payment_Term_Key, 
			   Event_Type_Key,
			   Invoice_Key, 
	           Invoice_Attribute_Key, 
			   Invoice_Date_Utc_Key, 
			   Bill_Created_Date_Utc_Key, 
	           Bill_Created_Time_Utc_Key, 
			   Bill_Start_Date_Utc_Key, 
			   Bill_End_Date_Utc_Key, 
	           Prepay_Start_Date_Utc_Key, 
			   Prepay_End_Date_Utc_Key,
               Event_Min_Created_Date_Utc_Key, 
			   Event_Min_Created_Time_Utc_Key, 
	           Event_Max_Created_Date_Utc_Key, 
			   Event_Max_Created_Time_Utc_Key, 
	           Earned_Min_Start_Date_Utc_Key, 
			   Earned_Min_Start_Time_Utc_Key, 
	           Earned_Max_Start_Date_Utc_Key, 
			   Earned_Max_Start_Time_Utc_Key, 
	           Currency_Key, 
			   Transaction_Term, 
			   Quantity, 
			   Billing_Days_In_Month, 
	           Amount_Usd, 
	           Amount_Usd AS  Amount_Normalized_Usd,  
	           Unit_Selling_Price_Usd,
	           Amount_Gbp, 
	           Amount_Gbp  AS Amount_Normalized_Gbp,  
			   Unit_Selling_Price_Gbp, 
	           Amount_Local, 
	           Amount_Local AS Amount_Normalized_Local, 
			   Unit_Selling_Price_Local, 
	           Is_Standalone_Fee, 
			   0 as  Is_Normalize, 
			   Is_Prepay, 
			   Is_Back_Bill, 
			   Is_Fastlane, 
	           Bill_Created_Date_Cst_Key, 
			   Bill_Created_Time_Cst_Key, 
	           Event_Min_Created_Date_Cst_Key, 
			   Event_Min_Created_Time_Cst_Key, 
	           Event_Max_Created_Date_Cst_Key, 
			   Event_Max_Created_Time_Cst_Key, 
	           Earned_Min_Start_Date_Cst_Key, 
			   Earned_Min_Start_Time_Cst_Key, 
	           Earned_Max_Start_Date_Cst_Key, 
			   Earned_Max_Start_Time_Cst_Key, 
	           Record_Created_Source_Key, 
			   Record_Created_Datetime, 
			   Record_Updated_Source_Key, 
	           Record_Updated_Datetime, 
			   Source_System_Key
FROM `rax-datamart-dev`.corporate_dmart.fact_invoice_line_item_dedicated  
WHERE Date_Month_Key >=Begindate and Date_Month_Key <= Enddate
UNION  all
SELECT          Date_Month_Key, 
                Revenue_Type_Key, 
				Account_Key, 
				Team_Key, 
				Primary_Contact_Key, 
                Billing_Contact_Key, 
				Product_Key, 
				Datacenter_Key, 
				Device_Key, 
                Item_Key, 
				GL_Product_Key, 
				GL_Account_Key, 
				GLid_Configuration_Key, 
                Payment_Term_Key, 
				Event_Type_Key, 
				Invoice_Key, 
				Invoice_Attribute_Key,
                Invoice_Date_Utc_Key, 
				Bill_Created_Date_Utc_Key, 
				Bill_Created_Time_Utc_Key, 
                Bill_Start_Date_Utc_Key, 
				Bill_End_Date_Utc_Key, 
				Prepay_Start_Date_Utc_Key, 
                Prepay_End_Date_Utc_Key, 
				Event_Min_Created_Date_Utc_Key, 
                Event_Min_Created_Time_Utc_Key, 
				Event_Max_Created_Date_Utc_Key, 
				Event_Max_Created_Time_Utc_Key, 
                Earned_Min_Start_Date_Utc_Key, 
				Earned_Min_Start_Time_Utc_Key, 
				Earned_Max_Start_Date_Utc_Key, 
                Earned_Max_Start_Time_Utc_Key, 
				Currency_Key, 
				Transaction_Term, 
				Quantity,
                Billing_Days_In_Month, 
				Amount_Usd, 
                Amount_Usd Amount_Normalized_Usd,   
				Unit_Selling_Price_Usd, 
                Amount_Gbp,
                Amount_Gbp as  Amount_Normalized_Gbp, 
				Unit_Selling_Price_Gbp, 
                Amount_Local, 
                Amount_Local as Amount_Normalized_Local,  
				Unit_Selling_Price_Local, 
				Is_Standalone_Fee, 
                0 as Is_Normalize,   
				Is_Prepay, 
				Is_Back_Bill, 
				Is_Fastlane, 
				Bill_Created_Date_Cst_Key, 
				Bill_Created_Time_Cst_Key, 
				Event_Min_Created_Date_Cst_Key, 
				Event_Min_Created_Time_Cst_Key, 
				Event_Max_Created_Date_Cst_Key, 
				Event_Max_Created_Time_Cst_Key, 
				Earned_Min_Start_Date_Cst_Key, 
				Earned_Min_Start_Time_Cst_Key, 
				Earned_Max_Start_Date_Cst_Key, 
				Earned_Max_Start_Time_Cst_Key, 
				Record_Created_Source_Key, 
				Record_Created_Datetime, 
				Record_Updated_Source_Key, 
				Record_Updated_Datetime, 
				Source_System_Key
FROM `rax-datamart-dev`.corporate_dmart.fact_invoice_line_item_email_and_apps 
WHERE Date_Month_Key >=Begindate and Date_Month_Key <= Enddate
) DD
;
--------------------------------------------------------------------------------------------------------------------
                                              /* DELETE FROM SOURCE */ 
--------------------------------------------------------------------------------------------------------------------
IF EXISTS ( SELECT count(*) FROM `rax-datamart-dev`.corporate_dmart.fact_invoice_line_item_detail
             where date_month_key < Begindate
             )
then
 
     DELETE FROM `rax-datamart-dev`.corporate_dmart.fact_invoice_line_item_detail
     WHERE  date_month_key <Begindate;
end if;

end;
