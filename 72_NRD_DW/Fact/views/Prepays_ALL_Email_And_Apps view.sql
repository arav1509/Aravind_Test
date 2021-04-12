create or replace view `rax-datamart-dev`.corporate_dmart.Prepays_ALL_Email_And_Apps as
SELECT --INTO #Prepays_ALL_Email_And_Apps
     Invoice.Bill_Number
    ,Fact.Date_Month_Key
    ,Fact.Revenue_Type_Key
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
    ,Fact.Bill_Created_Date_Cst_Key
    ,Fact.Bill_Created_Time_Cst_Key
    ,Fact.Bill_Start_Date_Utc_Key
    ,Fact.Bill_End_Date_Utc_Key
    ,Fact.Prepay_Start_Date_Utc_Key
    ,Fact.Prepay_End_Date_Utc_Key
    ,Fact.Event_Min_Created_Date_Utc_Key
    ,Fact.Event_Min_Created_Time_Utc_Key
    ,Fact.Event_Min_Created_Date_Cst_Key
    ,Fact.Event_Min_Created_Time_Cst_Key
    ,Fact.Earned_Min_Start_Date_Utc_Key
    ,Fact.Earned_Min_Start_Time_Utc_Key
    ,Fact.Earned_Min_Start_Date_Cst_Key
    ,Fact.Earned_Min_Start_Time_Cst_Key
	,Fact.Event_Max_Created_Date_Utc_Key
    ,Fact.Event_Max_Created_Time_Utc_Key
    ,Fact.Event_Max_Created_Date_Cst_Key
    ,Fact.Event_Max_Created_Time_Cst_Key
    ,Fact.Earned_Max_Start_Date_Utc_Key
    ,Fact.Earned_Max_Start_Time_Utc_Key
    ,Fact.Earned_Max_Start_Date_Cst_Key
    ,Fact.Earned_Max_Start_Time_Cst_Key
    ,Fact.Currency_Key
    ,Fact.Transaction_Term
    ,Fact.Quantity
    ,Fact.Billing_Days_In_Month
	,Fact.Amount_Usd
	,Fact.Extended_Amount_Usd
	,Fact.Unit_Selling_Price_Usd
	,Fact.Amount_Gbp
	,Fact.Extended_Amount_Gbp
	,Fact.Unit_Selling_Price_Gbp
    ,Fact.Amount_Local
    ,Fact.Extended_Amount_Local
    ,Fact.Unit_Selling_Price_Local
    ,Fact.Is_Standalone_Fee
	,1 As Is_Include_In_Payable
	,1 As Is_Prepay
	,1 As Is_Amortize_Prepay
    ,Fact.Is_Back_Bill
    ,Fact.Is_Fastlane
	,Fact.Source_System_Key
	,round(CAST(Fact.Amount_Local/(CASE WHEN Fact.Transaction_Term = 0 THEN 1 ELSE Fact.Transaction_Term end ) as numeric),4) AS Amortized_Local
	,round(CAST(Fact.Amount_Usd/(CASE WHEN Fact.Transaction_Term = 0 THEN 1 ELSE Fact.Transaction_Term end )as numeric),4)   AS Amortized_USD
	,round(CAST(Fact.Amount_Gbp/(CASE WHEN Fact.Transaction_Term = 0 THEN 1 ELSE Fact.Transaction_Term end) as numeric),4)   AS Amortized_GBP
FROM 
    `rax-datamart-dev`.corporate_dmart.fact_invoice_line_item_email_and_apps Fact 
	Left Join  `rax-datamart-dev`.corporate_dmart.dim_invoice Invoice  On Fact.Invoice_Key=Invoice.Invoice_Key
	WHERE 
	Fact.Date_Month_Key >= `rax-staging-dev`.bq_functions.udf_yearmonth_nohyphen(date_sub(current_date(), interval 2 Month))
AND	Fact.Date_Month_Key <= `rax-staging-dev`.bq_functions.udf_yearmonth_nohyphen(current_date())
AND	Is_Prepay is true
AND Is_Amortize_Prepay is true
;
