CREATE or replace PROCEDURE `rax-staging-dev`.stage_three_dw.udsp_load_fact_net_revenue()
begin




DECLARE Begindate INT64;
DECLARE Enddate   INT64;
DECLARE Record_Created_Datetime DATETIME ;
DECLARE Record_Updated_Datetime DATETIME ;
DECLARE Record_Created_By string;
DECLARE Record_Updated_By string;

---------------------------------
SET Begindate= `rax-staging-dev`.bq_functions.udf_yearmonth_nohyphen(date_trunc(date_sub(current_date(), interval 2 year), year)); 
SET Enddate= `rax-staging-dev`.bq_functions.udf_yearmonth_nohyphen(current_date());


IF EXISTS (Select count(*) FROM `rax-datamart-dev`.corporate_dmart.fact_net_revenue
WHERE date_month_key < Begindate
) 

then


	INSERT INTO `rax-datamart-dev`.corporate_dmart.fact_net_revenue_history 
	SELECT * 	
	FROM `rax-datamart-dev`.corporate_dmart.fact_net_revenue
	WHERE date_month_key < Begindate;
	
END if;

/********************************************
TRUNCATE FACT TABLE 
*********************************************/

delete from  `rax-datamart-dev`.corporate_dmart.fact_net_revenue where true;


/***********************************************
Insert into tmp table 
*************************************************/

INSERT INTO `rax-datamart-dev`.corporate_dmart.fact_net_revenue
SELECT * FROM 
( 
SELECT Fact. Date_Month_Key
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
      ,Fact.Invoice_Key
      ,Fact.Invoice_Date_Utc_Key
      ,Fact.Currency_Key
      ,SUM(Fact.Amount_Usd) AS Amount_Usd
      ,SUM(Fact.Amount_Normalized_Usd) As Amount_Normalized_Usd
      ,SUM(Fact.Amount_Gbp) As Amount_Gbp
      ,SUM(Fact.Amount_Normalized_Gbp) As Amount_Normalized_Gbp
      ,SUM(Fact.Amount_Local) As Amount_Local
      ,SUM(Fact.Amount_Normalized_Local) AS Amount_Normalized_Local
      ,Fact.Record_Created_Datetime 
	  , cast(Record_Created_Source_Key as string) AS Record_Created_By -- get from source. 
	  ,Fact.Record_Updated_Datetime 
	  ,cast(Record_Updated_Source_Key as string)AS Record_Updated_By
      ,Fact.Source_System_Key
  FROM `rax-datamart-dev`.corporate_dmart.fact_invoice_line_item_detail Fact
  INNER JOIN `rax-datamart-dev`.corporate_dmart.dim_gl_account GLAccount
  On Fact.GL_Account_Key=GLAccount.GL_Account_Key
  --INNER JOIN Stage_Three_Dw_NRD.dbo.XRef_GL_Account RefGLAccount WITH (NOLOCK) ON GLAccount.GL_Account_Id=RefGLAccount.GL_Account_Id
 WHERE
	GLAccount.gl_account_id like '4%'
  GROUP  BY
	Fact. Date_Month_Key
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
      ,Fact.Invoice_Key
      ,Fact.Invoice_Date_Utc_Key
      ,Fact.Currency_Key
	  ,Fact.Record_Created_Datetime 
	  ,Fact.Record_Updated_Datetime 
	  ,fact.Record_Created_Source_Key
	  ,fact.Record_Updated_Source_Key
	  ,Fact.Source_System_Key
) DER 
;


end;
