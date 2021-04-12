CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.report_tables.udsp_etl_device_online_oracle_vs_core()
/****************************************************************************
**  Last Modified by David Alvarez		
**  8/24/2015			
**  Added to Report_Tables

------ Modification History

03/01/19	CHANDRA PUTTA		DATA-3941: Changed the condition compare current_date() to be compatible with SQL SERVER 2016
*****************************************************************************/
begin
create or replace temp table Rev as
SELECT  --INTO #Rev
	Device_Number,
	measure_Dollar_Amount,
	Local_Currency_Amount,
	Local_Currency_Type_UOM
FROM  
	`rax-datamart-dev`.corporate_dmart.fact_revenue F  
JOIN
	`rax-datamart-dev`.corporate_dmart.dim_device D  
ON F.Device_Key = D.Device_Key
WHERE 1=1 
 and Time_month_key =`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(current_date()) 
and Revenue_Type_Key = 1;


create or replace temp table BSD as
SELECT --INTO #BSD 
	DEVICE_NUM, 
	BILLING_START_DATE 
FROM 
	`rax-landing-qa`.ebs_ods.raw_xxrs_sc_ci_dev_prod_vw 
WHERE 
	BILLING_END_dATE IS NULL;
	
create or replace temp table Team as 
SELECT DISTINCT--INTO #Team
	case when lower(team_name) like '%au%' or lower(team_name) like '%hk%' or lower(team_name) like '%apac%' then 'apac'
	when  lower(team_name) in ('team em1','team em2','team em3','team em4','team em5','team em6','team em7','team em8','team em9','team em10')
	or lower(team_name) like '%team int%' or lower(team_name) like 'team b %' or lower(team_name) like  '%uk%'
	or lower(team_name) like 'team mena m%' or lower(team_name) like '%nordic%' or lower(team_name) like 'team ch %' then 'emea'
	else 'usa' end  as account_geographic_location,
	lower(team_name)  as team_name
FROM   
	`rax-datamart-dev`.corporate_dmart.vw_account_device v ;
 ------------------------------------------------------------------             
create or replace temp table Core as
SELECT--INTO #Core
	Account_Number,
	Account_Name,
	Account_Status,
	T.Account_Geographic_Location,
	Account_Business_Type,
	V.Device_Number,
	Device_Online_Date,
	Device_Status_Number,
	Device_Status,
	Device_OS,
	Device_Placed_Order_Date,
	measure_Dollar_Amount as Device_CMRR,
	Local_Currency_Amount,
	Local_Currency_Type_UOM as Local_Currency,
	Device_Offline_Date,
	Team_Business_Segment,
	v.team_name,
	Device_Sales_Rep_1,
	Device_Sales_Rep_2,
	Account_Manager,
	BSD.BILLING_START_DATE,
	Account_sub_type
FROM
	`rax-datamart-dev`.corporate_dmart.vw_account_device v 
LEFT JOIN
	Rev Rev 
ON V.Device_Number = Rev.Device_Number 
LEFT JOIN 
	BSD BSD 
ON cast(v.device_number as string) = bsd.device_num
LEFT JOIN
	Team T
ON lower(v.team_name) = lower( T.team_name)
WHERE
	Time_month_key = `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(current_date())
and lower(account_source_system_name) = 'salesforce'
and lower(device_active_status) = 'active'
and lower(device_online_status) in ( 'online', 'support maintenance', 'suspended - billing')
and lower(device_os) not in ('net screens','vs cluster','unas','noteworth')
and device_status_number not in (50)
and device_status_number not in (50)
and lower(t.account_geographic_location) in ('apac','emea', 'usa')
and lower(team_business_segment) in ('ent east','ent f1000', 'ent west', 'ent z', 'enterprise services', 'hostingmatrix','hostingmatrix_uk', 'intensive,latam','mailtrust','managed','managed cloud',' managed cloud uk','managed colocation', 'racker it','rackspace cloud', 'rackspace managed hosting', 'self service', 'self service uk', 'slicehost', 'unknown')
;
create or replace temp table rate as
SELECT --INTO #rate --drop table #rate
	exchange_rate_exchange_rate_value,
	Exchange_Rate_From_Currency_Code
FROM 
	`rax-datamart-dev`.corporate_dmart.report_exchange_rate Rate 
WHERE 
	Rate.Exchange_Rate_Month = case when extract(day from current_date()) = 1 then extract(month from date_add(current_date(), interval -1 month))  else extract(month from current_date()) end
AND Rate.Exchange_Rate_Year=   case when extract(day from current_date()) = 1 then extract(year from date_add(current_date(), interval -1 month)) else  extract(year from current_date()) end
AND lower(Rate.Exchange_Rate_To_Currency_Code)='usd'
AND lower(Rate.Source_System_Name) ='oracle';


create or replace temp table Oracle as
SELECT --INTO #Oracle
	dev.ORG_ID as Country,
	CUSTOMER_NUM as Acct,
	DEVICE_NUM as Server,
	flv.meaning as Status,
	dev.ORG_ID,
	max(dev.MONTHLY_FEE)*Exchange_Rate_Exchange_Rate_Value as Monthly_Fee,
	max(dev.MONTHLY_FEE) as Local_Currency_Fee,
	hcas.attribute1 Local_Currency_Oracle,
	flv.start_date_active,
	flv.end_date_active,
	 case when dev.ORG_ID = 127 then 'USA'
	when dev.ORG_ID = 126 then 'U.K.'
	when dev.ORG_ID = 420 then 'NL'
	when dev.ORG_ID = 559 then 'HK' 
	else cast(dev.ORG_ID as string) end as Oracle_Location,
	DATE_CANCELLED,
	dev.CREATION_DATE,
	dev.LAST_UPDATE_DATE
FROM
	`rax-landing-qa`.ebs_ods.raw_xxrs_sc_device_product_tbl dev 
JOIN
	`rax-landing-qa`.ebs_ods.raw_fnd_lookup_values flv  
ON dev.product_status_code = flv.lookup_code 
LEFT JOIN
	`rax-landing-qa`.ebs_ods.raw_xxrs_sc_cust_bill_to_sites_vw acct  
ON dev.CUST_ACCOUNT_ID = acct.CUST_ACCOUNT_ID
LEFT JOIN
`rax-landing-qa`.ebs_ods.raw_hz_cust_acct_sites_all hcas  
ON dev.cust_acct_site_id = hcas.cust_acct_site_id
LEFT JOIN
	rate rate 
ON hcas.attribute1 = Rate.Exchange_Rate_From_Currency_Code

WHERE 
	lower(flv.meaning) in ('Offline','Draft', 'Void')
AND lower(flv.lookup_type )= 'rs_sc_product_status'
and cast(current_date() as date)  between ifnull(cast(start_date_active as date), cast(current_date() as date)) and ifnull(cast(end_date_active as date), cast(current_date() as date))
Group By
	dev.ORG_ID,
	CUSTOMER_NUM,
	DEVICE_NUM,
	flv.meaning,
	hcas.attribute1,
	dev.ORG_ID,
	flv.start_date_active,
	flv.end_date_active,
	DATE_CANCELLED,
	Exchange_Rate_Exchange_Rate_Value,
	dev.CREATION_DATE,
	dev.LAST_UPDATE_DATE;
------------------------------------------------------------------             
create or replace temp table creators as
SELECT --into #creators
	-1 computer_number, 
	'no comment' comments ;
------------------------------------------------------------------             
create or replace table `rax-abo-72-dev`.report_tables.device_online_oracle_vs_core as
SELECT DISTINCT 
	Account_Number AS oracle_link,
	Oracle_Location,
	Device_Number core_device_number,
	Device_Online_Date AS core_device_online_date,
	Local_Currency_Fee,
	Local_Currency_Oracle,
	Device_Status core_device_status,
	Status oracle_device_status,
	BILLING_START_DATE,
	N.CREATION_DATE oracle_creation_date,
	N.LAST_UPDATE_DATE oracle_update_date
FROM
	Core C 
LEFT JOIN
	`rax-landing-qa`.core_ods.queue_cancel_server q 
ON c.device_number = q.computer_number 
LEFT JOIN
	(Select * from Oracle WHERE upper(Oracle_Location) in ('HK','NL','USA', 'U.K.') )N
ON cast(C.Device_Number as string) = N.Server 
LEFT JOIN 
	creators X 
ON C.Device_Number = X.computer_number --LM 12/14/10
WHERE
	Billing_start_date is NULL
AND lower(Account_sub_type)<>'internal'
AND lower(Device_OS )not like '%virtual%'--GN 1/1/10
AND Account_Geographic_Location='USA'
Order by 
1,6;
end;