CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.report_tables.udsp_etl_revenue_materialization_om()
begin
/****************************************************************************
**  Last Modified by David Alvarez		
**  8/24/2015			
**  Added to Report_Tables
*****************************************************************************/

create or replace temp table dim_device as 
SELECT --INTO #dim_device
	Device_Number,
	Due_to_Customer_Date,
	Due_to_Support_Date
FROM 
	`rax-datamart-dev`.corporate_dmart.dim_device 
WHERE current_record=1
AND due_to_customer_date is not null;


create or replace table `rax-abo-72-dev`.report_tables.revenue_materialization_om as
SELECT --INTO dbo.Revenue_Materialization_OM
	Opportunity_ID,
	Rev_Ticket_Number,
	Rev_Ticket_Authorized_Date,
	Rev_Ticket_Completion_Date,
	EC_Queue_Name,
	R.Device_Number,
	Device_Type,
	Device_Type_eConnect,
	Device_Status,
	Case When Device_Online_Date is null then Device_Forecasted_Online_Date else Device_Online_Date end as Device_Online_Date,
	Case When Device_Online_TMK is null then Device_forecast_TMK else Device_Online_TMK end as Device_Online_TMK,
	Device_Initial_Forecast_Date,
	Device_Initial_TMK,
	Days_Online_InitialForecast,
	Device_Forecasted_Online_Date,
	Device_forecast_TMK,
	Days_Online_Forecasted,
	Bookings_Bucket,
	Free_Days,
	Debook_Flag,
	Debook_Date,
	Debook_Device_MRR_Local,
	Debooked_MRR,
	Device_Gross_MRR_Local,
	Device_Gross_MRR_USD,
	Device_Previous_MRR_Local,
	Device_Previous_MRR_USD,
	Device_Net_MRR_Local,
	Device_Net_MRR_USD,
	Device_Currency,
	EC_OM_Reason,
	EC_OM_Note,
	IS_EC_OM,
	Is_Trackable,
	Materialization_Type,
	Materialization_date,
	Materialization_TMK,
	Prorate_Multiple,
	Materialization_Conversion_Rate,
	Full_Materialization_Local,
	Full_Materialization_USD,
	Materialization_Minus_Migration_Local,
	Materialization_Minus_Migration_USD,
	Materialization_TMK2,
	M1_Prorated_Full,
	M1_Prorated_Minus_Migration,
	M2_Prorated_Full,
	M2_Prorated_Minus_Migration,
	load_Date,
	EC_Contract_Type,
	Online_to_Forecast_Flag,
	Online_to_InitialForecast_flag,
	OM_Flag,
	OPP_ID_Type,
	VM_Flag,
	OM_bucket,
	Due_to_Customer_Date,
	datetime_diff(device_online_date,due_to_customer_date,day)										AS Days_From_Due_to_Customer,
	CASE 
	WHEN datetime_diff(device_online_date,due_to_customer_date,day)>2 then 'Early'
	WHEN datetime_diff(device_online_date,due_to_customer_date,day)<-2 then 'Late'
	Else 'On Time' END																			AS Late_Early,
	CASE 
	WHEN datetime_diff(device_online_date,due_to_customer_date,day) between 2 and 15 then '2-15'
	WHEN datetime_diff(device_online_date,due_to_customer_date,day) between 15 and 30 then '16-30'
	WHEN datetime_diff(device_online_date,due_to_customer_date,day) between 30 and 60 then '30-60'
	WHEN datetime_diff(device_online_date,due_to_customer_date,day) between -15 and -2 then '-15--2'
	WHEN datetime_diff(device_online_date,due_to_customer_date,day) between -30 and -15 then '-30--15'
	WHEN datetime_diff(device_online_date,due_to_customer_date,day) between -60 and -30 then '-60--30'
	WHEN datetime_diff(device_online_date,due_to_customer_date,day) <= -60  then '<-60'
	WHEN datetime_diff(device_online_date,due_to_customer_date,day) >= 60  then '>60' 		
	Else 'On Time'	END	AS Due_to_Customer_Buckets
FROM  `rax-abo-72-dev`.report_tables.revenue_materialization R 
LEFT JOIN 
	dim_device dd 
ON dd.device_number=r.device_number
WHERE 
	lower(SF_Stage_Name)<>'Closed Lost' 
AND (Opportunity_ID IS NOT NULL OR Rev_Ticket_Number IS NOT NULL)

GROUP BY 
	Opportunity_ID,
	Rev_Ticket_Number,
	Rev_Ticket_Authorized_Date,
	Rev_Ticket_Completion_Date,
	EC_Queue_Name,
	EC_Contract_Received_Date,
	EC_Contract_Type,
	R.Device_Number,
	Device_Type,
	Device_Type_eConnect,
	Device_Status,
	Device_Online_Date,
	Case When Device_Online_Date is null then Device_Forecasted_Online_Date else Device_Online_Date end,
	Case When Device_Online_TMK is null then Device_forecast_TMK else Device_Online_TMK end,
	Device_Initial_Forecast_Date,
	Device_Initial_TMK,
	Days_Online_InitialForecast,
	Device_Forecasted_Online_Date,
	Device_forecast_TMK,
	Days_Online_Forecasted,
	Bookings_Bucket,
	Free_Days,
	Debook_Flag,
	Debook_Date,
	Debook_Device_MRR_Local,
	Debooked_MRR,
	Device_Gross_MRR_Local,
	Device_Gross_MRR_USD,
	Device_Previous_MRR_Local,
	Device_Previous_MRR_USD,
	Device_Net_MRR_Local,
	Device_Net_MRR_USD,
	Device_Currency,
	EC_OM_Reason,
	EC_OM_Note,
	IS_EC_OM,
	Is_Trackable,
	Materialization_Type,
	Materialization_date,
	Materialization_TMK,
	Prorate_Multiple,
	Materialization_Conversion_Rate,
	Full_Materialization_Local,
	Full_Materialization_USD,
	Materialization_Minus_Migration_Local,
	Materialization_Minus_Migration_USD,
	Materialization_TMK2,
	M1_Prorated_Full,
	M1_Prorated_Minus_Migration,
	M2_Prorated_Full,
	M2_Prorated_Minus_Migration,
	load_Date,
	EC_Contract_Type,
	Online_to_Forecast_Flag,
	Online_to_InitialForecast_flag,
	OM_Flag,
	OPP_ID_Type,
	VM_Flag,
	OM_bucket,
	due_to_customer_date,
	datetime_diff(device_online_date,due_to_customer_date,day),
	datetime_diff(device_online_date,due_to_customer_date,day),
	CASE 
	WHEN datetime_diff(device_online_date,due_to_customer_date,day)>2 then 'Early'
	WHEN datetime_diff(device_online_date,due_to_customer_date,day)<-2 then 'Late'
	Else 'On Time' END,
	CASE 
	WHEN datetime_diff(device_online_date,due_to_customer_date,day) between 2 and 15 then '2-15'
	WHEN datetime_diff(device_online_date,due_to_customer_date,day) between 15 and 30 then '16-30'
	WHEN datetime_diff(device_online_date,due_to_customer_date,day) between 30 and 60 then '30-60'
	WHEN datetime_diff(device_online_date,due_to_customer_date,day) between -15 and -2 then '-15--2'
	WHEN datetime_diff(device_online_date,due_to_customer_date,day) between -30 and -15 then '-30--15'
	WHEN datetime_diff(device_online_date,due_to_customer_date,day) between -60 and -30 then '-60--30'
	WHEN datetime_diff(device_online_date,due_to_customer_date,day) <= -60  then '<-60'
	WHEN datetime_diff(device_online_date,due_to_customer_date,day) >= 60  then '>60' 		
	Else 'On Time' end
;
end;