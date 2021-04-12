CREATE OR REPLACE PROCEDURE `rax-staging-dev.stage_one.udsp_etl_raw_report_exchange_rate`()
BEGIN


-----------------------------------------------------------------------------------------------------------------------  
create or replace table stage_one.raw_report_exchange_rate as
SELECT DISTINCT  
 Exchange_Rate_ID,   
 Exchange_Rate_From_Currency_Code,   
 Exchange_Rate_From_Currency_Description,   
 Exchange_Rate_To_Currency_Code,   
 Exchange_Rate_To_Currency_Description,   
 Exchange_Rate_To_Currency_Symbol,   
 Exchange_Rate_Time_Month_Key,  
 Exchange_Rate_Exchange_Rate_Value,   
 Exchange_Rate_From_Currency_Symbol,   
 Source_system_Name,
 current_date() as loaded_date    
FROM   
 (
	 SELECT DISTINCT  --#Data  
	 concat(Exchange_Rate_From_Currency_Code,Exchange_Rate_To_Currency_Code,Source_system_Name,CAST(Exchange_Rate_Month+Exchange_Rate_Year*100 as String )) AS Exchange_Rate_ID,   
	 Exchange_Rate_From_Currency_Code,   
	 Exchange_Rate_From_Currency_Description,   
	 Exchange_Rate_To_Currency_Code,   
	 Exchange_Rate_To_Currency_Description,   
	 Exchange_Rate_To_Currency_Symbol,   
	 Exchange_Rate_Month+Exchange_Rate_Year*100 AS Exchange_Rate_Time_Month_Key,  
	 Exchange_Rate_Exchange_Rate_Value,   
	 Exchange_Rate_From_Currency_Symbol,   
	 Source_system_Name  
		
	FROM   
	 `rax-datamart-dev`.corporate_dmart.report_exchange_rate A 
	where  
		Upper(Source_system_Name) in ('ORACLE','NAVISION')  
	AND report_exchange_rate_id not in (73048,73049)  
	AND Exchange_Rate_Month+Exchange_Rate_Year*100 >=200001
 )--#Data  
 ;
END;
