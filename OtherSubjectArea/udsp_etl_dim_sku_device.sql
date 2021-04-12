CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.report_tables.udsp_etl_dim_sku_device()
begin

DECLARE  BeginDate datetime;
DECLARE  EndDate datetime;

-----------------------------------------------------------------------------------------------------------------
Set BeginDate =DATE_ADD(`rax-abo-72-dev.bq_functions.udfdatepart`(current_date()), interval -14 month);
Set EndDate =current_date();

create or replace table `rax-abo-72-dev`.report_tables.dim_sku_device as
SELECT 
    Time_Month_Key,
    Account_Number,
    Device_Number,
    Device_Status,
    SKU_Number
FROM
(
SELECT
	Time_Month_Key,
	Account_Number,
	Device_Number,
	Device_Status,
	SKU_Number
FROM
    `rax-datamart-dev`.corporate_dmart.fact_sku_assignment	 A 
JOIN
    `rax-datamart-dev`.corporate_dmart.dim_account C 
ON A.Account_Key = C.Account_Key
JOIN
    `rax-datamart-dev`.corporate_dmart.dim_device D 
ON A.Device_Key = D.Device_Key
JOIN
	`rax-datamart-dev`.corporate_dmart.dim_sku E 
ON A.SKU_Key = E.SKU_Key
WHERE
	lower(Account_Status) <> 'closed'
AND lower(Device_Status)<>'computer no longer active'
AND Time_Month_Key >= `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(BeginDate)
AND Time_Month_Key <= `rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(EndDate)
);

end;
