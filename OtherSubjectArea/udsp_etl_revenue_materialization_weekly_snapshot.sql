CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.report_tables.udsp_etl_revenue_materialization_weekly_snapshot()

BEGIN
DECLARE CurrentTime_Week int64;
-------------------------------------------------------------------------------------------------------------------------------------------------------------
SET CurrentTime_Week=cast( concat(extract(year from current_date()),extract(week from current_date())) as int64);

DELETE FROM `rax-abo-72-dev`.report_tables.revenue_materialization_snapshot WHERE Rev_Mat_Snapshot_Load_Week=CurrentTime_Week;
---------------------------------------------------------------------------------------------------------------------------------------------------
create or replace temp table  REV_MAT  as
SELECT DISTINCT
    ifnull(Opportunity_ID,'0')							AS Opportunity_ID,
    Account_Number,
    Account_Name,
    BDC,
    ifnull(EC_Contract_Received_Date, '1900-01-01')		AS EC_Contract_Received_Date,
    Device_Number,
    Device_Type_eConnect,
    Device_Status,
    ifnull(Device_Online_Date, '1900-01-01')				AS Device_Online_Date,
    ifnull(Device_Initial_Forecast_Date, '1900-01-01')	AS Device_Initial_Forecast_Date,
    ifnull(Device_Forecasted_Online_Date,'1900-01-01')	AS Device_Forecasted_Online_Date,
    Debook_Flag,
    Device_Gross_MRR_USD,
    Device_Gross_MRR_Local,
    Opp_Bookings_USD,
    Opp_Bookings_Local,
    Is_EC_OM,
    EC_OM_Reason,
    EC_OM_Note,
	CASE WHEN EC_Queue_Name LIKE 'Sales/Support Required Actions'
		AND Debook_Flag<>1 THEN 1 Else 0 END			AS In_Kickback,
    ifnull(Materialization_TMK,190001)					AS Materialization_TMK,
    ifnull(Materialization_date,'1900-01-01')				AS Materialization_Date,
    Full_Materialization_USD,
    cast( concat(extract(year from current_date()),extract(week from current_date())) as int64)	AS Rev_Mat_Snapshot_Load_Week,
    current_date()											AS Rev_Mat_Snapshot_Load_Date
FROM  `rax-abo-72-dev`.report_tables.revenue_materialization ;
--------------------------------------------------------------------------------------------------------------------------------------------------
INSERT INTO `rax-abo-72-dev`.report_tables.revenue_materialization_snapshot
SELECT
*
FROM REV_MAT ;   
--------------------------------------------------------------------------------------------------------------------------------------------------



end;