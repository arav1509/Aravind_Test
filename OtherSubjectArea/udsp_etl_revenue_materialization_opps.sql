CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.report_tables.udsp_etl_revenue_materialization_opps()
/****************************************************************************
**  Last Modified by David Alvarez		
**  8/24/2015			
**  Added to Report_Tables
*****************************************************************************/
begin
create or replace table `rax-abo-72-dev`.report_tables.revenue_materialization_opps as
SELECT DISTINCT 
	Account_Number,
	IfNULL(Opportunity_ID,Rev_Ticket_Number)				AS Opportunity_ID,
	Bookings_Date,
	Bookings_TMK,
	Opp_Bookings_USD,
	Opp_Bookings_Local,
	EC_Contract_Received_Date,
	`rax-abo-72-dev`.bq_functions.udf_yearmonth_nohyphen(EC_Contract_Received_Date)	AS Contract_Received_TMK,
	SF_Stage_Name,
	Bookings_Conversion_Rate,
	Bookings_Currency,
	SF_Opportunity_Type,
	Opp_Owner,
	Opp_Owner_Role,
	Opp_Owner_Region,
	CASE WHEN lower(EC_Queue_Name) like 'sales/support required actions'
		AND Debook_Flag<>1 THEN 1 Else 0 END				AS KickBackFlag
FROM 
	`rax-abo-72-dev`.report_tables.revenue_materialization R 
WHERE 
	(Opportunity_ID IS NOT NULL OR Rev_Ticket_Number IS NOT NULL) 
AND lower(SF_Stage_Name)<>'closed lost';
-----------------------------------------------------------------------------
end;
